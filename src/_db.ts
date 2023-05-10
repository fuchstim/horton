import { Pool, PoolClient, PoolConfig } from 'pg';
import { validatePostgresString } from './common/utils';

const TRIGGER_OPERATIONS = [ 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', ] as const;

export type TConnectionOptions = PoolConfig & { prefix?: string };
export type TOperation = typeof TRIGGER_OPERATIONS[number];

export default class DatabaseClient {
  private pool: Pool;
  private prefix: string;

  constructor(options: TConnectionOptions) {
    this.pool = new Pool(options);
    this.prefix = options.prefix || 'horton-meta';

    if (!validatePostgresString(this.prefix)) {
      throw new Error(`Invalid prefix: ${this.prefix}`);
    }
  }

  async connect() {
    await this.pool.connect();
  }

  async disconnect() {
    await this.pool.end();
  }

  async initialize() {
    await this.transaction(async client => {
      await this.createQueueTable(client);
      await this.validateQueueTable(client);
      await this.createQueueTableTrigger(client);
    });
  }

  async teardown() {
    await this.transaction(async client => {
      const existingTriggers = await this.findListenerTriggers(client);

      for (const tableName of Object.keys(existingTriggers)) {
        await this.dropListenerTrigger(client, tableName);
      }

      await this.dropQueueTable(client);
    });
  }

  async syncListeners(requiredListeners: Record<string, TOperation[]>) {
    await this.transaction(async client => {
      const existingTriggers = await this.findListenerTriggers(client);

      const triggersToRemove = Object
        .keys(existingTriggers)
        .filter(tableName => !requiredListeners[tableName]?.length);

      for (const tableName of triggersToRemove) {
        await this.dropListenerTrigger(client, tableName);
      }

      for (const [ tableName, operations, ] of Object.entries(requiredListeners)) {
        await this.createListenerTrigger(client, tableName, operations);
      }
    });
  }

  async onQueued(cb: (rowId: number) => void | Promise<void>) {
    const client = await this.pool.connect();

    client.on('notification', message => {
      if (!message.payload) { return; }

      cb.apply(cb, [ Number(message.payload), ]);
    });

    const notificationChannelName = this.prefixName('notifications', client.escapeIdentifier);
    await client.query(`LISTEN ${notificationChannelName}`);
  }

  private prefixName(name: string, escaper?: (str: string) => string) {
    const prefixedName = `${this.prefix}__${name}`;

    if (!validatePostgresString(prefixedName)) {
      throw new Error(`Invalid prefixed name: ${prefixedName}`);
    }

    return escaper?.apply(escaper, [ prefixedName, ]) ?? prefixedName;
  }

  private async createQueueTable(client: PoolClient) {
    const tableName = this.prefixName('queue', client.escapeIdentifier);

    await client.query(/* sql */ `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        "id" SERIAL PRIMARY KEY,
        "tableName" NAME NOT NULL,
        "operation" TEXT NOT NULL,
        "rowId" TEXT NOT NULL,
        "metadata" JSONB NOT NULL,
        "queuedAt" TIMESTAMPTZ NOT NULL
      )
    `);
  }

  private async dropQueueTable(client: PoolClient) {
    const tableName = this.prefixName('queue', client.escapeIdentifier);

    await client.query(/* sql */ `
      DROP TABLE IF EXISTS ${tableName} CASCADE;
    `);
  }

  private async validateQueueTable(client: PoolClient) {
    const tableName =this.prefixName('queue');

    const tableData = await client.query<{ name: string, type: string }>(/* sql */ `
      SELECT 
        column_name as "name",
        data_type as "type"
      FROM
        information_schema.columns
      WHERE
        table_name = $1;
      `, [ tableName, ]
    );

    const expectedColumns = [
      { name: 'id', type: 'integer', },
      { name: 'tableName', type: 'name', },
      { name: 'operation', type: 'text', },
      { name: 'rowId', type: 'text', },
      { name: 'metadata', type: 'jsonb', },
      { name: 'queuedAt', type: 'timestamp with time zone', },
    ];

    if (tableData.rowCount !== expectedColumns.length) {
      throw new Error('Queue table exists but is not valid');
    }

    for (const row of tableData.rows) {
      const expectedColumn = expectedColumns.find(
        (col) => row.name === col.name && row.type === col.type
      );

      if (!expectedColumn) {
        throw new Error('Queue table exists but is not valid');
      }
    }
  }

  private async createQueueTableTrigger(client: PoolClient) {
    const triggerName = this.prefixName('queue_trigger', client.escapeIdentifier);
    const triggerFunctionName = this.prefixName('queue_trigger_fn', client.escapeIdentifier);
    const notificationChannelName = this.prefixName('notifications', client.escapeLiteral);
    const queueTableName = this.prefixName('queue', client.escapeIdentifier);

    await client.query(/* sql */ `
      CREATE OR REPLACE FUNCTION ${triggerFunctionName}() RETURNS trigger AS $$
        BEGIN
          PERFORM pg_notify(${notificationChannelName}, NEW.id::TEXT);
          
          RETURN NEW;
        END;
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE TRIGGER ${triggerName}
      AFTER INSERT ON ${queueTableName}
      FOR EACH ROW
      EXECUTE PROCEDURE ${triggerFunctionName}();
    `);
  }

  private async findListenerTriggers(client: PoolClient) {
    const triggerPrefix = this.prefixName('listener_trigger');

    const triggers = await client.query<{ tableName: string, operation: TOperation }>(/* sql */ `
      SELECT event_object_table as "tableName", event_manipulation as "operation"
      FROM information_schema.triggers
      WHERE trigger_name LIKE $1
    `, [ `${triggerPrefix}%`, ]);

    return triggers.rows.reduce(
      (acc, { tableName, operation, }) => {
        if (!acc[tableName]) { acc[tableName] = []; }

        if (!acc[tableName].includes(operation)) {
          acc[tableName].push(operation);
        }

        return acc;
      },
      {} as Record<string, TOperation[]>
    );
  }

  private async createListenerTrigger(client: PoolClient, tableName: string, operations: TOperation[]) {
    const escapedTableName = client.escapeIdentifier(tableName);
    const triggerName = this.prefixName(`listener_trigger_${tableName}`, client.escapeIdentifier);
    const triggerFunctionName = this.prefixName(`listener_trigger_${tableName}_fn`, client.escapeIdentifier);
    const queueTableName = this.prefixName('queue', client.escapeIdentifier);

    const formattedOperations = operations.join(' OR ');
    if (operations.some(o => !TRIGGER_OPERATIONS.includes(o))) {
      throw new Error(`One or more operations is not valid: ${formattedOperations}`);
    }

    await client.query(/* sql */ `
      CREATE OR REPLACE FUNCTION ${triggerFunctionName}() RETURNS trigger AS $$
        BEGIN
          INSERT INTO ${queueTableName} ("tableName", "operation", "rowId", "metadata", "queuedAt")
          VALUES (
            TG_TABLE_NAME,
            TG_OP,
            NEW.id::TEXT,
            JSONB_BUILD_OBJECT('old', row_to_json(OLD)::jsonb, 'new', row_to_json(NEW)::jsonb),
            CLOCK_TIMESTAMP()
          );

          RETURN NEW;
        END;
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE TRIGGER ${triggerName}
      AFTER ${formattedOperations} ON ${escapedTableName}
      FOR EACH ROW
      EXECUTE PROCEDURE ${triggerFunctionName}();
    `);
  }

  private async dropListenerTrigger(client: PoolClient, tableName: string) {
    const escapedTableName = client.escapeIdentifier(tableName);
    const triggerName = this.prefixName(`listener_trigger_${tableName}`, client.escapeIdentifier);
    const triggerFunctionName = this.prefixName(`listener_trigger_${tableName}_fn`, client.escapeIdentifier);

    await client.query(/* sql */ `
      DROP TRIGGER IF EXISTS ${triggerName}
      ON ${escapedTableName};

      DROP FUNCTION IF EXISTS ${triggerFunctionName}();
    `);
  }

  async transaction<T>(cb: (client: PoolClient) => T): Promise<T> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const result = await Promise.resolve(cb.apply(cb, [ client, ]));

      await client.query('COMMIT');

      return result;
    } catch (error: unknown) {
      await client.query('ROLLBACK');

      throw error;
    } finally {
      client.release();
    }
  }
}
