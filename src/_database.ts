import Logger from './common/logger';
const logger = Logger.ns('DatabaseClient');

import { Pool, PoolClient, PoolConfig } from 'pg';
import { validatePostgresString } from './common/utils';
import { EBuiltinDatabaseObjectNames, TRIGGER_OPERATIONS } from './common/constants';

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
    logger.debug('Connecting...');

    await this.pool.connect();

    logger.debug('Connected.');
  }

  async disconnect() {
    logger.debug('Disconnecting...');

    await this.pool.end();

    logger.debug('Disconnected.');
  }

  async teardown() {
    await this.transaction(async client => {
      const existingTriggers = await this.findListenerTriggers(client);

      for (const tableName of Object.keys(existingTriggers)) {
        await this.dropListenerTrigger(client, tableName);
      }
    });
  }

  async createClient() {
    const client = await this.pool.connect();

    return client;
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

  prefixName(name: string, escaper?: (str: string) => string) {
    const prefixedName = `${this.prefix}__${name}`;

    if (!validatePostgresString(prefixedName)) {
      throw new Error(`Invalid prefixed name: ${prefixedName}`);
    }

    return escaper?.apply(escaper, [ prefixedName, ]) ?? prefixedName;
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

  private async createListenerTrigger(client: PoolClient, tableName: string, operations: TOperation[], keepColumns?: string[]) {
    const escapedTableName = client.escapeIdentifier(tableName);
    const triggerName = this.prefixName(`listener_trigger_${tableName}`, client.escapeIdentifier);
    const triggerFunctionName = this.prefixName(`listener_trigger_${tableName}_fn`, client.escapeIdentifier);
    const queueTableName = this.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
      client.escapeIdentifier
    );

    const formattedOperations = operations.join(' OR ');
    if (operations.some(o => !TRIGGER_OPERATIONS.includes(o))) {
      throw new Error(`One or more operations is not valid: ${formattedOperations}`);
    }

    const createJsonFormatter = (source: 'OLD' | 'NEW') => {
      if (!keepColumns) {
        return /* sql */`CASE WHEN ${source} IS NULL THEN NULL ELSE to_jsonb(${source}) END`;
      }

      if (keepColumns.length === 0) { return /* sql */'NULL'; }

      const buildObjectParams = keepColumns
        .map(column => `${client.escapeLiteral(column)}, ${source}.${client.escapeIdentifier(column)}`)
        .join(', ');

      return /* sql */`jsonb_build_object(${buildObjectParams})`;
    };

    await client.query(/* sql */ `
      CREATE OR REPLACE FUNCTION ${triggerFunctionName}() RETURNS trigger AS $$
        BEGIN
          INSERT INTO ${queueTableName} ("tableName", "operation", "previousRecord", "currentRecord", "queuedAt")
          VALUES (
            TG_TABLE_NAME,
            TG_OP,
            ${createJsonFormatter('OLD')},
            ${createJsonFormatter('NEW')},
            CLOCK_TIMESTAMP()
          );

          RETURN NEW;
        EXCEPTION
          WHEN UNIQUE_VIOLATION THEN RAISE NOTICE 'failed to execute trigger';
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
}
