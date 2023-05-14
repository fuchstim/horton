import Logger from './common/logger';
const logger = Logger.ns('EventQueue');

import type DatabaseClient from './_database';
import { EBuiltinDatabaseObjectNames, TQueueEvents, TQueueNotification, TQueueRow } from './common/types';

import { PoolClient } from 'pg';
import EventEmitter from './common/event-emitter';
import { isTriggerOperation } from './common/utils';

export default class EventQueue extends EventEmitter<TQueueEvents> {
  private dbClient: DatabaseClient;

  constructor(dbClient: DatabaseClient) {
    super();

    this.dbClient = dbClient;
  }

  async connect() {
    await this.startListening()
      .then(() => logger.info('Started listening'))
      .catch(error => logger.error('Failed to start listening', error));

    // TODO: Start periodic reconciliation
  }

  async disconnect() {
    // TODO: Flush all listeners
  }

  async initialize() {
    await this.dbClient.transaction(async client => {
      await this.create(client);

      await this.validate(client);

      await this.createTrigger(client);
    });
  }

  async teardown() {
    await this.dbClient.transaction(client => this.drop(client));
  }

  async resolveRow(client: PoolClient, rowId: number): Promise<TQueueRow | undefined> {
    logger.debug(`Resolving row ${rowId}`);

    const tableName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
      client.escapeIdentifier
    );

    const results = await client.query<TQueueRow>(/* sql */`
      SELECT *
      FROM ${tableName}
      WHERE id = $1
      FOR UPDATE
    `, [ rowId, ]);

    if (!results.rowCount) {
      logger.debug(`Failed to resolve row ${rowId}`);

      return;
    }

    return results.rows[0];
  }

  private async create(client: PoolClient) {
    const tableName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
      client.escapeIdentifier
    );

    await client.query(/* sql */ `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        "id" SERIAL PRIMARY KEY,
        "tableName" NAME NOT NULL,
        "operation" TEXT NOT NULL,
        "previousRecord" JSONB,
        "currentRecord" JSONB NOT NULL,
        "queuedAt" TIMESTAMPTZ NOT NULL
      )
    `);

    logger.debug('Created queue table');
  }

  private async drop(client: PoolClient) {
    const tableName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
      client.escapeIdentifier
    );

    await client.query(/* sql */ `DROP TABLE IF EXISTS ${tableName} CASCADE;`);

    logger.debug('Dropped queue table');
  }

  private async validate(client: PoolClient) {
    const tableName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE
    );

    const tableData = await client.query<{ name: string, type: string, nullable: string }>(/* sql */ `
      SELECT 
        column_name as "name",
        data_type as "type",
        is_nullable as "nullable"
      FROM
        information_schema.columns
      WHERE
        table_name = $1;
      `, [ tableName, ]
    );

    const expectedColumns = [
      { name: 'id', type: 'integer', nullable: 'NO', },
      { name: 'tableName', type: 'name', nullable: 'NO', },
      { name: 'operation', type: 'text', nullable: 'NO', },
      { name: 'previousRecord', type: 'jsonb', nullable: 'YES', },
      { name: 'currentRecord', type: 'jsonb', nullable: 'NO', },
      { name: 'queuedAt', type: 'timestamp with time zone', nullable: 'NO', },
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

    logger.debug('Validated queue table.');
  }

  private async createTrigger(client: PoolClient) {
    const triggerName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TRIGGER,
      client.escapeIdentifier
    );
    const triggerFunctionName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TRIGGER_FUNCTION,
      client.escapeIdentifier
    );
    const notificationChannelName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_NOTIFICATION_CHANNEL,
      client.escapeLiteral
    );
    const queueTableName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
      client.escapeIdentifier
    );

    await client.query(/* sql */ `
      CREATE OR REPLACE FUNCTION ${triggerFunctionName}() RETURNS trigger AS $$
        BEGIN
          PERFORM pg_notify(${notificationChannelName}, CONCAT(NEW."id", ':', NEW."tableName", ':', NEW."operation"));
          
          RETURN NEW;
        END;
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE TRIGGER ${triggerName}
      AFTER INSERT ON ${queueTableName}
      FOR EACH ROW
      EXECUTE PROCEDURE ${triggerFunctionName}();
    `);

    logger.debug('Created queue table trigger');
  }

  private async startListening() {
    const client = await this.dbClient.createClient();

    client.on('notification', async message => {
      const [ rowId, tableName, operation, ] = message.payload?.split(':') ?? [];

      if (!rowId || !tableName || !operation || !isTriggerOperation(operation)) {
        return;
      }

      const notification: TQueueNotification = {
        rowId: Number(rowId),
        tableName,
        operation,
      };

      this.emit('queued', notification);
    });

    const notificationChannelName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_NOTIFICATION_CHANNEL,
      client.escapeIdentifier
    );

    await client.query(`LISTEN ${notificationChannelName}`);
  }
}
