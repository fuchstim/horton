import Logger from './common/logger';
const logger = Logger.ns('EventQueue');

import type DatabaseClient from './_database';
import { EBuiltinDatabaseObjectNames, TQueueEvents, TQueueNotification, TQueueRow, TQueueRowId } from './common/types';

import { PoolClient } from 'pg';
import EventEmitter from './common/event-emitter';
import { isTriggerOperation } from './common/utils';

export default class EventQueue extends EventEmitter<TQueueEvents> {
  private dbClient: DatabaseClient;
  private reconciliationIntervalMs: number;
  private reconciliationTimer?: NodeJS.Timer;

  constructor(dbClient: DatabaseClient, reconciliationIntervalMs = 5_000) {
    super();

    this.dbClient = dbClient;
    this.reconciliationIntervalMs = reconciliationIntervalMs;
  }

  async connect() {
    await this.startListening()
      .then(() => logger.info('Started listening'))
      .catch(error => logger.error('Failed to start listening', error));

    logger.debug(`Beginning reconciliation every ${this.reconciliationIntervalMs / 1_000}s`);
    this.reconciliationTimer = setInterval(
      () => this.reconcileQueuedEvents(),
      this.reconciliationIntervalMs
    );
  }

  async disconnect() {
    clearInterval(this.reconciliationTimer);

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
    await this.disconnect();

    await this.dbClient.transaction(client => this.drop(client));
  }

  async dequeue<T>(rowId: TQueueRowId, callback: (row: TQueueRow) => T | Promise<T>): Promise<T | undefined> {
    logger.debug(`Resolving row ${rowId}`);

    const result = await this.dbClient.transaction(async client => {
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

      const [ row, ] = results.rows;

      const result = await Promise.resolve(
        callback.apply(callback, [ row, ])
      );

      await client.query(/* sql */`
        DELETE
        FROM ${tableName}
        WHERE id = $1
      `, [ rowId, ]);

      return result;
    });

    return result;
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

  private async reconcileQueuedEvents() {
    logger.debug('Beginning reconciliation...');

    await this.dbClient.transaction(async client => {
      const tableName = this.dbClient.prefixName(
        EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
        client.escapeIdentifier
      );

      const data = await client.query<TQueueNotification>(/* sql */ `
        SELECT id AS "rowId", "tableName", "operation"
        FROM ${tableName}
        ORDER BY "queuedAt" ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1000
      `);

      if (!data.rowCount) {
        logger.debug('Nothing to reconcile');

        return;
      }

      logger.debug(`${data.rowCount} queued events to reconcile`);

      data.rows.forEach(notification => this.emit('queued', notification));
    });
  }
}
