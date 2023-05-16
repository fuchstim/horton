import Logger from './common/logger';
const logger = Logger.ns('EventQueue');

import type DatabaseClient from './_database';
import { EBuiltinDatabaseObjectNames, EInternalOperation, ETriggerOperation, TQueueNotification, TQueueRow, TQueueRowId, TTableName } from './common/types';

import { PoolClient } from 'pg';
import EventEmitter from './common/event-emitter';
import { isInternalOperation, isTriggerOperation } from './common/utils';

export type TEventQueueEvents = Record<
  `internal:${EInternalOperation}` | `queued:${TTableName}:${ETriggerOperation}`,
  TQueueRowId
>;

export default class EventQueue extends EventEmitter<TEventQueueEvents> {
  private dbClient: DatabaseClient;
  private notificationListenerClient?: PoolClient;
  private reconciliationIntervalMs: number;
  private reconciliationTimer?: NodeJS.Timer;

  constructor(dbClient: DatabaseClient, reconciliationIntervalMs: number) {
    super();

    this.dbClient = dbClient;
    this.reconciliationIntervalMs = reconciliationIntervalMs;
  }

  async connect() {
    logger.debug('Connecting...');

    await this.startListening()
      .then(() => logger.info('Started listening'))
      .catch(error => logger.error('Failed to start listening', error));

    logger.debug(`Beginning reconciliation every ${this.reconciliationIntervalMs / 1_000}s`);
    this.reconciliationTimer = setInterval(
      () => this.reconcileQueuedEvents(),
      this.reconciliationIntervalMs
    );
  }

  async disconnect(gracePeriod = 5_000) {
    logger.debug('Disconnecting...');

    clearInterval(this.reconciliationTimer);

    this.stopListening();

    await new Promise(
      resolve => setTimeout(resolve, gracePeriod)
    );
  }

  async reconnect(cooldown = 5_000) {
    await this.disconnect(cooldown);

    await this.connect();
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

  async queue(row: Omit<TQueueRow, 'id'>) {
    await this.dbClient.transaction(async client => {
      const tableName = this.dbClient.prefixName(
        EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
        client.escapeIdentifier
      );

      await client.query(/* sql */ `
        INSERT INTO ${tableName} ("tableName", "operation", "previousRecord", "currentRecord", "queuedAt")
        VALUES ($1, $2, $3, $4, $5)
      `, [ row.tableName, row.operation, row.previousRecord, row.currentRecord, row.queuedAt, ]);
    });
  }

  async queueInternal(operation: EInternalOperation, metadata: object = {}) {
    await this.queue({
      tableName: this.dbClient.prefixName(EBuiltinDatabaseObjectNames.INTERNAL_PSEUDO_TABLE),
      operation,
      currentRecord: metadata,
      queuedAt: new Date(),
    });
  }

  async dequeue<T extends TQueueRow, R>(
    rowId: TQueueRowId,
    callback: (row: T) => R | Promise<R>
  ): Promise<R | undefined> {
    logger.debug(`Resolving row ${rowId}`);

    const result = await this.dbClient.transaction(async client => {
      const tableName = this.dbClient.prefixName(
        EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
        client.escapeIdentifier
      );

      const results = await client.query<T>(/* sql */`
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
    if (!this.notificationListenerClient) {
      this.notificationListenerClient = await this.dbClient.createClient();
    }

    const internalPseudoTableName = this.dbClient.prefixName(EBuiltinDatabaseObjectNames.INTERNAL_PSEUDO_TABLE);

    this.notificationListenerClient.on('notification', async message => {
      const [ rowId, tableName, operation, ] = message.payload?.split(':') ?? [];

      if (!rowId || !tableName || !operation) {
        return;
      }

      if (!isTriggerOperation(operation) && !isInternalOperation(operation)) {
        return;
      }

      const notification: TQueueNotification = {
        rowId: Number(rowId),
        tableName,
        operation,
        isInternal: tableName === internalPseudoTableName,
      };

      this.emitNotification(notification);
    });

    const notificationChannelName = this.dbClient.prefixName(
      EBuiltinDatabaseObjectNames.EVENT_QUEUE_NOTIFICATION_CHANNEL,
      this.notificationListenerClient.escapeIdentifier
    );

    await this.notificationListenerClient.query(`LISTEN ${notificationChannelName}`);
  }

  private emitNotification(notification: TQueueNotification) {
    const { rowId, tableName, operation, isInternal, } = notification;

    if (isInternal && isInternalOperation(operation)) {
      this.emit(`internal:${operation}`, rowId);
    }

    if (!isInternal && isTriggerOperation(operation)) {
      this.emit(`queued:${tableName}:${operation}`, rowId);
    }
  }

  private stopListening() {
    this.notificationListenerClient!.release(true);

    this.notificationListenerClient = undefined;
  }

  private async reconcileQueuedEvents() {
    logger.debug('Beginning reconciliation...');

    await this.dbClient.transaction(async client => {
      const pseudoTableName = this.dbClient.prefixName(
        EBuiltinDatabaseObjectNames.INTERNAL_PSEUDO_TABLE,
        client.escapeLiteral
      );

      const queueTableName = this.dbClient.prefixName(
        EBuiltinDatabaseObjectNames.EVENT_QUEUE_TABLE,
        client.escapeIdentifier
      );

      const data = await client.query<TQueueNotification>(/* sql */ `
        SELECT 
          id AS "rowId",
          "tableName",
          "operation",
          "tableName" = ${pseudoTableName} AS "isInternal"
        FROM ${queueTableName}
        ORDER BY "queuedAt" ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1000
      `);

      if (!data.rowCount) {
        logger.debug('Nothing to reconcile');

        return;
      }

      logger.debug(`${data.rowCount} queued events to reconcile`);

      data.rows.forEach(
        notification => this.emitNotification(notification)
      );
    });
  }
}
