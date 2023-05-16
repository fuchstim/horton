import { TTableName, TOperation, TDatabaseConnectionOptions, TQueueNotification, TTableListener, TTableListenerOptions } from './common/types';

import DatabaseClient from './_database';
import EventQueue from './_event-queue';
import { TypedEventEmitter } from './common/event-emitter';

export type THortonOptions = {
  connectionOptions: TDatabaseConnectionOptions,
  tableListeners: Record<TTableName, TTableListenerOptions>,
  reconciliationFrequency?: number
};

export type THortonEvents = Record<`${keyof THortonOptions['tableListeners']}:${TOperation | '*'}`, object>;

class Horton extends TypedEventEmitter<THortonEvents> {
  private dbClient: DatabaseClient;
  private eventQueue: EventQueue;
  private tableListeners: TTableListener[];

  constructor(options: THortonOptions) {
    super({ captureRejections: false, });

    this.tableListeners = this.formatTableListenerOptions(options.tableListeners);

    this.dbClient = new DatabaseClient(options.connectionOptions);
    this.eventQueue = new EventQueue(this.dbClient, options.reconciliationFrequency);

    this.eventQueue.on(
      'queued',
      notification => this.handleQueueNotification(notification)
    );
  }

  async connect(initializeQueue = true) {
    await this.dbClient.connect();
    await this.eventQueue.connect();

    await this.teardown(); // TODO: Remove
    await this.dbClient.connect();
    await this.eventQueue.connect();

    if (initializeQueue) {
      await this.eventQueue.initialize();
    }

    await this.syncListeners();
  }

  async disconnect() {
    // Stop listening for queue events
    // Flush listeners
    // Disconnect database
    await this.eventQueue.disconnect();
    await this.dbClient.disconnect();
  }

  async teardown() {
    await this.dbClient.teardown();
    await this.eventQueue.teardown();
  }

  private formatTableListenerOptions(options: THortonOptions['tableListeners']): TTableListener[] {
    return Object
      .entries(options)
      .map(([ tableName, config, ]) => {
        if (Array.isArray(config)) {
          return { tableName, operations: config, };
        }

        return { tableName, ...config, };
      })
      .filter(({ operations, }) => operations.length);
  }

  private async syncListeners() {
    await this.dbClient.transaction(async client => {
      for (const listenerConfig of this.tableListeners) {
        await this.dbClient.createListenerTrigger(client, listenerConfig);
      }
    });
  }

  private async handleQueueNotification(notification: TQueueNotification) {
    await this.eventQueue.dequeue(
      notification.rowId,
      row => Promise.all([
        this.emitSync(`${row.tableName}:${row.operation}`, row),
        this.emitSync(`${row.tableName}:*`, row),
      ])
    );
  }
}

export default Horton;
