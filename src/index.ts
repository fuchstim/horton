import { TTableName, ETriggerOperation, TDatabaseConnectionOptions, TTableListener, TTableListenerOptions, TQueueRowId, TTriggerQueueRow } from './common/types';

import DatabaseClient from './_database';
import EventQueue from './_event-queue';
import { TypedEventEmitter } from './common/event-emitter';
import LivenessChecker from './_liveness-checker';

export type THortonOptions = {
  connectionOptions: TDatabaseConnectionOptions,
  tableListeners: Record<TTableName, TTableListenerOptions>,
  reconciliationFrequency?: number
};

export type THortonEvents = Record<`${keyof THortonOptions['tableListeners']}:${ETriggerOperation | '*'}`, object>;

class Horton extends TypedEventEmitter<THortonEvents> {
  private dbClient: DatabaseClient;
  private eventQueue: EventQueue;
  private livenessChecker: LivenessChecker;
  private tableListeners: TTableListener[];

  constructor(options: THortonOptions) {
    super({ captureRejections: false, });

    this.tableListeners = this.formatTableListenerOptions(options.tableListeners);

    this.dbClient = new DatabaseClient(options.connectionOptions);

    const reconciliationFrequency = options.reconciliationFrequency ?? 10_000;
    this.eventQueue = new EventQueue(this.dbClient, reconciliationFrequency);
    this.livenessChecker = new LivenessChecker(this.eventQueue, reconciliationFrequency + 5_000);
  }

  async connect(initializeQueue = true) {
    await this.teardown(); // TODO: Remove

    await this.dbClient.connect();
    await this.eventQueue.connect();

    if (initializeQueue) {
      await this.eventQueue.initialize();
    }

    await this.createListeners();

    this.livenessChecker.start();
  }

  async disconnect() {
    this.livenessChecker.stop();

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
          return {
            tableName,
            operations: config as ETriggerOperation[],
          };
        }

        return {
          ...config,
          tableName,
          operations: config.operations as ETriggerOperation[],
        };
      })
      .filter(({ operations, }) => operations.length);
  }

  private async createListeners() {
    await this.dbClient.transaction(async client => {
      for (const listenerConfig of this.tableListeners) {
        const { tableName, operations, } = listenerConfig;
        operations.forEach(
          operation => this.eventQueue.on(
            `queued:${tableName}:${operation}`,
            rowId => this.handleQueueNotification(rowId)
          )
        );

        await this.dbClient.createListenerTrigger(client, listenerConfig);
      }
    });
  }

  private async handleQueueNotification(rowId: TQueueRowId) {
    await this.eventQueue.dequeue<TTriggerQueueRow, unknown>(
      rowId,
      row => Promise.all([
        this.emitSync(`${row.tableName}:${row.operation}`, row),
        this.emitSync(`${row.tableName}:*`, row),
      ])
    );
  }
}

export default Horton;
