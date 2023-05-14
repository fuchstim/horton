import DatabaseClient, { TConnectionOptions, TOperation } from './_database';
import EventQueue, { TQueueNotification } from './_event-queue';
import { TypedEventEmitter } from './common/event-emitter';

export type TOptions = {
  connectionOptions: TConnectionOptions,
  listeners: Record<string, boolean | TOperation[]>,
  reconciliationFrequency?: number
};

export type THortonEvents = Record<`${keyof TOptions['listeners']}:${TOperation | '*'}`, object>;

class Horton extends TypedEventEmitter<THortonEvents> {
  private dbClient: DatabaseClient;
  private eventQueue: EventQueue;

  constructor(options: TOptions) {
    super({ captureRejections: false, });

    this.dbClient = new DatabaseClient(options.connectionOptions);
    this.eventQueue = new EventQueue(this.dbClient);

    this.eventQueue.on('queued', this.handleQueueNotification);
  }

  async connect(initializeQueue = true) {
    await this.dbClient.connect();
    await this.eventQueue.connect();

    await this.teardown(); // TODO: Remove

    if (initializeQueue) {
      await this.eventQueue.initialize();
    }
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

  private async handleQueueNotification(notification: TQueueNotification) {
    await this.dbClient.transaction(async client => {
      const row = await this.eventQueue.resolveRow(client, notification.rowId);
      if (!row) { return; }

      await Promise.all([
        this.emitSync(`${row.tableName}:${row.operation}`, row),
        this.emitSync(`${row.tableName}:*`, row),
      ]);
    });
  }
}

export default Horton;
