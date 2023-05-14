import { randomUUID } from 'crypto';
import DatabaseClient, { TOperation } from './database';
import EventEmitter from './common/event-emitter';
import { TRIGGER_OPERATIONS } from './common/constants';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TListenerEvents = Record<TOperation, any>;

export default class Listener extends EventEmitter<TListenerEvents> {
  readonly id: string = randomUUID();
  readonly tableName: string;

  private dbClient: DatabaseClient;

  constructor(dbClient: DatabaseClient, tableName: string) {
    super();

    this.tableName = tableName;
    this.dbClient = dbClient;

    this.on('error', this.handleError);
    this.on('newListener', this.handleNewListener);
  }

  private handleError(error: Error) {
    debugger;

    console.error(error);
  }

  private async handleNewListener(eventName: string) {
    if (eventName in TRIGGER_OPERATIONS) {
      // await this.dbClient.
    }
  }
}
