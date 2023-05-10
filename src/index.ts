import { UUID, randomUUID } from 'crypto';

import DatabaseClient, { TOperation, TConnectionOptions } from './_db';

export type TOptions = {
  connectionOptions: TConnectionOptions,
  reconciliationFrequency?: number
};

class Horton {
  dbClient: DatabaseClient;
  private connected = false;
  private listeners: Record<UUID, { tableName: string, operations: TOperation[] }> = {};

  constructor(options: TOptions) {
    this.dbClient = new DatabaseClient(options.connectionOptions);
  }

  async connect() {
    await this.dbClient.connect();

    this.dbClient.onQueued(rowId => { this.onQueued(rowId); });

    await this.dbClient.teardown();
    await this.dbClient.initialize();

    this.connected = true;

    // create triggers
  }

  async disconnect() {
    await this.dbClient.disconnect();
  }

  async createListener(tableName: string, operations: TOperation[] = []) {
    if (!this.connected) {
      throw new Error('Must connect before creating listener');
    }

    if (!tableName || !operations.length) {
      throw new Error('Table name and operations must be specified');
    }

    const listenerId = randomUUID();

    this.listeners[listenerId] = { tableName, operations, };

    await this.syncListeners();

    return listenerId;
  }

  async removeListener(listenerId: UUID) {
    delete this.listeners[listenerId];

    await this.syncListeners();
  }

  private async syncListeners() {
    const requiredListeners = Object.values(this.listeners).reduce(
      (acc, { tableName, operations, }) => {
        if (!acc[tableName]) {
          acc[tableName] = [];
        }

        for (const operation of operations) {
          if (!acc[tableName].includes(operation)) {
            acc[tableName].push(operation);
          }
        }

        return acc;
      },
      {} as Record<string, TOperation[]>
    );

    await this.dbClient.syncListeners(requiredListeners);
  }

  private async onQueued(queueRowId: number) {
    debugger;
  }
}

export default Horton;
