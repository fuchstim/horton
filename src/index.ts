import DatabaseClient, { TConnectionOptions } from './database';
import Listener from './_listener';

export type TOptions = {
  connectionOptions: TConnectionOptions,
  reconciliationFrequency?: number
};

class Horton {
  dbClient: DatabaseClient;

  constructor(options: TOptions) {
    this.dbClient = new DatabaseClient(options.connectionOptions);
  }

  async connect(initialize = true) {
    await this.dbClient.connect();

    await this.teardown(); // TODO: Remove

    if (initialize) {
      await this.dbClient.initialize();
    }
  }

  async disconnect() {
    // Stop listening for queue events
    // Flush listeners
    // Disconnect database
    await this.dbClient.disconnect();
  }

  async teardown() {
    await this.dbClient.teardown();
  }

  createListener(tableName: string) {
    const listener = new Listener(this.dbClient, tableName);

    return listener;
  }
}

export default Horton;
