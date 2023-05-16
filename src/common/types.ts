import { PoolConfig } from 'pg';

export const TRIGGER_OPERATIONS = [ 'INSERT', 'UPDATE', 'DELETE', ] as const;
export type TOperation = typeof TRIGGER_OPERATIONS[number];

export enum EBuiltinDatabaseObjectNames {
  EVENT_QUEUE_TABLE = 'event_queue',
  EVENT_QUEUE_TRIGGER = 'event_queue_trigger',
  EVENT_QUEUE_TRIGGER_FUNCTION = 'event_queue_trigger_function',
  EVENT_QUEUE_NOTIFICATION_CHANNEL = 'event_queue_notifications',
}

export type TTableName = string;
export type TColumnName = string;

export type TQueueRowId = number;

export type TQueueNotification = {
  rowId: TQueueRowId,
  tableName: TTableName,
  operation: TOperation
};

export type TQueueEvents = {
  queued: TQueueNotification
};

export type TQueueRow = {
  id: TQueueRowId,
  tableName: TTableName,
  operation: TOperation,
  previousRecord: object | undefined,
  currentRecord: object,
  queuedAt: Date,
};

export type TDatabaseConnectionOptions = PoolConfig & { prefix?: string };

export type TTableTrigger = {
  tableName: TTableName,
  operations: TOperation[],
  recordColumns?: TColumnName[]
};

export type TTableListener = TTableTrigger;

export type TSimpleTableListenerOptions = TOperation[];

export type TAdvancedTableListenerOptions = {
  operations: TOperation[]
  recordColumns: TColumnName[]
};

export type TTableListenerOptions = TSimpleTableListenerOptions | TAdvancedTableListenerOptions;
