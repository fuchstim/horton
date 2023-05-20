import { PoolConfig } from 'pg';

export enum ETriggerOperation {
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE',
}

export enum EInternalOperation {
  LIVENESS_PULSE = 'LIVENESS_PULSE'
}

export enum EBuiltinDatabaseObjectNames {
  EVENT_QUEUE_TABLE = 'event_queue',
  EVENT_QUEUE_TRIGGER = 'event_queue_trigger',
  EVENT_QUEUE_TRIGGER_FUNCTION = 'event_queue_trigger_function',
  EVENT_QUEUE_NOTIFICATION_CHANNEL = 'event_queue_notifications',

  INTERNAL_PSEUDO_TABLE = 'internal',
}

export type TTableName = string;
export type TColumnName = string;

export type TQueueRowId = number;

export type TQueueNotification = {
  rowId: TQueueRowId,
  tableName: TTableName,
  operation: EInternalOperation | ETriggerOperation,
  isInternal: boolean,
};

export type TQueueRow = {
  id: TQueueRowId,
  tableName: TTableName,
  operation: ETriggerOperation | EInternalOperation,
  previousRecord?: object,
  currentRecord: object,
  queuedAt: Date,
};

export type TTriggerQueueRow = TQueueRow & { operation: ETriggerOperation };
export type TInternalQueueRow = TQueueRow & { operation: EInternalOperation };

export type TTableTrigger = {
  tableName: TTableName,
  operations: ETriggerOperation[],
  recordColumns?: TColumnName[]
};

export type TTableListener = TTableTrigger;

export type TLivenessCheckerStatus = 'healthy' | 'unhealthy' | 'dead';
export type TLivenessCheckerEvents = Record<TLivenessCheckerStatus, { lastHeartbeatAt: Date }> & {
  heartbeat: { pulsedAt: Date, pulseLag: number }
};

// Public types
export type TDatabaseConnectionOptions = PoolConfig & { prefix?: string };

export type TSimpleTableListenerOptions = `${ETriggerOperation}`[];
export type TAdvancedTableListenerOptions = {
  operations: `${ETriggerOperation}`[]
  recordColumns: TColumnName[]
};
export type TTableListenerOptions = TSimpleTableListenerOptions | TAdvancedTableListenerOptions;

export type TEventQueueOptions = {
  reconciliationFrequencyMs?: number
};

export type TLivenessCheckerOptions = {
  pulseIntervalMs?: number,
  maxMissedPulses?: number
};
