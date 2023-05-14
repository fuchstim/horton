export const TRIGGER_OPERATIONS = [ 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', ] as const;

export enum EBuiltinDatabaseObjectNames {
  EVENT_QUEUE_TABLE = 'event_queue',
  EVENT_QUEUE_TRIGGER = 'event_queue_trigger',
  EVENT_QUEUE_TRIGGER_FUNCTION = 'event_queue_trigger_function',
  EVENT_QUEUE_NOTIFICATION_CHANNEL = 'event_queue_notifications',
}
