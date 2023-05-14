import { TOperation, TRIGGER_OPERATIONS } from './types';

export function validatePostgresString(str: string) {
  const match = /^([a-z-_])+$/.exec(str);

  return !!match;
}

export function isTriggerOperation(str: string): str is TOperation {
  return TRIGGER_OPERATIONS.includes(str as unknown as TOperation);
}
