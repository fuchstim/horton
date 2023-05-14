import { TOperation } from '../_database';
import { TRIGGER_OPERATIONS } from './constants';

export function validatePostgresString(str: string) {
  const match = /^([a-z-_])+$/.exec(str);

  return !!match;
}

export function isTriggerOperation(str: string): str is TOperation {
  return str in TRIGGER_OPERATIONS;
}
