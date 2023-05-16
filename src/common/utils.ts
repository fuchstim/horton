import { EInternalOperation, ETriggerOperation } from './types';

export function validatePostgresString(str: string) {
  const match = /^([a-z-_])+$/.exec(str);

  return !!match;
}

export function isTriggerOperation(str: string): str is ETriggerOperation {
  return Object.values(ETriggerOperation).includes(str as ETriggerOperation);
}

export function isInternalOperation(str: string): str is EInternalOperation {
  return Object.values(EInternalOperation).includes(str as EInternalOperation);
}
