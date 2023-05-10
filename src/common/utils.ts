export function validatePostgresString(str: string) {
  const match = /^([a-z-_])+$/.exec(str);

  return !!match;
}
