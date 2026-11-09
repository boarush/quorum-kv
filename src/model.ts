export interface Version {
  counter: number;
  writer: string;
}

export interface RecordValue {
  value: ArrayBuffer | null;
  version: Version;
  expiresAt: number | null;
}

export const EMPTY: RecordValue = {
  value: null,
  version: { counter: 0, writer: "" },
  expiresAt: null,
};

export function compare(a: Version, b: Version): number {
  return a.counter - b.counter || (a.writer > b.writer ? 1 : a.writer < b.writer ? -1 : 0);
}
