
export enum OnBadVectors {
  DROP = 1,
  ERROR = 2,
  FILL = 3,
}

export type Result<T, E = Error> =
  | { ok: true, value: T }
  | { ok: false, error: E }
