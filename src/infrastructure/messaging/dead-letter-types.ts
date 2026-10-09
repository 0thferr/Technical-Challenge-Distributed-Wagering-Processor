export const DEAD_LETTER_FAILURE_TYPES = [
  "INVALID_MESSAGE",
  "PERMANENT_FAILURE",
  "RETRY_EXHAUSTED",
  "UNKNOWN",
] as const;

export type DeadLetterFailureType = (typeof DEAD_LETTER_FAILURE_TYPES)[number];

export interface DeadLetterFailureMetadata {
  failureType: DeadLetterFailureType;
  failureCode: string;
  sourceQueue: string;
}
