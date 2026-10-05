/** A copied message; no native allocation survives receive(). */
export interface Message {
  readonly payload: Uint8Array;
  readonly source: string;
  readonly schema: bigint | undefined;
  readonly timestamp: bigint | undefined;
}

export class ReinyError extends Error {
  readonly status: number;
  constructor(message: string, status: number);
}

/** Select the native library before creating any objects. */
export function loadLibrary(path?: string): void;

export class LocalBus {
  private constructor();
  static new(): LocalBus;
  connect(id: string, domain?: string): Session;
  dispose(): void;
}

export class Session {
  private constructor();
  static open(id: string, domain?: string, zenohConfig?: string): Session;
  publisher(topic: string, schema?: bigint): Publisher;
  subscriber(topic: string, source?: string): Subscription;
  publishers(topic: string, timeoutMs: number): readonly string[];
  shutdown(): void;
  dispose(): void;
}

export class Publisher {
  private constructor();
  send(payload: Uint8Array): void;
  dispose(): void;
}

export class Subscription {
  private constructor();
  /** Blocks the calling thread until a message arrives or the deadline expires. */
  receive(timeoutMs: number): Message | undefined;
  dispose(): void;
}
