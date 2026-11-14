/** Configured TCP keep-alive host suffixes (`net.tcpKeepAlive`). */
export type TcpKeepAliveSettings = { hosts?: string[]; initialDelayMs?: number };

let settingsReader: (() => TcpKeepAliveSettings | undefined) | undefined;

/**
 * The config runtime registers how to read `net.tcpKeepAlive`, so network code
 * reads it without importing the config graph (that import pulled config into
 * the native hook relay bundle and broke its size budget).
 */
export function registerTcpKeepAliveSettingsReader(
  reader: (() => TcpKeepAliveSettings | undefined) | undefined,
): void {
  settingsReader = reader;
}

/** Current keep-alive settings, or undefined when no config runtime is loaded. */
export function readTcpKeepAliveSettings(): TcpKeepAliveSettings | undefined {
  try {
    return settingsReader?.();
  } catch {
    return undefined;
  }
}
