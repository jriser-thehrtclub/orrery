import type { QueryResult } from "../../query/executor.js";

export interface ConnectionConfig {
  type: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  path?: string;
  ssl?: boolean;
  connection_string?: string;
  pool_size?: number;
  max_concurrent_queries?: number;
  timeout?: number;
  options?: Record<string, unknown>;
}

export interface DatabaseDriver {
  connect(config: ConnectionConfig): Promise<void>;
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
}
