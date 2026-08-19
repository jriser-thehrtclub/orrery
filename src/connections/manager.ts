import type { ConnectionConfig, DatabaseDriver } from "./drivers/base.js";
import { PostgresDriver } from "./drivers/postgres.js";
import { MySQLDriver } from "./drivers/mysql.js";
import { SQLiteDriver } from "./drivers/sqlite.js";
import { DuckDBDriver } from "./drivers/duckdb.js";
import { loadEnvFiles } from "./env.js";
import { loadConnectionFiles, loadConnectionFilesFromSource } from "./loader.js";
import type { DashboardSource } from "../sources/types.js";

const DRIVER_MAP: Record<string, new () => DatabaseDriver> = {
  postgres: PostgresDriver,
  postgresql: PostgresDriver,
  mysql: MySQLDriver,
  sqlite: SQLiteDriver,
  duckdb: DuckDBDriver,
};

export interface ConnectionInfo {
  name: string;
  type: string;
  connected: boolean;
  sourceFile: string;
}

export type HealthStatus = { ok: true } | { ok: false; error: string };

export class ConnectionManager {
  private connections = new Map<
    string,
    { driver: DatabaseDriver; config: ConnectionConfig; sourceFile: string }
  >();

  /**
   * Initialize all connections from YAML files in a directory.
   * Loads .env files from projectRoot, parses connection YAML files,
   * and registers each connection.
   */
  async init(
    connectionsDir: string,
    projectRoot?: string,
  ): Promise<void> {
    if (projectRoot) {
      loadEnvFiles(projectRoot);
    }

    const loaded = loadConnectionFiles(connectionsDir);

    for (const conn of loaded) {
      await this.register(conn.name, conn.config, conn.sourceFile);
    }

    // Non-fatal health check: warn on unreachable connections, don't crash
    await this.warnUnhealthy();
  }

  /**
   * Initialize all connections from a remote (or local) DashboardSource.
   * Connection YAML files are read through the source; env vars are still
   * resolved from local process.env.
   */
  async initFromSource(
    source: DashboardSource,
    projectRoot?: string,
  ): Promise<void> {
    if (projectRoot) {
      loadEnvFiles(projectRoot);
    }

    const loaded = await loadConnectionFilesFromSource(source);

    for (const conn of loaded) {
      await this.register(conn.name, conn.config, conn.sourceFile);
    }

    await this.warnUnhealthy();
  }

  private async warnUnhealthy(): Promise<void> {
    const health = await this.healthCheck();
    for (const [name, status] of health) {
      if (!status.ok) {
        console.warn(
          `Warning: Connection "${name}" health check failed: ${status.error}`,
        );
      }
    }
  }

  async register(
    name: string,
    config: ConnectionConfig,
    sourceFile: string = "<programmatic>",
  ): Promise<void> {
    const DriverClass = DRIVER_MAP[config.type];
    if (!DriverClass) {
      throw new Error(
        `Unknown database type: ${config.type}. Supported: ${Object.keys(DRIVER_MAP).join(", ")}`,
      );
    }
    const driver = new DriverClass();
    await driver.connect(config);
    this.connections.set(name, { driver, config, sourceFile });
  }

  get(name: string): DatabaseDriver {
    const entry = this.connections.get(name);
    if (!entry) {
      throw new Error(
        `Connection "${name}" not found. Available: ${[...this.connections.keys()].join(", ")}`,
      );
    }
    return entry.driver;
  }

  /**
   * Per-connection throttle for executeAll's concurrent dispatch. Not part of
   * ConnectionInfo since that's the sanitized public view (no config internals).
   */
  getMaxConcurrentQueries(name: string): number | undefined {
    return this.connections.get(name)?.config.max_concurrent_queries;
  }

  getConnection(name: string): ConnectionInfo | undefined {
    const entry = this.connections.get(name);
    if (!entry) return undefined;
    return {
      name,
      type: entry.config.type,
      connected: entry.driver.isConnected(),
      sourceFile: entry.sourceFile,
    };
  }

  listConnections(): ConnectionInfo[] {
    return [...this.connections.entries()].map(([name, entry]) => ({
      name,
      type: entry.config.type,
      connected: entry.driver.isConnected(),
      sourceFile: entry.sourceFile,
    }));
  }

  async healthCheck(): Promise<Map<string, HealthStatus>> {
    const results = new Map<string, HealthStatus>();
    for (const [name, entry] of this.connections) {
      try {
        // Use a lightweight query to verify the connection works
        const testSql =
          entry.config.type === "sqlite" || entry.config.type === "duckdb"
            ? "SELECT 1"
            : "SELECT 1";
        await entry.driver.query(testSql);
        results.set(name, { ok: true });
      } catch (err) {
        results.set(name, {
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
    return results;
  }

  async disconnectAll(): Promise<void> {
    for (const entry of this.connections.values()) {
      await entry.driver.disconnect();
    }
    this.connections.clear();
  }
}
