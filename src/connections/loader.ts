import { readFileSync, readdirSync } from "fs";
import { resolve, extname } from "path";
import { parse as parseYaml } from "yaml";
import { resolveEnvVarsInConfig } from "./env.js";
import type { ConnectionConfig } from "./drivers/base.js";
import type { DashboardSource } from "../sources/types.js";

/** Raw shape of a single connection in YAML (before env resolution). */
interface RawConnectionEntry {
  name: string;
  type: string;
  [key: string]: unknown;
}

/** Shape of a multi-connection YAML file. */
interface MultiConnectionFile {
  connections: RawConnectionEntry[];
}

function isMultiConnectionFile(obj: unknown): obj is MultiConnectionFile {
  return (
    typeof obj === "object" &&
    obj !== null &&
    "connections" in obj &&
    Array.isArray((obj as MultiConnectionFile).connections)
  );
}

function isSingleConnectionEntry(obj: unknown): obj is RawConnectionEntry {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof (obj as RawConnectionEntry).name === "string" &&
    typeof (obj as RawConnectionEntry).type === "string"
  );
}

function rawEntryToConfig(entry: RawConnectionEntry): ConnectionConfig {
  const { name: _name, ...rest } = entry;
  const config: ConnectionConfig = {
    type: rest.type as string,
  };
  if (rest.host !== undefined) config.host = rest.host as string;
  if (rest.port !== undefined) config.port = Number(rest.port);
  if (rest.database !== undefined) config.database = rest.database as string;
  if (rest.username !== undefined) config.username = rest.username as string;
  if (rest.password !== undefined) config.password = rest.password as string;
  if (rest.path !== undefined) config.path = rest.path as string;
  if (rest.ssl !== undefined) config.ssl = Boolean(rest.ssl);
  if (rest.connection_string !== undefined)
    config.connection_string = rest.connection_string as string;
  if (rest.pool_size !== undefined) config.pool_size = Number(rest.pool_size);
  if (rest.max_concurrent_queries !== undefined)
    config.max_concurrent_queries = Number(rest.max_concurrent_queries);
  if (rest.timeout !== undefined) config.timeout = Number(rest.timeout);
  if (rest.options !== undefined)
    config.options = rest.options as Record<string, unknown>;
  return config;
}

export interface LoadedConnection {
  name: string;
  config: ConnectionConfig;
  sourceFile: string;
}

/**
 * Parse a single YAML connection file.
 * Supports both single-connection and multi-connection formats.
 * Env vars are resolved during parsing.
 */
export function parseConnectionFile(
  filePath: string,
  content: string,
): LoadedConnection[] {
  const parsed = parseYaml(content);

  if (parsed === null || parsed === undefined) {
    throw new Error(`Connection file '${filePath}' is empty`);
  }

  const results: LoadedConnection[] = [];

  if (isMultiConnectionFile(parsed)) {
    for (const entry of parsed.connections) {
      validateEntry(entry, filePath);
      const resolved = resolveEnvVarsInConfig(
        entry as unknown as Record<string, unknown>,
        entry.name,
      );
      results.push({
        name: entry.name,
        config: rawEntryToConfig(resolved as unknown as RawConnectionEntry),
        sourceFile: filePath,
      });
    }
  } else if (isSingleConnectionEntry(parsed)) {
    validateEntry(parsed, filePath);
    const resolved = resolveEnvVarsInConfig(
      parsed as unknown as Record<string, unknown>,
      parsed.name,
    );
    results.push({
      name: parsed.name,
      config: rawEntryToConfig(resolved as unknown as RawConnectionEntry),
      sourceFile: filePath,
    });
  } else {
    throw new Error(
      `Connection file '${filePath}' has invalid format. Expected a connection object with 'name' and 'type' fields, or an object with a 'connections' array.`,
    );
  }

  return results;
}

function validateEntry(entry: RawConnectionEntry, filePath: string): void {
  if (!entry.name || typeof entry.name !== "string") {
    throw new Error(
      `Connection in '${filePath}' is missing required 'name' field`,
    );
  }
  if (!entry.type || typeof entry.type !== "string") {
    throw new Error(
      `Connection '${entry.name}' in '${filePath}' is missing required 'type' field`,
    );
  }
}

/**
 * Load all connection YAML files from a directory.
 * Returns a flat array of all connections found across all files.
 */
export function loadConnectionFiles(connectionsDir: string): LoadedConnection[] {
  const files = readdirSync(connectionsDir).filter(
    (f) => extname(f) === ".yaml" || extname(f) === ".yml",
  );

  if (files.length === 0) {
    return [];
  }

  const allConnections: LoadedConnection[] = [];
  const seenNames = new Set<string>();

  for (const file of files.sort()) {
    const filePath = resolve(connectionsDir, file);
    const content = readFileSync(filePath, "utf-8");
    const connections = parseConnectionFile(filePath, content);

    for (const conn of connections) {
      if (seenNames.has(conn.name)) {
        throw new Error(
          `Duplicate connection name '${conn.name}' in '${conn.sourceFile}'. Connection names must be unique.`,
        );
      }
      seenNames.add(conn.name);
      allConnections.push(conn);
    }
  }

  return allConnections;
}

/**
 * Load all connection YAML files from a DashboardSource (remote or local).
 * Same deduplication rules as loadConnectionFiles.
 * Env vars (${VAR}) are still resolved from local process.env.
 */
export async function loadConnectionFilesFromSource(
  source: DashboardSource,
): Promise<LoadedConnection[]> {
  const files = await source.list();

  if (files.length === 0) {
    return [];
  }

  const allConnections: LoadedConnection[] = [];
  const seenNames = new Set<string>();

  for (const file of files.sort()) {
    const content = await source.read(file);
    const connections = parseConnectionFile(file, content);

    for (const conn of connections) {
      if (seenNames.has(conn.name)) {
        throw new Error(
          `Duplicate connection name '${conn.name}' in '${conn.sourceFile}'. Connection names must be unique.`,
        );
      }
      seenNames.add(conn.name);
      allConnections.push(conn);
    }
  }

  return allConnections;
}
