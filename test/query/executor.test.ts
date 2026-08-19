import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { QueryExecutor, QueryExecutionError } from "../../src/query/executor.js";
import { ConnectionManager } from "../../src/connections/manager.js";

describe("QueryExecutor (SQLite integration)", () => {
  let cm: ConnectionManager;
  let executor: QueryExecutor;

  beforeEach(async () => {
    cm = new ConnectionManager();
    await cm.register("test", { type: "sqlite", path: ":memory:" });

    // Seed test data
    const driver = cm.get("test");
    await driver.query(
      "CREATE TABLE orders (id INTEGER, region TEXT, amount REAL, created_at TEXT)",
    );
    await driver.query(
      "INSERT INTO orders VALUES (1, 'North', 100.0, '2024-01-15')",
    );
    await driver.query(
      "INSERT INTO orders VALUES (2, 'South', 200.0, '2024-02-10')",
    );
    await driver.query(
      "INSERT INTO orders VALUES (3, 'North', 150.0, '2024-03-05')",
    );

    executor = new QueryExecutor(cm);
  });

  afterEach(async () => {
    await cm.disconnectAll();
  });

  it("executes a simple query", async () => {
    const result = await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
    });
    expect(result.columns).toEqual(["id", "region", "amount", "created_at"]);
    expect(result.rowCount).toBe(3);
    expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
  });

  it("executes a parameterized query", async () => {
    const result = await executor.execute({
      sql: "SELECT * FROM orders WHERE region = {{region}}",
      connection: "test",
      params: { region: "North" },
    });
    expect(result.rowCount).toBe(2);
    expect(result.rows.every((r) => r.region === "North")).toBe(true);
  });

  it("handles daterange parameters with .start and .end", async () => {
    const result = await executor.execute({
      sql: "SELECT * FROM orders WHERE created_at >= {{dates.start}} AND created_at <= {{dates.end}}",
      connection: "test",
      params: { dates: { start: "2024-01-01", end: "2024-02-28" } },
    });
    expect(result.rowCount).toBe(2);
  });

  it("prevents SQL injection via parameterized queries", async () => {
    const result = await executor.execute({
      sql: "SELECT * FROM orders WHERE region = {{region}}",
      connection: "test",
      params: { region: "'; DROP TABLE orders; --" },
    });
    // No rows match the malicious string, table still exists
    expect(result.rowCount).toBe(0);

    // Table should still be intact
    const check = await executor.execute({
      sql: "SELECT COUNT(*) as cnt FROM orders",
      connection: "test",
    });
    expect(check.rows[0].cnt).toBe(3);
  });

  it("caches results when cacheTtl is set", async () => {
    const result1 = await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
      cacheTtl: 60,
    });
    expect(executor.cacheSize).toBe(1);

    // Second call should return cached result
    const result2 = await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
      cacheTtl: 60,
    });
    expect(result2).toEqual(result1);
  });

  it("does not cache when cacheTtl is 0 or unset", async () => {
    await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
    });
    expect(executor.cacheSize).toBe(0);
  });

  it("throws connection_not_found for unknown connection", async () => {
    try {
      await executor.execute({
        sql: "SELECT 1",
        connection: "nonexistent",
      });
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(QueryExecutionError);
      expect((err as QueryExecutionError).detail.type).toBe("connection_not_found");
    }
  });

  it("throws param_error for unknown parameter", async () => {
    try {
      await executor.execute({
        sql: "SELECT {{missing}}",
        connection: "test",
        params: {},
      });
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(QueryExecutionError);
      expect((err as QueryExecutionError).detail.type).toBe("param_error");
    }
  });

  it("throws row_limit_exceeded when result is too large", async () => {
    try {
      await executor.execute({
        sql: "SELECT * FROM orders",
        connection: "test",
        maxRows: 2,
      });
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(QueryExecutionError);
      const detail = (err as QueryExecutionError).detail;
      expect(detail.type).toBe("row_limit_exceeded");
      if (detail.type === "row_limit_exceeded") {
        expect(detail.limit).toBe(2);
        expect(detail.actual).toBe(3);
      }
    }
  });

  it("executes multiple queries in parallel via executeAll", async () => {
    const results = await executor.executeAll([
      { sql: "SELECT * FROM orders WHERE region = {{region}}", connection: "test", params: { region: "North" } },
      { sql: "SELECT * FROM orders WHERE region = {{region}}", connection: "test", params: { region: "South" } },
      { sql: "SELECT COUNT(*) as cnt FROM orders", connection: "test" },
    ]);

    expect(results.size).toBe(3);

    const r0 = results.get(0)!;
    expect(r0).not.toBeInstanceOf(QueryExecutionError);
    expect((r0 as { rowCount: number }).rowCount).toBe(2);

    const r1 = results.get(1)!;
    expect((r1 as { rowCount: number }).rowCount).toBe(1);

    const r2 = results.get(2)!;
    expect((r2 as { rows: Record<string, unknown>[] }).rows[0].cnt).toBe(3);
  });

  it("executeAll captures per-query errors without failing other queries", async () => {
    const results = await executor.executeAll([
      { sql: "SELECT * FROM orders", connection: "test" },
      { sql: "SELECT * FROM nonexistent_table", connection: "test" },
    ]);

    const r0 = results.get(0)!;
    expect(r0).not.toBeInstanceOf(QueryExecutionError);
    expect((r0 as { rowCount: number }).rowCount).toBe(3);

    const r1 = results.get(1)!;
    expect(r1).toBeInstanceOf(QueryExecutionError);
  });

  it("deduplicates identical in-flight queries", async () => {
    // Both promises use the same SQL+params, so only one execution should happen
    const [r1, r2] = await Promise.all([
      executor.execute({ sql: "SELECT * FROM orders", connection: "test" }),
      executor.execute({ sql: "SELECT * FROM orders", connection: "test" }),
    ]);
    expect(r1.rowCount).toBe(3);
    expect(r2.rowCount).toBe(3);
    // They should be the exact same object reference (shared result)
    expect(r1).toBe(r2);
  });

  it("clears cache", async () => {
    await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
      cacheTtl: 60,
    });
    expect(executor.cacheSize).toBe(1);
    executor.clearCache();
    expect(executor.cacheSize).toBe(0);
  });

  it("executeAll runs multiple queries and returns results", async () => {
    const results = await executor.executeAll([
      { sql: "SELECT COUNT(*) as cnt FROM orders", connection: "test" },
      { sql: "SELECT id FROM orders ORDER BY id LIMIT 1", connection: "test" },
    ]);
    expect(results.size).toBe(2);
    const r0 = results.get(0);
    expect(r0).toBeDefined();
    if (r0 && !(r0 instanceof Error)) {
      expect(r0.rows[0].cnt).toBe(3);
    }
  });

  it("executeAll captures errors without rejecting", async () => {
    const results = await executor.executeAll([
      { sql: "SELECT * FROM nonexistent_table", connection: "test" },
      { sql: "SELECT 1 as val", connection: "test" },
    ]);
    expect(results.size).toBe(2);
    // First query should be an error
    const r0 = results.get(0);
    expect(r0).toBeInstanceOf(QueryExecutionError);
    // Second query should succeed
    const r1 = results.get(1);
    expect(r1).not.toBeInstanceOf(Error);
  });

  it("invalidateByParams runs without error", async () => {
    await executor.execute({
      sql: "SELECT * FROM orders",
      connection: "test",
      cacheTtl: 60,
    });
    expect(executor.cacheSize).toBe(1);
    // invalidateByParams checks meta.sql which isn't stored yet,
    // so it won't clear entries — just verify it doesn't throw
    executor.invalidateByParams(["region"]);
    expect(executor.cacheSize).toBe(1);
    executor.clearCache();
  });

  it("throws on non-existent connection", async () => {
    await expect(
      executor.execute({ sql: "SELECT 1", connection: "no_such_conn" }),
    ).rejects.toThrow("not found");
  });
});

describe("executeAll concurrency throttle", () => {
  let cm: ConnectionManager;

  afterEach(async () => {
    await cm.disconnectAll();
  });

  /** Stubs `execute` with a timed, concurrency-counting fake so we can
   * observe dispatch shape without depending on a driver that's actually
   * async under the hood (better-sqlite3 is synchronous). */
  function trackConcurrency(executor: QueryExecutor) {
    const current = new Map<string, number>();
    const max = new Map<string, number>();
    const spy = vi
      .spyOn(executor, "execute")
      .mockImplementation(async (opts) => {
        const conn = opts.connection;
        const next = (current.get(conn) ?? 0) + 1;
        current.set(conn, next);
        max.set(conn, Math.max(max.get(conn) ?? 0, next));
        await new Promise((resolve) => setTimeout(resolve, 20));
        current.set(conn, next - 1);
        return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
      });
    return { spy, max };
  }

  it("dispatches fully in parallel when max_concurrent_queries is unset", async () => {
    cm = new ConnectionManager();
    await cm.register("unthrottled", { type: "sqlite", path: ":memory:" });
    const executor = new QueryExecutor(cm);
    const { max } = trackConcurrency(executor);

    await executor.executeAll(
      Array.from({ length: 4 }, () => ({
        sql: "SELECT 1",
        connection: "unthrottled",
      })),
    );

    expect(max.get("unthrottled")).toBe(4);
  });

  it("caps concurrent dispatch at the connection's max_concurrent_queries", async () => {
    cm = new ConnectionManager();
    await cm.register("throttled", {
      type: "sqlite",
      path: ":memory:",
      max_concurrent_queries: 2,
    });
    const executor = new QueryExecutor(cm);
    const { max } = trackConcurrency(executor);

    await executor.executeAll(
      Array.from({ length: 5 }, () => ({
        sql: "SELECT 1",
        connection: "throttled",
      })),
    );

    expect(max.get("throttled")).toBe(2);
  });

  it("throttles per-connection without affecting other connections in the same batch", async () => {
    cm = new ConnectionManager();
    await cm.register("slow", {
      type: "sqlite",
      path: ":memory:",
      max_concurrent_queries: 1,
    });
    await cm.register("fast", { type: "sqlite", path: ":memory:" });
    const executor = new QueryExecutor(cm);
    const { max } = trackConcurrency(executor);

    await executor.executeAll([
      { sql: "SELECT 1", connection: "slow" },
      { sql: "SELECT 1", connection: "slow" },
      { sql: "SELECT 1", connection: "fast" },
      { sql: "SELECT 1", connection: "fast" },
      { sql: "SELECT 1", connection: "fast" },
    ]);

    expect(max.get("slow")).toBe(1);
    expect(max.get("fast")).toBe(3);
  });
});
