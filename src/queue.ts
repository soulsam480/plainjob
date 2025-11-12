import type { Client } from "@libsql/client";
import cronParser from "cron-parser";
import {
  JobStatus,
  type Logger,
  type PersistedJob,
  type PersistedScheduledJob,
  ScheduledJobStatus,
} from "./jobs";

// biome-ignore lint/suspicious/noExplicitAny: this is fine
type VariableArgFunction = <T>(...params: any[]) => Promise<T>;

interface ISQLStatement {
  sql: string;
  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  args: any[];
  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  with: (...args: any[]) => ISQLStatement;
}

interface GenericStatement {
  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  run: (...params: any) => Promise<ITransactionResult>;
  get: VariableArgFunction;
  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  all: <T>(...params: any[]) => Promise<T[]>;
  stmt: ISQLStatement;
}

interface ITransactionResult {
  changes: number;
  lastInsertRowid: number | bigint;
  rows: unknown[];
}

export function sql(
  strings: TemplateStringsArray,
  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  ...interpolations: any[]
): ISQLStatement {
  let query = "";

  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  const args: any[] = [];

  // Build SQL string, replacing interpolations with '?'
  strings.forEach((part, i) => {
    query += part;
    if (i < interpolations.length) {
      query += "?";
      args.push(interpolations[i]);
    }
  });

  // Check if there are additional args after the template literal
  // e.g. sql`SELECT * FROM users WHERE id = ${id} AND active = ?`, true
  const extraArgs = Array.prototype.slice.call(
    // biome-ignore lint/complexity/noArguments: this is fine
    arguments,
    1 + interpolations.length,
  );

  if (extraArgs.length) {
    args.push(...extraArgs);
  }

  // Optional cleanup: remove redundant whitespace
  const sql = query.trim().replace(/\s+/g, " ");

  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  const mapper = (...child_args: any[]) => ({
    sql,
    args: [...args, ...child_args],
    with: mapper,
  });

  return {
    sql,
    args,
    with: mapper,
  };
}

export interface Connection {
  driver: string;
  pragma: (stmt: string | ISQLStatement) => Promise<void>;
  exec: (stmt: string | ISQLStatement) => Promise<void>;
  prepare: (stmt: ISQLStatement) => GenericStatement;
  transaction(stmts: Array<ISQLStatement>): Promise<ITransactionResult[]>;
  close: () => void;
}

function tapAndLogStatement(stmt: ISQLStatement) {
  // console.log(`[QUERY]: ${stmt.sql} with ${JSON.stringify(stmt.args)}`);

  return stmt;
}

export function libsql(database: Client): Connection {
  return {
    driver: "libsql",
    pragma: async (...args) => {
      try {
        await database.execute(...args);
      } catch {}
    },
    async transaction(stmts) {
      const res = await database.batch(stmts);

      return res.map((it) => ({
        changes: it.rowsAffected,
        lastInsertRowid: it.lastInsertRowid as unknown as number,
        rows: it.rows,
      }));
    },
    async exec(stmt) {
      await database.execute(stmt);
    },
    prepare(stmt) {
      return {
        // biome-ignore lint/suspicious/noExplicitAny: this is fine
        async get<T>(...args: any[]) {
          const res = await database.execute(
            tapAndLogStatement(stmt.with(...args)),
          );

          return res.rows[0] as unknown as T;
        },

        // biome-ignore lint/suspicious/noExplicitAny: this is fine
        async all<T>(...args: any[]) {
          const res = await database.execute(
            tapAndLogStatement(stmt.with(...args)),
          );

          return res.rows as unknown as T[];
        },

        // biome-ignore lint/suspicious/noExplicitAny: this is fine
        async run(...args: any[]) {
          const res = await database.execute(
            tapAndLogStatement(stmt.with(...args)),
          );

          return {
            changes: res.rowsAffected,
            lastInsertRowid: res.lastInsertRowid as unknown as number,
            rows: res.rows,
          };
        },
        stmt,
      } satisfies GenericStatement;
    },
    close: database.close.bind(database),
  };
}

export type QueueOptions = {
  /** A SQLite database connection, supports better-sqlite3 and bun:sqlite. */
  connection: Connection;
  /** An optional logger, defaults to console. Winston compatible. */
  logger?: Logger;
  /** Job gets re-queued after this time in milliseconds, defaults to 30min. */
  timeout?: number;
  /** Interval for maintenance tasks in milliseconds, defaults to 1 minute. */
  maintenanceInterval?: number;
  /** Remove done jobs older than this time in milliseconds, defaults to 7 days. */
  removeDoneJobsOlderThan?: number;
  /** Remove failed jobs older than this time in milliseconds, defaults to 30 days. */
  removeFailedJobsOlderThan?: number;
  /** Gets called when done jobs are removed by the maintenance task. */
  onDoneJobsRemoved?: (n: number) => void;
  /** Gets called when failed jobs are removed by the maintenance task. */
  onFailedJobsRemoved?: (n: number) => void;
  /** Gets called when timed out jobs are requeued by the maintenance task. */
  onProcessingJobsRequeued?: (n: number) => void;
  /** A function that serializes job data before storing it in the database. */
  serializer?: (data: unknown) => string;
};

/** A queue of jobs. */
export type Queue = {
  /** Adds a new job to the queue. */
  add: (
    type: string,
    data: unknown,
    options?: { delay?: number },
  ) => Promise<{ id: number }>;

  /** Adds multiple new jobs of the same type to the queue. */
  addMany: (
    type: string,
    data: unknown[],
    options?: { delay?: number },
  ) => Promise<{ ids: number[] }>;
  /** Schedules a recurring job using a cron expression. */
  schedule: (
    type: string,
    { cron }: { cron: string },
  ) => Promise<{ id: number }>;
  /** Counts jobs in the queue, optionally filtered by type and/or status. */
  countJobs: (opts?: { type?: string; status?: JobStatus }) => Promise<number>;
  /** Retrieves a job by its ID. */
  getJobById: (id: number) => Promise<PersistedJob | undefined>;
  /** Gets a list of all unique job types in the queue. */
  getJobTypes: () => Promise<string[]>;
  /** Retrieves all scheduled jobs. */
  getScheduledJobs: () => Promise<PersistedScheduledJob[]>;
  /** Retrieves a scheduled job by its ID. */
  getScheduledJobById: (
    id: number,
  ) => Promise<PersistedScheduledJob | undefined>;
  /** Requeues jobs that have exceeded the specified timeout. */
  requeueTimedOutJobs: (timeout: number) => Promise<void>;
  /** Removes completed jobs older than the specified duration. */
  removeDoneJobs: (olderThan: number) => Promise<void>;
  /** Removes failed jobs older than the specified duration. */
  removeFailedJobs: (olderThan: number) => Promise<void>;
  /** Fetches the next pending job of the specified type and marks it as processing. */
  getAndMarkJobAsProcessing: (
    type: string,
  ) => Promise<{ id: number } | undefined>;

  /** Fetches the next scheduled job due for execution and marks it as processing. */
  getAndMarkScheduledJobAsProcessing: () => Promise<
    PersistedScheduledJob | undefined
  >;
  /** Marks a job as completed. */
  markJobAsDone: (id: number) => Promise<void>;
  /** Marks a job as failed with an error message. */
  markJobAsFailed: (id: number, error: string) => Promise<void>;
  /** Marks a scheduled job as idle and sets its next execution time. */
  markScheduledJobAsIdle: (id: number, nextRunAt: number) => Promise<void>;
  /** Closes the queue, stopping maintenance tasks and releasing resources. */
  close: () => Promise<void>;
};

export async function setupQueueDeps(db: Connection) {
  await db.pragma("journal_mode = WAL");
  await db.pragma("busy_timeout = 5000");

  await db.transaction([
    sql`CREATE TABLE IF NOT EXISTS plainjob_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      data TEXT NOT NULL,
      status INTEGER DEFAULT 0 NOT NULL,
      failed_at INTEGER,
      error TEXT,
      next_run_at INTEGER,
      created_at INTEGER NOT NULL
    )`,
    sql`CREATE INDEX IF NOT EXISTS idx_jobs_status_type_next_run_at ON plainjob_jobs (status, type, next_run_at)`,
    sql`CREATE TABLE IF NOT EXISTS plainjob_scheduled_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL UNIQUE,
      status INTEGER DEFAULT 0 NOT NULL,
      cron_expression TEXT,
      next_run_at INTEGER,
      created_at INTEGER NOT NULL
    )`,
    sql`CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_status_type_next_run_at ON plainjob_scheduled_jobs (status, type, next_run_at)`,
  ]);
}

export function defineQueue(opts: QueueOptions): Queue {
  const db = opts.connection;
  const log = opts.logger || console;
  const jobProcessingTimeout = opts.timeout || 30 * 60 * 1000; // 30 minutes
  const maintenanceInterval = opts.maintenanceInterval || 60 * 1000; // 1 minute

  const removeDoneJobsOlderThan =
    opts.removeDoneJobsOlderThan || 7 * 24 * 60 * 60 * 1000; // 7 days

  const removeFailedJobsOlderThan =
    opts.removeFailedJobsOlderThan || 30 * 24 * 60 * 60 * 1000; // 30 days

  let maintenanceTimeout: Timer;

  const serializer = opts.serializer || ((data) => JSON.stringify(data));

  const removeDoneJobsStmt = db.prepare(sql`
    DELETE FROM plainjob_jobs
    WHERE status = ${JobStatus.Done} AND created_at < ?
  `);

  const removeFailedJobsStmt = db.prepare(sql`
    DELETE FROM plainjob_jobs
    WHERE status = ${JobStatus.Failed} AND failed_at < ?
  `);

  function initializeMaintenance() {
    maintenanceTimeout = setInterval(async () => {
      await queue.requeueTimedOutJobs(jobProcessingTimeout);
      await queue.removeDoneJobs(removeDoneJobsOlderThan);
      await queue.removeFailedJobs(removeFailedJobsOlderThan);
    }, maintenanceInterval);
  }

  const countJobsStmt = db.prepare(sql`SELECT COUNT(*) FROM plainjob_jobs`);

  const countJobsByTypeAndStatusStmt = db.prepare(
    sql`SELECT COUNT(*) FROM plainjob_jobs WHERE status = ? AND type = ?`,
  );

  const countJobsByStatusStmt = db.prepare(
    sql`SELECT COUNT(*) FROM plainjob_jobs WHERE status = ?`,
  );

  const countJobsByTypeStmt = db.prepare(
    sql`SELECT COUNT(*) FROM plainjob_jobs WHERE type = ?`,
  );

  const getJobTypesStmt = db.prepare(
    sql`SELECT DISTINCT type FROM plainjob_jobs`,
  );

  const getJobByIdStmt = db.prepare(sql`
    SELECT 
      id,
      type,
      data,
      status,
      created_at as createdAt,
      failed_at as failedAt,
      next_run_at as nextRunAt,
      error
    FROM plainjob_jobs 
    WHERE id = ?
  `);

  const getScheduledJobByIdStmt = db.prepare(sql`
    SELECT 
      id,
      type,
      status,
      created_at as createdAt,
      cron_expression as cronExpression,
      next_run_at as nextRunAt
    FROM plainjob_scheduled_jobs 
    WHERE id = ?
  `);

  const getScheduledJobByTypeStmt = db.prepare(sql`
    SELECT * FROM plainjob_scheduled_jobs WHERE type = ?
  `);

  const getScheduledJobsStmt = db.prepare(sql`
    SELECT 
      id,
      type,
      status,
      created_at as createdAt,
      cron_expression as cronExpression,
      next_run_at as nextRunAt
    FROM plainjob_scheduled_jobs
    ORDER BY created_at
  `);

  const insertJobStmt = db.prepare(
    sql`INSERT INTO plainjob_jobs (type, data, created_at, next_run_at) VALUES (?, ?, ?, ?) RETURNING id`,
  );

  const insertScheduledJobStmt = db.prepare(
    sql`INSERT INTO plainjob_scheduled_jobs (type, cron_expression, next_run_at, created_at) VALUES (?, ?, ?, ?) RETURNING id`,
  );

  const updateScheduledJobCronExpressionStmt = db.prepare(sql`
    UPDATE plainjob_scheduled_jobs SET cron_expression = ? WHERE id = ?
  `);

  const requeueTimedOutJobsStmt = db.prepare(sql`
    UPDATE plainjob_jobs SET status = ${JobStatus.Pending} WHERE status = ${JobStatus.Processing} AND next_run_at < ?
  `);

  const getJobIdToProcessNextStmt = db.prepare(sql`
    SELECT id, created_at FROM plainjob_jobs
    WHERE status = ${JobStatus.Pending} AND type = ? AND next_run_at <= ?
    ORDER BY next_run_at LIMIT 1
  `);

  const updateJobAsProcessingStmt = db.prepare(sql`
    UPDATE plainjob_jobs SET status = ${JobStatus.Processing}, next_run_at = ? WHERE id = ?
  `);

  const getNextScheduledJobStmt = db.prepare(sql`
    SELECT
      id,
      type,
      status,
      created_at as createdAt,
      cron_expression as cronExpression,
      next_run_at as nextRunAt
    FROM plainjob_scheduled_jobs
    WHERE status = ${ScheduledJobStatus.Idle} AND next_run_at <= ?
    ORDER BY next_run_at LIMIT 1
  `);

  const updateJobStatusStmt = db.prepare(sql`
    UPDATE plainjob_jobs SET status = ? WHERE id = ?
  `);

  const updateJobNextRunAtStmt = db.prepare(sql`
    UPDATE plainjob_jobs SET next_run_at = ? WHERE id = ?
  `);

  const updateScheduledJobStatusStmt = db.prepare(sql`
    UPDATE plainjob_scheduled_jobs SET status = ?, next_run_at = ? WHERE id = ?
  `);

  const failJobStmt = db.prepare(sql`
    UPDATE plainjob_jobs SET status = ${JobStatus.Failed}, failed_at = ?, error = ? WHERE id = ?
  `);

  const queue: Queue = {
    async add(
      type: string,
      data: unknown,
      options: { delay?: number } = {},
    ): Promise<{ id: number }> {
      const now = Date.now();

      const serializedData = serializer(data);

      const result = await insertJobStmt.run(
        type,
        serializedData,
        now,
        now + (options.delay ?? 0),
      );

      return { id: (result.rows[0] as PersistedJob)?.id as number };
    },
    async addMany(
      type: string,
      dataList: unknown[],
      options: { delay?: number } = {},
    ): Promise<{ ids: number[] }> {
      const now = Date.now();

      const res = await db.transaction(
        dataList.map((data) => {
          return insertJobStmt.stmt.with(
            type,
            serializer(data),
            now,
            now + (options.delay ?? 0),
          );
        }),
      );

      return { ids: res.map((it) => (it.rows[0] as PersistedJob)?.id) };
    },
    async schedule(
      type: string,
      { cron }: { cron: string },
    ): Promise<{ id: number }> {
      try {
        cronParser.parseExpression(cron);
      } catch (error) {
        throw new Error(
          `invalid cron expression provided: ${cron} ${
            (error as Error).message
          }`,
        );
      }

      const now = Date.now();

      const found = await getScheduledJobByTypeStmt.get<
        PersistedScheduledJob | undefined
      >(type);

      if (found) {
        log.debug(
          `updating existing scheduled job ${found.id} with cron expression ${cron}`,
        );

        await updateScheduledJobCronExpressionStmt.run(cron, found.id);

        return { id: found.id };
      }

      const result = await insertScheduledJobStmt.run(type, cron, 0, now);
      return { id: (result.rows[0] as PersistedJob)?.id };
    },
    async countJobs(opts?: {
      type?: string;
      status?: JobStatus;
    }): Promise<number> {
      let result: Record<"COUNT(*)", number>;

      if (opts?.type && opts?.status !== undefined) {
        result = await countJobsByTypeAndStatusStmt.get(opts.status, opts.type);
      } else if (opts?.status !== undefined && !opts?.type) {
        result = await countJobsByStatusStmt.get(opts.status);
      } else if (opts?.type && opts?.status === undefined) {
        result = await countJobsByTypeStmt.get(opts.type);
      } else {
        result = await countJobsStmt.get();
      }

      return result["COUNT(*)"];
    },
    async getJobById(id: number): Promise<PersistedJob | undefined> {
      return await getJobByIdStmt.get<PersistedJob | undefined>(id);
    },
    async getJobTypes(): Promise<string[]> {
      const result = await getJobTypesStmt.all<{ type: string }>();
      return result.map((row) => row.type);
    },
    async getScheduledJobs(): Promise<PersistedScheduledJob[]> {
      return await getScheduledJobsStmt.all<PersistedScheduledJob>();
    },
    async getScheduledJobById(
      id: number,
    ): Promise<PersistedScheduledJob | undefined> {
      return await getScheduledJobByIdStmt.get<
        PersistedScheduledJob | undefined
      >(id);
    },
    async requeueTimedOutJobs(timeout: number): Promise<void> {
      const now = Date.now();
      const result = await requeueTimedOutJobsStmt.run(now - timeout);

      log.debug(`requeued ${result.changes} timed out jobs`);

      if (opts.onProcessingJobsRequeued) {
        opts.onProcessingJobsRequeued(result.changes);
      }
    },
    async removeDoneJobs(olderThan: number): Promise<void> {
      const now = Date.now();
      const result = await removeDoneJobsStmt.run(now - olderThan);
      log.debug(`removed ${result.changes} done jobs`);
      if (opts.onDoneJobsRemoved) {
        opts.onDoneJobsRemoved(result.changes);
      }
    },
    async removeFailedJobs(olderThan: number): Promise<void> {
      const now = Date.now();
      const result = await removeFailedJobsStmt.run(now - olderThan);
      log.debug(`removed ${result.changes} failed jobs`);
      if (opts.onFailedJobsRemoved) {
        opts.onFailedJobsRemoved(result.changes);
      }
    },
    async getAndMarkJobAsProcessing(
      type: string,
    ): Promise<{ id: number } | undefined> {
      const result = await getJobIdToProcessNextStmt.get<
        { id: number } | undefined
      >(type, Date.now());

      if (!result?.id) {
        return undefined;
      }

      await updateJobAsProcessingStmt.run(
        Date.now() + jobProcessingTimeout,
        result.id,
      );

      return result;
    },
    async getAndMarkScheduledJobAsProcessing(): Promise<
      PersistedScheduledJob | undefined
    > {
      const now = Date.now();

      const job = await getNextScheduledJobStmt.get<
        PersistedScheduledJob | undefined
      >(now);

      if (!job) {
        return;
      }

      await updateScheduledJobStatusStmt.run(
        ScheduledJobStatus.Processing,
        job.nextRunAt,
        job.id,
      );

      return { ...job, status: ScheduledJobStatus.Processing };
    },
    async markJobAsDone(id: number): Promise<void> {
      await updateJobStatusStmt.run(JobStatus.Done, id);
      // Update next_run_at to current time when marking as done for proper age tracking
      await updateJobNextRunAtStmt.run(Date.now(), id);
    },
    async markJobAsFailed(id: number, error: string): Promise<void> {
      await failJobStmt.run(Date.now(), error, id);
    },
    async markScheduledJobAsIdle(id: number, nextRunAt: number): Promise<void> {
      await updateScheduledJobStatusStmt.run(
        ScheduledJobStatus.Idle,
        nextRunAt,
        id,
      );
    },
    async close(): Promise<void> {
      if (maintenanceTimeout) {
        clearInterval(maintenanceTimeout);
      }
      db.close();
    },
  };

  initializeMaintenance();

  return queue;
}
