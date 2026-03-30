import { spawnSync } from 'child_process';

type SqlParams = unknown[] | Record<string, unknown> | undefined;

type QueryOutput = {
  rows: Array<Record<string, unknown>>;
  rowCount: number;
  command: string;
};

interface PreparedLike {
  run: (...params: unknown[]) => { changes: number; lastInsertRowid: number | null };
  get: (...params: unknown[]) => Record<string, unknown> | undefined;
  all: (...params: unknown[]) => Array<Record<string, unknown>>;
}

function normalizeWhitespace(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

function splitSqlStatements(sql: string): string[] {
  return sql
    .split(';')
    .map((s) => s.trim())
    .filter(Boolean);
}

function translateInsertOrReplace(sql: string): string {
  const pattern = /^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(["\w]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*$/i;
  const m = sql.match(pattern);
  if (!m) return sql;

  const table = m[1];
  const cols = m[2].split(',').map((c) => c.trim());
  const vals = m[3].trim();
  if (cols.length === 0) return sql;

  const conflictCol = cols[0];
  const updateCols = cols.slice(1);
  if (updateCols.length === 0) {
    return `INSERT INTO ${table} (${cols.join(', ')}) VALUES (${vals}) ON CONFLICT (${conflictCol}) DO NOTHING`;
  }

  const updates = updateCols.map((c) => `${c} = EXCLUDED.${c}`).join(', ');
  return `INSERT INTO ${table} (${cols.join(', ')}) VALUES (${vals}) ON CONFLICT (${conflictCol}) DO UPDATE SET ${updates}`;
}

function translateSqlBase(sql: string): string {
  let out = sql;

  out = out.replace(/\blast_insert_rowid\s*\(\s*\)\b/gi, 'NULL');
  out = out.replace(/\bAUTOINCREMENT\b/gi, '');
  out = out.replace(/\bINTEGER\s+PRIMARY\s+KEY\b/gi, 'BIGSERIAL PRIMARY KEY');
  out = out.replace(/\bDATETIME\b/gi, 'TIMESTAMPTZ');
  out = out.replace(/\bCURRENT_TIMESTAMP\b/gi, 'NOW()');
  out = out.replace(/\bREAL\b/gi, 'DOUBLE PRECISION');
  out = out.replace(/\s+REFERENCES\s+[\w"]+(?:\s*\([^)]+\))?(?:\s+ON\s+DELETE\s+\w+)?(?:\s+ON\s+UPDATE\s+\w+)?/gi, '');
  out = out.replace(/\bINSERT\s+OR\s+IGNORE\s+INTO\b/gi, 'INSERT INTO');
  if (/\bINSERT\s+INTO\b/i.test(out) && /\bON\s+CONFLICT\b/i.test(out) === false && /\bINSERT\s+OR\s+IGNORE\b/i.test(sql)) {
    out = `${out} ON CONFLICT DO NOTHING`;
  }
  out = translateInsertOrReplace(out);

  return out;
}

function bindSql(sql: string, params?: SqlParams): { sql: string; values: unknown[] } {
  const values: unknown[] = [];
  let idx = 1;

  if (Array.isArray(params)) {
    let p = 0;
    const rebound = sql.replace(/\?/g, () => {
      values.push(params[p++]);
      return `$${idx++}`;
    });
    return { sql: rebound, values };
  }

  const named = params ?? {};
  const rebound = sql
    .replace(/:(\w+)/g, (_all, name: string) => {
      values.push((named as Record<string, unknown>)[name]);
      return `$${idx++}`;
    })
    .replace(/\?/g, () => {
      values.push(undefined);
      return `$${idx++}`;
    });

  return { sql: rebound, values };
}

function translateForPostgres(sql: string, params?: SqlParams): { sql: string; values: unknown[] } {
  const compact = normalizeWhitespace(sql);

  if (compact.toUpperCase().startsWith('PRAGMA ')) {
    return { sql: '', values: [] };
  }

  const translated = translateSqlBase(compact);
  return bindSql(translated, params);
}

function asArrayParams(params: unknown[]): SqlParams {
  if (params.length === 1 && params[0] && typeof params[0] === 'object' && !Array.isArray(params[0])) {
    return params[0] as Record<string, unknown>;
  }
  return params;
}

const QUERY_RUNNER = `
const { Client } = require('pg');
(async () => {
  const client = new Client({ connectionString: process.env.PG_URL, ssl: { rejectUnauthorized: false } });
  await client.connect();
  const sql = process.env.PG_SQL || '';
  const values = JSON.parse(process.env.PG_VALUES || '[]');
  const result = await client.query(sql, values);
  await client.end();
  process.stdout.write(JSON.stringify({ rows: result.rows, rowCount: result.rowCount || 0, command: result.command || '' }));
})().catch(async (err) => {
  try {
    process.stderr.write(err && err.stack ? String(err.stack) : String(err));
  } catch {}
  process.exit(1);
});
`;

export class PgCompatDatabase {
  private readonly connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = this.normalizeConnectionString(connectionString);
    this.query('SELECT 1', []);
  }

  private normalizeConnectionString(raw: string): string {
    try {
      const url = new URL(raw);
      url.searchParams.delete('sslmode');
      url.searchParams.delete('ssl');
      url.searchParams.delete('sslcert');
      url.searchParams.delete('sslkey');
      url.searchParams.delete('sslrootcert');
      return url.toString();
    } catch {
      return raw;
    }
  }

  close(): void {
    // No persistent connection is held in process-based mode.
  }

  private query(sql: string, values: unknown[]): QueryOutput {
    const run = spawnSync(process.execPath, ['-e', QUERY_RUNNER], {
      env: {
        ...process.env,
        PG_URL: this.connectionString,
        PG_SQL: sql,
        PG_VALUES: JSON.stringify(values),
      },
      encoding: 'utf8',
      maxBuffer: 10 * 1024 * 1024,
    });

    if (run.status !== 0) {
      throw new Error((run.stderr || run.stdout || 'Postgres query failed').trim());
    }

    const raw = (run.stdout || '').trim();
    if (!raw) return { rows: [], rowCount: 0, command: '' };
    return JSON.parse(raw) as QueryOutput;
  }

  exec(sql: string): void {
    const statements = splitSqlStatements(sql);
    for (const stmt of statements) {
      const translated = translateForPostgres(stmt);
      if (!translated.sql) continue;
      try {
        this.query(translated.sql, translated.values);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const normalizedStmt = translated.sql.toUpperCase();
        if (normalizedStmt.startsWith('CREATE INDEX') && /relation .* does not exist/i.test(msg)) {
          continue;
        }
        throw err;
      }
    }
  }

  prepare(sql: string): PreparedLike {
    const runQuery = (params?: SqlParams): QueryOutput => {
      const translated = translateForPostgres(sql, params);
      if (!translated.sql) {
        return { rows: [], rowCount: 0, command: '' };
      }

      const upper = translated.sql.toUpperCase();
      const isInsert = upper.startsWith('INSERT ');
      const hasReturning = /\bRETURNING\b/i.test(translated.sql);
      const finalSql = isInsert && !hasReturning ? `${translated.sql} RETURNING id` : translated.sql;
      return this.query(finalSql, translated.values);
    };

    return {
      run: (...params: unknown[]) => {
        const result = runQuery(asArrayParams(params));
        const first = (result.rows[0] ?? {}) as { id?: number };
        return {
          changes: result.rowCount ?? 0,
          lastInsertRowid: typeof first.id === 'number' ? first.id : null,
        };
      },
      get: (...params: unknown[]) => {
        const result = runQuery(asArrayParams(params));
        return result.rows[0] as Record<string, unknown> | undefined;
      },
      all: (...params: unknown[]) => {
        const result = runQuery(asArrayParams(params));
        return result.rows as Array<Record<string, unknown>>;
      },
    };
  }

  transaction<TArgs extends unknown[]>(fn: (...args: TArgs) => void): (...args: TArgs) => void {
    return (...args: TArgs) => {
      // best-effort compatibility mode without shared session transaction
      fn(...args);
    };
  }
}
