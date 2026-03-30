import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';
import { Client } from 'pg';

type SqliteTableRow = {
  name: string;
  sql: string;
};

type SqliteIndexRow = {
  name: string;
  tbl_name: string;
  sql: string | null;
};

function toPgIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function translateCreateTable(sqliteCreateSql: string): string {
  let sql = sqliteCreateSql;

  sql = sql.replace(/\bAUTOINCREMENT\b/gi, '');
  sql = sql.replace(/\bINTEGER\s+PRIMARY\s+KEY\b/gi, 'BIGSERIAL PRIMARY KEY');
  sql = sql.replace(/\bDATETIME\b/gi, 'TIMESTAMPTZ');
  sql = sql.replace(/\bREAL\b/gi, 'DOUBLE PRECISION');
  sql = sql.replace(/\bCURRENT_TIMESTAMP\b/gi, 'NOW()');

  return sql;
}

function tableHasSerialId(sqliteCreateSql: string): boolean {
  return /\bid\s+INTEGER\s+PRIMARY\s+KEY\b/i.test(sqliteCreateSql);
}

function buildInsertSql(tableName: string, columns: string[], rowCount: number): string {
  const quotedColumns = columns.map(toPgIdentifier).join(', ');
  const tuples: string[] = [];

  for (let row = 0; row < rowCount; row += 1) {
    const base = row * columns.length;
    const placeholders = columns.map((_, idx) => `$${base + idx + 1}`);
    tuples.push(`(${placeholders.join(', ')})`);
  }

  return `INSERT INTO ${toPgIdentifier(tableName)} (${quotedColumns}) VALUES ${tuples.join(', ')}`;
}

function flattenRows(rows: Array<Record<string, unknown>>, columns: string[]): unknown[] {
  const values: unknown[] = [];
  for (const row of rows) {
    for (const col of columns) {
      values.push(row[col] ?? null);
    }
  }
  return values;
}

async function main(): Promise<void> {
  const dbUrl = process.env.SUPABASE_DB_URL || process.env.DATABASE_URL;
  if (!dbUrl) {
    throw new Error('Missing SUPABASE_DB_URL (or DATABASE_URL) environment variable');
  }

  const sqlitePath = process.env.SQLITE_DB_PATH || path.join(__dirname, '../../data/travel.db');
  if (!fs.existsSync(sqlitePath)) {
    throw new Error(`SQLite database not found: ${sqlitePath}`);
  }

  console.log(`[MIGRATE] SQLite source: ${sqlitePath}`);
  console.log('[MIGRATE] Connecting to Supabase Postgres...');

  const sqlite = new Database(sqlitePath, { readonly: true });
  const pg = new Client({ connectionString: dbUrl, ssl: { rejectUnauthorized: false } });
  await pg.connect();

  try {
    const tables = sqlite
      .prepare(
        `
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
          AND sql IS NOT NULL
        ORDER BY name
      `
      )
      .all() as SqliteTableRow[];

    if (tables.length === 0) {
      throw new Error('No SQLite tables found to migrate');
    }

    console.log(`[MIGRATE] Found ${tables.length} tables`);

    await pg.query('BEGIN');
    await pg.query('SET session_replication_role = replica');

    const serialTables = new Set<string>();

    for (const table of tables) {
      const createSql = translateCreateTable(table.sql);
      if (tableHasSerialId(table.sql)) {
        serialTables.add(table.name);
      }
      await pg.query(createSql);
    }

    for (const table of tables) {
      const tableName = table.name;
      const pragma = sqlite.prepare(`PRAGMA table_info(${toPgIdentifier(tableName)})`).all() as Array<{ name: string }>;
      const columns = pragma.map((c) => c.name);

      if (columns.length === 0) {
        console.log(`[MIGRATE] Skipped empty schema table ${tableName}`);
        continue;
      }

      await pg.query(`TRUNCATE TABLE ${toPgIdentifier(tableName)} RESTART IDENTITY CASCADE`);

      const rows = sqlite.prepare(`SELECT * FROM ${toPgIdentifier(tableName)}`).all() as Array<Record<string, unknown>>;
      if (rows.length === 0) {
        console.log(`[MIGRATE] ${tableName}: 0 rows`);
        continue;
      }

      const chunkSize = 250;
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        const insertSql = buildInsertSql(tableName, columns, chunk.length);
        const values = flattenRows(chunk, columns);
        await pg.query(insertSql, values);
      }

      console.log(`[MIGRATE] ${tableName}: ${rows.length} rows`);
    }

    const indexes = sqlite
      .prepare(
        `
        SELECT name, tbl_name, sql
        FROM sqlite_master
        WHERE type = 'index'
          AND name NOT LIKE 'sqlite_autoindex_%'
          AND sql IS NOT NULL
        ORDER BY name
      `
      )
      .all() as SqliteIndexRow[];

    for (const index of indexes) {
      try {
        await pg.query(index.sql as string);
      } catch (err) {
        // Keep migration resilient: some SQLite index expressions are not valid in Postgres.
        console.warn(`[MIGRATE] Skipped index ${index.name}: ${(err as Error).message}`);
      }
    }

    for (const tableName of serialTables) {
      const setvalSql = `
        SELECT setval(
          pg_get_serial_sequence('${tableName}', 'id'),
          COALESCE((SELECT MAX(id) FROM ${toPgIdentifier(tableName)}), 1),
          (SELECT COUNT(*) > 0 FROM ${toPgIdentifier(tableName)})
        )
      `;
      await pg.query(setvalSql);
    }

    await pg.query('SET session_replication_role = DEFAULT');
    await pg.query('COMMIT');

    console.log('[MIGRATE] Migration completed successfully');
  } catch (err) {
    try {
      await pg.query('ROLLBACK');
    } catch {}
    throw err;
  } finally {
    sqlite.close();
    await pg.end();
  }
}

main().catch((err) => {
  console.error('[MIGRATE] Failed:', err instanceof Error ? err.message : err);
  process.exit(1);
});
