import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import { createTables } from './schema';
import { runMigrations } from './migrations';
import { runSeeds } from './seeds';
import { PgCompatDatabase } from './pgCompat';
import { Place, Tag } from '../types';

const dataDir = path.join(__dirname, '../../data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, 'travel.db');
const postgresUrl = process.env.SUPABASE_DB_URL || process.env.DATABASE_URL;
const isPostgresMode = !!postgresUrl;

type AnyDb = Database.Database | PgCompatDatabase;
let _db: AnyDb | null = null;

function ensurePostgresCompatibility(db: AnyDb): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS addons (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      type TEXT NOT NULL DEFAULT 'global',
      icon TEXT DEFAULT 'Puzzle',
      enabled INTEGER DEFAULT 0,
      config TEXT DEFAULT '{}',
      sort_order INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS vacay_plans (
      id BIGSERIAL PRIMARY KEY,
      owner_id INTEGER NOT NULL,
      block_weekends INTEGER DEFAULT 1,
      holidays_enabled INTEGER DEFAULT 0,
      holidays_region TEXT DEFAULT '',
      company_holidays_enabled INTEGER DEFAULT 1,
      carry_over_enabled INTEGER DEFAULT 1,
      weekend_days TEXT DEFAULT '0,6',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(owner_id)
    );

    CREATE TABLE IF NOT EXISTS budget_item_members (
      id BIGSERIAL PRIMARY KEY,
      budget_item_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      paid INTEGER NOT NULL DEFAULT 0,
      UNIQUE(budget_item_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS collab_message_reactions (
      id BIGSERIAL PRIMARY KEY,
      message_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      emoji TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(message_id, user_id, emoji)
    );

    CREATE TABLE IF NOT EXISTS collab_notes (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      category TEXT DEFAULT 'General',
      title TEXT NOT NULL,
      content TEXT,
      color TEXT DEFAULT '#6366f1',
      pinned INTEGER DEFAULT 0,
      website TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS collab_polls (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      question TEXT NOT NULL,
      options TEXT NOT NULL,
      multiple INTEGER DEFAULT 0,
      closed INTEGER DEFAULT 0,
      deadline TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS collab_poll_votes (
      id BIGSERIAL PRIMARY KEY,
      poll_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      option_index INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(poll_id, user_id, option_index)
    );

    CREATE TABLE IF NOT EXISTS collab_messages (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      text TEXT NOT NULL,
      reply_to INTEGER,
      deleted INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS invite_tokens (
      id BIGSERIAL PRIMARY KEY,
      token TEXT UNIQUE NOT NULL,
      max_uses INTEGER NOT NULL DEFAULT 1,
      used_count INTEGER NOT NULL DEFAULT 0,
      expires_at TEXT,
      created_by INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS packing_category_assignees (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      category_name TEXT NOT NULL,
      user_id INTEGER NOT NULL,
      UNIQUE(trip_id, category_name, user_id)
    );

    CREATE TABLE IF NOT EXISTS packing_templates (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      created_by INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS packing_template_categories (
      id BIGSERIAL PRIMARY KEY,
      template_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS packing_template_items (
      id BIGSERIAL PRIMARY KEY,
      category_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS packing_bags (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      color TEXT NOT NULL DEFAULT '#6366f1',
      weight_limit_grams INTEGER,
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS visited_countries (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      country_code TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(user_id, country_code)
    );

    CREATE TABLE IF NOT EXISTS bucket_list (
      id BIGSERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      lat DOUBLE PRECISION,
      lng DOUBLE PRECISION,
      country_code TEXT,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS trip_photos (
      id BIGSERIAL PRIMARY KEY,
      trip_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      immich_asset_id TEXT NOT NULL,
      shared INTEGER NOT NULL DEFAULT 1,
      added_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(trip_id, user_id, immich_asset_id)
    );

    CREATE TABLE IF NOT EXISTS file_links (
      id BIGSERIAL PRIMARY KEY,
      file_id INTEGER NOT NULL,
      reservation_id INTEGER,
      assignment_id INTEGER,
      place_id INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(file_id, reservation_id),
      UNIQUE(file_id, assignment_id),
      UNIQUE(file_id, place_id)
    );
  `);

  db.exec(`
    ALTER TABLE collab_messages ADD COLUMN IF NOT EXISTS deleted INTEGER DEFAULT 0;
    ALTER TABLE collab_notes ADD COLUMN IF NOT EXISTS website TEXT;
    ALTER TABLE day_assignments ADD COLUMN IF NOT EXISTS assignment_time TEXT;
    ALTER TABLE day_assignments ADD COLUMN IF NOT EXISTS assignment_end_time TEXT;
    ALTER TABLE trip_files ADD COLUMN IF NOT EXISTS note_id INTEGER;
    ALTER TABLE trip_files ADD COLUMN IF NOT EXISTS uploaded_by INTEGER;
    ALTER TABLE trip_files ADD COLUMN IF NOT EXISTS starred INTEGER DEFAULT 0;
    ALTER TABLE trip_files ADD COLUMN IF NOT EXISTS deleted_at TEXT;
    ALTER TABLE reservations ADD COLUMN IF NOT EXISTS reservation_end_time TEXT;
    ALTER TABLE reservations ADD COLUMN IF NOT EXISTS accommodation_id INTEGER;
    ALTER TABLE reservations ADD COLUMN IF NOT EXISTS metadata TEXT;
    ALTER TABLE places ADD COLUMN IF NOT EXISTS osm_id TEXT;
    ALTER TABLE packing_items ADD COLUMN IF NOT EXISTS weight_grams INTEGER;
    ALTER TABLE packing_items ADD COLUMN IF NOT EXISTS bag_id INTEGER;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_enabled INTEGER DEFAULT 0;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS mfa_secret TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS immich_url TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS immich_api_key TEXT;
    ALTER TABLE vacay_plans ADD COLUMN IF NOT EXISTS weekend_days TEXT DEFAULT '0,6';
  `);
}

function initDb(): void {
  if (_db) {
    if (!isPostgresMode) {
      try { _db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch (e) {}
    }
    try { _db.close(); } catch (e) {}
    _db = null;
  }

  if (isPostgresMode) {
    _db = new PgCompatDatabase(postgresUrl as string);
    createTables(_db as unknown as Database.Database);
    ensurePostgresCompatibility(_db);
    runSeeds(_db as unknown as Database.Database);
    console.log('[DB] Using Supabase/Postgres');
  } else {
    _db = new Database(dbPath);
    _db.exec('PRAGMA journal_mode = WAL');
    _db.exec('PRAGMA busy_timeout = 5000');
    _db.exec('PRAGMA foreign_keys = ON');

    createTables(_db);
    runMigrations(_db);
    runSeeds(_db);
    console.log('[DB] Using SQLite');
  }
}

initDb();

if (process.env.DEMO_MODE === 'true') {
  try {
    const { seedDemoData } = require('../demo/demo-seed');
    seedDemoData(_db);
  } catch (err: unknown) {
    console.error('[Demo] Seed error:', err instanceof Error ? err.message : err);
  }
}

const db = new Proxy({} as Database.Database, {
  get(_, prop: string | symbol) {
    if (!_db) throw new Error('Database connection is not available (restore in progress?)');
    const val = (_db as unknown as Record<string | symbol, unknown>)[prop];
    return typeof val === 'function' ? val.bind(_db) : val;
  },
  set(_, prop: string | symbol, val: unknown) {
    (_db as unknown as Record<string | symbol, unknown>)[prop] = val;
    return true;
  },
});

function closeDb(): void {
  if (_db) {
    if (!isPostgresMode) {
      try { _db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch (e) {}
    }
    try { _db.close(); } catch (e) {}
    _db = null;
    console.log('[DB] Database connection closed');
  }
}

function reinitialize(): void {
  console.log('[DB] Reinitializing database connection after restore...');
  if (_db) closeDb();
  initDb();
  console.log('[DB] Database reinitialized successfully');
}

interface PlaceWithCategory extends Place {
  category_name: string | null;
  category_color: string | null;
  category_icon: string | null;
}

interface PlaceWithTags extends Place {
  category: { id: number; name: string; color: string; icon: string } | null;
  tags: Tag[];
}

function getPlaceWithTags(placeId: number | string): PlaceWithTags | null {
  const place = _db!.prepare(`
    SELECT p.*, c.name as category_name, c.color as category_color, c.icon as category_icon
    FROM places p
    LEFT JOIN categories c ON p.category_id = c.id
    WHERE p.id = ?
  `).get(placeId) as PlaceWithCategory | undefined;

  if (!place) return null;

  const tags = _db!.prepare(`
    SELECT t.* FROM tags t
    JOIN place_tags pt ON t.id = pt.tag_id
    WHERE pt.place_id = ?
  `).all(placeId) as Tag[];

  return {
    ...place,
    category: place.category_id ? {
      id: place.category_id,
      name: place.category_name!,
      color: place.category_color!,
      icon: place.category_icon!,
    } : null,
    tags,
  };
}

interface TripAccess {
  id: number;
  user_id: number;
}

function canAccessTrip(tripId: number | string, userId: number): TripAccess | undefined {
  return _db!.prepare(`
    SELECT t.id, t.user_id FROM trips t
    LEFT JOIN trip_members m ON m.trip_id = t.id AND m.user_id = ?
    WHERE t.id = ? AND (t.user_id = ? OR m.user_id IS NOT NULL)
  `).get(userId, tripId, userId) as TripAccess | undefined;
}

function isOwner(tripId: number | string, userId: number): boolean {
  return !!_db!.prepare('SELECT id FROM trips WHERE id = ? AND user_id = ?').get(tripId, userId);
}

export { db, closeDb, reinitialize, getPlaceWithTags, canAccessTrip, isOwner, isPostgresMode };
