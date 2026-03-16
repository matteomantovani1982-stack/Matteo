-- docker/postgres-init.sql
-- Runs once on first postgres container start.
-- Creates extensions needed by the app.

-- UUID generation (used by all models as primary key default)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Full-text search (reserved for future CV search feature)
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
