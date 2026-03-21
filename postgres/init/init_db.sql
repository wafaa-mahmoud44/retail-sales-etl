-- ============================================================
-- File: init_db.sql
-- Purpose: Runs automatically when PostgreSQL container starts
--          (placed in postgres/init/ volume)
-- ============================================================

-- Create the data warehouse database (if not exists)
SELECT 'CREATE DATABASE retail_dwh'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'retail_dwh'
)\gexec

-- Create the airflow user with a password
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
   ) THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$$;

-- Grant all privileges on the new database
GRANT ALL PRIVILEGES ON DATABASE retail_dwh TO airflow;
