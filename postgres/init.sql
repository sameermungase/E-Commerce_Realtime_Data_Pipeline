-- Initialize schemas for the data warehouse
-- raw: landing zone for PySpark-loaded data
-- analytics: dbt-built star schema models

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
