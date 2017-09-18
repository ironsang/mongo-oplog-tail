CREATE DATABASE crowdstaffing;
\c crowdstaffing;
CREATE SCHEMA oplog;
CREATE ROLE cs SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD 'secret';

CREATE TABLE "oplog".mongo_oplog (
  id         BIGSERIAL PRIMARY KEY,
  h          BIGINT,
  t          BIGINT,
  ts         BIGINT,
  v          INTEGER,
  op         CHAR(1),
  db         VARCHAR(32),
  collection VARCHAR(64),
  o          JSONB
);
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA oplog TO cs;
GRANT USAGE ON SCHEMA oplog TO cs;

-- DROP TABLE oplog.mongo_oplog;
-- DROP DATABASE crowdstaffing;
-- DROP ROLE cs;