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



-- data schema tables
CREATE SCHEMA data;
CREATE TABLE "data".talents (
  _id  VARCHAR(24) NOT NULL PRIMARY KEY,
  city VARCHAR(256)
);
CREATE TABLE "data".talent_jobs (
  talent_id VARCHAR(24),
  job_id    VARCHAR(24)
);
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data TO cs;
GRANT USAGE ON SCHEMA data TO cs;


-- ignore this
/*CREATE TABLE talents (
  _id  VARCHAR(24) NOT NULL PRIMARY KEY,
  city VARCHAR(256)
);

CREATE TABLE talent_jobs (
  talent_id VARCHAR(24),
  job_id    VARCHAR(24)
);
INSERT INTO talents VALUES ('58b7cc1009087d0c8aec003b', 'Pune');
INSERT INTO talent_jobs VALUES ('58b7cc1009087d0c8aec003b', '58aaa62309087db37ff3f44e');
INSERT INTO talent_jobs VALUES ('58b7cc1009087d0c8aec003b', '58c91b1609087d2d7033391d');
SELECT *
FROM data.talent_jobs AS tj, data.talents AS t
WHERE tj.talent_id = t._id AND t.city = 'Pune';

DROP TABLE talents;
DROP TABLE talent_jobs;*/
-- ignore ends


-- DROP TABLE oplog.mongo_oplog;
-- DROP DATABASE crowdstaffing;
-- DROP ROLE cs;