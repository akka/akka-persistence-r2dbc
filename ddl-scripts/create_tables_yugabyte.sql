CREATE TABLE IF NOT EXISTS event_journal(
  slice INT NOT NULL,
  entity_type VARCHAR(255) NOT NULL,
  persistence_id VARCHAR(255) NOT NULL,
  seq_nr BIGINT NOT NULL,
  db_timestamp timestamp with time zone NOT NULL,

  event_ser_id INTEGER NOT NULL,
  event_ser_manifest VARCHAR(255) NOT NULL,
  event_payload BYTEA NOT NULL,

  deleted BOOLEAN DEFAULT FALSE NOT NULL,
  writer VARCHAR(255) NOT NULL,
  adapter_manifest VARCHAR(255),
  tags TEXT ARRAY,

  meta_ser_id INTEGER,
  meta_ser_manifest VARCHAR(255),
  meta_payload BYTEA,

  PRIMARY KEY(persistence_id HASH, seq_nr ASC)
);

-- `event_journal_slice_idx` is only needed if the slice based queries are used
CREATE INDEX IF NOT EXISTS event_journal_slice_idx ON event_journal(slice ASC, entity_type ASC, db_timestamp ASC, seq_nr ASC, persistence_id, deleted)
  SPLIT AT VALUES ((127), (255), (383), (511), (639), (767), (895));

CREATE TABLE IF NOT EXISTS snapshot(
  slice INT NOT NULL,
  entity_type VARCHAR(255) NOT NULL,
  persistence_id VARCHAR(255) NOT NULL,
  seq_nr BIGINT NOT NULL,
  db_timestamp timestamp with time zone,
  write_timestamp BIGINT NOT NULL,
  ser_id INTEGER NOT NULL,
  ser_manifest VARCHAR(255) NOT NULL,
  snapshot BYTEA NOT NULL,
  meta_ser_id INTEGER,
  meta_ser_manifest VARCHAR(255),
  meta_payload BYTEA,

  PRIMARY KEY(persistence_id HASH)
);

-- `snapshot_slice_idx` is only needed if the slice based queries are used together with snapshot as starting point
CREATE INDEX IF NOT EXISTS snapshot_slice_idx ON snapshot(slice ASC, entity_type ASC, db_timestamp ASC, persistence_id)
  SPLIT AT VALUES ((127), (255), (383), (511), (639), (767), (895));

CREATE TABLE IF NOT EXISTS durable_state (
  slice INT NOT NULL,
  entity_type VARCHAR(255) NOT NULL,
  persistence_id VARCHAR(255) NOT NULL,
  revision BIGINT NOT NULL,
  db_timestamp timestamp with time zone NOT NULL,

  state_ser_id INTEGER NOT NULL,
  state_ser_manifest VARCHAR(255),
  state_payload BYTEA NOT NULL,
  tags TEXT ARRAY,

  PRIMARY KEY(persistence_id HASH, revision ASC)
);

-- `durable_state_slice_idx` is only needed if the slice based queries are used
CREATE INDEX IF NOT EXISTS durable_state_slice_idx ON durable_state(slice ASC, entity_type ASC, db_timestamp ASC, revision ASC, persistence_id)
  SPLIT AT VALUES ((127), (255), (383), (511), (639), (767), (895));
