--DROP TABLE IF EXISTS event_journal CASCADE;

CREATE TABLE IF NOT EXISTS event_journal(
  slice INT NOT NULL,
  entity_type_hint VARCHAR(255) NOT NULL,
  persistence_id VARCHAR(255) NOT NULL,
  sequence_number BIGINT NOT NULL,
  db_timestamp timestamp with time zone NOT NULL,
  deleted BOOLEAN DEFAULT FALSE NOT NULL,

  writer VARCHAR(255) NOT NULL,
  write_timestamp BIGINT,
  adapter_manifest VARCHAR(255),

  event_ser_id INTEGER NOT NULL,
  event_ser_manifest VARCHAR(255) NOT NULL,
  event_payload BYTEA NOT NULL,

  meta_ser_id INTEGER,
  meta_ser_manifest VARCHAR(255),
  meta_payload BYTEA,

  PRIMARY KEY(slice, entity_type_hint, persistence_id, sequence_number)
);

CREATE INDEX event_journal_slice_idx ON event_journal(slice, entity_type_hint, db_timestamp);
