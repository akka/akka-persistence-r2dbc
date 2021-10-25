DROP INDEX event_journal_slice_idx;
DROP TABLE IF EXISTS event_journal CASCADE;
DROP TABLE IF EXISTS akka_projection_offset_store;
DROP TABLE IF EXISTS akka_projection_timestamp_offset_store;
DROP TABLE IF EXISTS akka_projection_management;
