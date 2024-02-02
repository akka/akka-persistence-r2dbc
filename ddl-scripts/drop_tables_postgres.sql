DROP INDEX event_journal_slice_idx;
DROP INDEX snapshot_slice_idx;
DROP INDEX durable_state_slice_idx;
DROP TABLE IF EXISTS event_journal;
DROP TABLE IF EXISTS snapshot;
DROP TABLE IF EXISTS durable_state;
