# Design notes

One of the primary goals with this Akka Persistence plugin is to have a design that is efficient for Postgres compatible distributed SQL databases like Yugabyte or Cockroach. It should also work well with ordinary Postgres.

## Problems with eventsByTag

The usage of `eventsByTag` for Projections has the major drawback that the number of tags must be decided up-front and can't easily be changed afterwards. Starting with too many tags means much overhead since many projection instances would be running on each node in a small Akka Cluster. Each projection instance polling the database periodically. Starting with too few tags means that it can't be scaled later to more Akka nodes.

## Introducing event slicing

Instead of tags we can store a slice number by hashing the persistence id. Like `math.abs(persistenceId.hashCode % numberOfSlices)`.

Then the Projection query can be a range query of the slices. For example if using 128 slices and running 4 Projection instances the slice ranges would be 0-31, 32-63, 64-95, 96-128. That can easily be split to more Projection instances when needed and still reuse the offsets for the previous range distributions.

## Offset

What offset shall be used for the Projection queries? A database sequence is not good because it becomes a single point of bottleneck on the write path, and it doesn't have much guarantees for monotonically increasing without gaps anyway.

A rich offset that tracked sequence numbers for persistence id would be very useful for deduplication. Then the offset itself doesn't have to be very exact since we can scan back in time for potentially missed events. That would also make it easier to have a live feed with Akka messages of the events directly from the write-side to the Projection, which would reduce the need for frequently polling the database.

That rich offset can be stored in a database table with one row per persistence id. It can be capped to a time window. For quick deduplication it would also have a cache in memory of all or part of that time window.

With such sequence number tracking in place the `eventsBySlices` query can use an ordinary database timestamp as the offset.

Using `transaction_timestamp()` as this timestamp based offset has a few challenges:

* The `transaction_timestamp()` is the time when the transaction started, not when it was committed. This means that a "later" event may be visible first and when retrieving events after the previously seen timestamp we may miss some events.
* In distributed SQL databases there can also be clock skews for the database timestamps.
* There can be more than one event per timestamp.

This means that it would be possible to miss events when tailing the end of the event log, but it can perform additional backtracking queries to catch missed events since the deduplication will filter out already processed events.

## Secondary index for eventsBySlices

The range query for `eventsBySlices` would look something like

```sql
SELECT * FROM event_journal
  WHERE entity_type = $1
  AND slice BETWEEN $2 AND $2
  AND db_timestamp >= $3
  AND db_timestamp < transaction_timestamp() - interval '200 milliseconds'
  ORDER BY db_timestamp, seq_nr
```

That would need a secondary index like:

```sql
CREATE INDEX IF NOT EXISTS event_journal_slice_idx ON event_journal(slice, entity_type, db_timestamp)
```

An alternative to `slice BETWEEN` would be `slice in (0, 1, 2,.., 31)`.

## Changing slice ranges

When changing the number of Projection instances it is important that a given slice is not running at more than one place. The rich offset table can include the slice number so that a new range distribution can continue with the offsets of the previous distribution. Once again, the exact deduplication is important.
