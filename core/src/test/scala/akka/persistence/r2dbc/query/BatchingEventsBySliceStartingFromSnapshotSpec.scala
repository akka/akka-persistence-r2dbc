/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

/**
 * Runs the full EventsBySliceStartingFromSnapshotSpec suite again with `start-from-snapshot.batching.enabled = true`,
 * so PostgresSnapshotDao's batched `= ANY(?)` sequence-number lookup is exercised against a real database in CI.
 */
class BatchingEventsBySliceStartingFromSnapshotSpec
    extends EventsBySliceStartingFromSnapshotSpec(EventsBySliceStartingFromSnapshotSpec.batchingConfig)
