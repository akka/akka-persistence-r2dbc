/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.stream.javadsl.Source

// FIXME include this in Akka

/**
 * A plugin may optionally support this query by implementing this trait.
 */
trait CurrentEventsBySliceQuery extends ReadJournal {

  /**
   * Same type of query as [[EventsBySliceQuery.eventsBySlices]] but the event stream is completed immediately when it
   * reaches the end of the "result set". Depending on journal implementation, this may mean all events up to when the
   * query is started, or it may include events that are persisted while the query is still streaming results. For
   * eventually consistent stores, it may only include all events up to some point before the query is started.
   */
  def currentEventsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed]

  def sliceForPersistenceId(persistenceId: String): Int

  def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]]
}
