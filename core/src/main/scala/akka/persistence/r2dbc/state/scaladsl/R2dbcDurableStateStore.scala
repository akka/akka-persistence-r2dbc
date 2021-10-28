/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.scaladsl.DurableStateStoreBySliceQuery
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.query.TimestampOffset
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao.SerializedStateRow
import akka.persistence.state.scaladsl.DurableStateUpdateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object R2dbcDurableStateStore {
  val Identifier = "akka.persistence.r2dbc.state"

  private object ChangesBySlicesState {
    val empty: ChangesBySlicesState =
      ChangesBySlicesState(TimestampOffset.Zero, 0, 0, 0, backtracking = false, TimestampOffset.Zero)
  }

  private final case class ChangesBySlicesState(
      latest: TimestampOffset,
      rowCount: Int,
      queryCount: Long,
      idleCount: Long,
      backtracking: Boolean,
      latestBacktracking: TimestampOffset) {

    def currentOffset: TimestampOffset =
      if (backtracking) latestBacktracking
      else latest

    def nextQueryFromTimestamp: Instant =
      if (backtracking) latestBacktracking.timestamp
      else latest.timestamp

    def nextQueryUntilTimestamp: Option[Instant] =
      if (backtracking) Some(latest.timestamp)
      else None
  }
}

class R2dbcDurableStateStore[A](system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateUpdateStore[A]
    with DurableStateStoreBySliceQuery[A] {
  import R2dbcDurableStateStore.ChangesBySlicesState
  import TimestampOffset.toTimestampOffset

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.state$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  import settings.maxNumberOfSlices

  private val typedSystem = system.toTyped
  private val serialization = SerializationExtension(system)
  private val stateDao =
    new DurableStateDao(
      settings,
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(sharedConfigPath + ".connection-factory"))(
      typedSystem.executionContext,
      typedSystem)

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val firstBacktrackingQueryWindow =
    backtrackingWindow.plus(JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis))

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    stateDao.readState(persistenceId).map {
      case None => GetObjectResult(None, 0L)
      case Some(serializedRow) =>
        val payload = serialization
          .deserialize(serializedRow.payload, serializedRow.serId, serializedRow.serManifest)
          .get
          .asInstanceOf[A]
        GetObjectResult(Some(payload), serializedRow.revision)
    }
  }

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = {
    val valueAnyRef = value.asInstanceOf[AnyRef]
    val serialized = serialization.serialize(valueAnyRef).get
    val serializer = serialization.findSerializerFor(valueAnyRef)
    val manifest = Serializers.manifestFor(serializer, valueAnyRef)
    val timestamp = System.currentTimeMillis()

    val serializedRow = SerializedStateRow(
      persistenceId,
      revision,
      DurableStateDao.EmptyDbTimestamp,
      DurableStateDao.EmptyDbTimestamp,
      timestamp,
      serialized,
      serializer.identifier,
      manifest)

    stateDao.writeState(serializedRow)

  }

  override def deleteObject(persistenceId: String): Future[Done] =
    stateDao.deleteState(persistenceId)

  override def sliceForPersistenceId(persistenceId: String): Int =
    SliceUtils.sliceForPersistenceId(persistenceId, maxNumberOfSlices)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    SliceUtils.sliceRanges(numberOfRanges, maxNumberOfSlices)

  override def currentChangesBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    implicit val ec: ExecutionContext = system.dispatcher

    def nextOffset(state: ChangesBySlicesState, change: DurableStateChange[A]): ChangesBySlicesState = {
      state.copy(latest = change.offset.asInstanceOf[TimestampOffset], rowCount = state.rowCount + 1)
    }

    def nextQuery(
        state: ChangesBySlicesState,
        toDbTimestamp: Instant): (ChangesBySlicesState, Option[Source[DurableStateChange[A], NotUsed]]) = {
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize - 1) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "currentChangesBySlices query [{}] from slices [{} - {}], from time [{}] to now [{}]. Found [{}] rows in previous query.",
            state.queryCount,
            minSlice,
            maxSlice,
            state.latest.timestamp,
            toDbTimestamp,
            state.rowCount)

        newState -> Some(
          stateDao
            .stateBySlices(
              entityTypeHint,
              minSlice,
              maxSlice,
              state.latest.timestamp,
              untilTimestamp = Some(toDbTimestamp),
              behindCurrentTime = Duration.Zero)
            .via(deserializeAndAddOffset(state.latest)))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "currentChangesBySlices query [{}] from slices [{} - {}] completed. Found [{}] rows in previous query.",
            state.queryCount,
            minSlice,
            maxSlice,
            state.rowCount)

        state -> None
      }
    }

    Source
      .futureSource[DurableStateChange[A], NotUsed] {
        stateDao.currentDbTimestamp().map { currentDbTime =>
          if (log.isDebugEnabled())
            log.debug(
              "currentChangesBySlices query slices [{} - {}], from time [{}] until now [{}].",
              minSlice,
              maxSlice,
              initialOffset.timestamp,
              currentDbTime)

          ContinuousQuery[ChangesBySlicesState, DurableStateChange[A]](
            initialState = ChangesBySlicesState.empty.copy(latest = initialOffset),
            updateState = nextOffset,
            delayNextQuery = _ => None,
            nextQuery = state => nextQuery(state, currentDbTime))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  override def changesBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    val someRefreshInterval = Some(settings.querySettings.refreshInterval)

    if (log.isDebugEnabled())
      log.debug(
        "Starting changesBySlices query from slices [{} - {}], from time [{}].",
        minSlice,
        maxSlice,
        initialOffset.timestamp)

    def nextOffset(state: ChangesBySlicesState, change: DurableStateChange[A]): ChangesBySlicesState = {
      if (state.backtracking) {
        val offset = change.offset.asInstanceOf[TimestampOffset]
        if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
          throw new IllegalArgumentException(
            s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

        state.copy(latestBacktracking = offset, rowCount = state.rowCount + 1)
      } else {
        val offset = change.offset.asInstanceOf[TimestampOffset]
        if (offset.timestamp.isBefore(state.latest.timestamp))
          throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

        state.copy(latest = offset, rowCount = state.rowCount + 1)
      }
    }

    def delayNextQuery(state: ChangesBySlicesState): Option[FiniteDuration] = {
      // FIXME verify that this is correct
      // the same row comes back and is filtered due to how the offset works
      val delay =
        if (0 <= state.rowCount && state.rowCount <= 1) someRefreshInterval
        else None // immediately if there might be more rows to fetch

      // TODO we could have different delays here depending on how many rows that were found.
      // e.g. a short delay if rowCount is < some threshold

      if (log.isDebugEnabled)
        delay.foreach { d =>
          log.debug(
            "changesBySlices query [{}] from slices [{} - {}] delay next [{}] ms.",
            state.queryCount,
            minSlice,
            maxSlice,
            d.toMillis)
        }

      delay
    }

    def nextQuery(
        state: ChangesBySlicesState): (ChangesBySlicesState, Option[Source[DurableStateChange[A], NotUsed]]) = {
      val newIdleCount = if (state.rowCount == 0) state.idleCount + 1 else 0
      val newState =
        if (settings.querySettings.backtrackingEnabled && !state.backtracking &&
          (newIdleCount >= 5 || JDuration
            .between(state.latestBacktracking.timestamp, state.latest.timestamp)
            .compareTo(halfBacktrackingWindow) > 0)) {
          // FIXME config for newIdleCount >= 5 and maybe something like `newIdleCount % 5 == 0`

          // switching to backtracking
          val fromOffset =
            if (state.latestBacktracking == TimestampOffset.Zero && initialOffset == TimestampOffset.Zero)
              TimestampOffset.Zero
            else if (state.latestBacktracking == TimestampOffset.Zero)
              TimestampOffset.Zero.copy(timestamp = initialOffset.timestamp.minus(firstBacktrackingQueryWindow))
            else
              state.latestBacktracking

          // FIXME the backtracking until offset is state.latest (not equal), but we should probably have an
          // additional lag to support async (at-least-once) projections without too many duplicates from
          // backtracking. For exactly-once I think all duplicates are filtered as expected.

          state.copy(
            rowCount = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtracking = true,
            latestBacktracking = fromOffset)
        } else if (state.backtracking && state.rowCount < settings.querySettings.bufferSize - 1) {
          // switch from backtracking
          state.copy(rowCount = 0, queryCount = state.queryCount + 1, idleCount = newIdleCount, backtracking = false)
        } else {
          state.copy(rowCount = 0, queryCount = state.queryCount + 1, idleCount = newIdleCount)
        }

      // FIXME for backtracking we could consider to use behind latest offset instead of behind current db time
      val behindCurrentTime =
        if (newState.backtracking) settings.querySettings.backtrackingBehindCurrentTime
        else settings.querySettings.behindCurrentTime

      if (log.isDebugEnabled())
        log.debug(
          "changesBySlices query [{}]{} from slices [{} - {}], from time [{}]. {}",
          newState.queryCount,
          if (newState.backtracking) " in backtracking mode" else "",
          minSlice,
          maxSlice,
          newState.nextQueryFromTimestamp,
          if (newIdleCount >= 3) s"Idle in [$newIdleCount] queries."
          else if (state.backtracking) s"Found [${state.rowCount}] rows in previous backtracking query."
          else s"Found [${state.rowCount}] rows in previous query.")

      newState ->
      Some(
        stateDao
          .stateBySlices(
            entityTypeHint,
            minSlice,
            maxSlice,
            newState.nextQueryFromTimestamp,
            newState.nextQueryUntilTimestamp,
            behindCurrentTime)
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    ContinuousQuery[ChangesBySlicesState, DurableStateChange[A]](
      initialState = ChangesBySlicesState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery)
  }

  private def deserializeAndAddOffset(
      timestampOffset: TimestampOffset): Flow[SerializedStateRow, DurableStateChange[A], NotUsed] = {
    Flow[SerializedStateRow].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      row => {
        def toEnvelope(offset: TimestampOffset): DurableStateChange[A] = {
          val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get.asInstanceOf[A]
          new UpdatedDurableState[A](row.persistenceId, row.revision, payload, offset, row.timestamp)
        }

        if (row.dbTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(row.persistenceId).exists(_ >= row.revision)) {
            log.debug(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              row.persistenceId,
              row.revision,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(row.persistenceId, row.revision)
            val offset =
              TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
            toEnvelope(offset) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = row.dbTimestamp
          currentSequenceNrs = Map(row.persistenceId -> row.revision)
          val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
          toEnvelope(offset) :: Nil
        }
      }
    }
  }

}
