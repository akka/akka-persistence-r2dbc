/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object ContinuousQuery {
  def apply[S, T](
      initialState: S,
      updateState: (S, T) => S,
      delayNextQuery: S => Option[FiniteDuration],
      nextQuery: S => (S, Option[Source[T, NotUsed]]),
      beforeQuery: S => Option[Future[S]] = (_: S) => None,
      heartbeat: S => Option[T] = (_: S) => None): Source[T, NotUsed] =
    Source.fromGraph(
      new ContinuousQuery[S, T](initialState, updateState, delayNextQuery, nextQuery, beforeQuery, heartbeat))

  private case object NextQuery

  def adjustNextDelay(rowCount: Int, bufferSize: Int, fullDelay: FiniteDuration): Option[FiniteDuration] = {
    val lowerWatermark = math.max(1, bufferSize / 10)
    val upperWatermark = math.min(bufferSize - 1, bufferSize - bufferSize / 10)
    if (rowCount >= upperWatermark) None // immediately
    else if (rowCount <= lowerWatermark) Some(fullDelay)
    else Some(fullDelay / 2)
  }
}

/**
 * INTERNAL API
 *
 * Keep running the Source's returned by `nextQuery` until None is returned Each time updating the state
 * @param initialState
 *   Initial state for first call to nextQuery
 * @param updateState
 *   Called for every element
 * @param delayNextQuery
 *   Called when previous source completes
 * @param nextQuery
 *   Called each time the previous source completes
 * @param beforeQuery
 *   Called before each `nextQuery` to allow for async update of the state
 * @tparam S
 *   State type
 * @tparam T
 *   Element type
 */
@InternalApi
final private[r2dbc] class ContinuousQuery[S, T](
    initialState: S,
    updateState: (S, T) => S,
    delayNextQuery: S => Option[FiniteDuration],
    nextQuery: S => (S, Option[Source[T, NotUsed]]),
    beforeQuery: S => Option[Future[S]],
    heartbeat: S => Option[T])
    extends GraphStage[SourceShape[T]] {
  import ContinuousQuery._

  val out = Outlet[T]("continous.query.out")
  override def shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogicWithLogging(shape) with OutHandler {
      var nextRow: OptionVal[T] = OptionVal.none[T]
      var sinkIn: SubSinkInlet[T] = _
      var state = initialState
      var nrElements = Long.MaxValue
      var subStreamFinished = false

      private val beforeQueryCallback = getAsyncCallback[Try[S]] {
        case Success(newState) =>
          state = newState
          runNextQuery()
        case Failure(exc) =>
          failStage(exc)
      }

      private def pushAndUpdateState(t: T): Unit = {
        state = updateState(state, t)
        push(out, t)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case NextQuery => next()
      }

      def next(): Unit = {
        val delay =
          if (nrElements == Long.MaxValue) None
          else delayNextQuery(state)

        delay match {
          case Some(d) =>
            nrElements = Long.MaxValue
            scheduleOnce(NextQuery, d)
          case None =>
            nrElements = 0
            subStreamFinished = false

            beforeQuery(state) match {
              case None => runNextQuery()
              case Some(beforeQueryFuture) =>
                beforeQueryFuture.onComplete(beforeQueryCallback.invoke)(ExecutionContext.parasitic)
            }
        }
      }

      private def runNextQuery(): Unit = {
        nextQuery(state) match {
          case (newState, Some(source)) =>
            state = newState
            sinkIn = new SubSinkInlet[T]("queryIn")
            sinkIn.setHandler(new InHandler {
              override def onPush(): Unit = {
                if (!nextRow.isEmpty) {
                  throw new IllegalStateException(s"onPush called when we already have next row.")
                }
                nrElements += 1
                if (isAvailable(out)) {
                  val element = sinkIn.grab()
                  pushAndUpdateState(element)
                  sinkIn.pull()
                } else {
                  nextRow = OptionVal(sinkIn.grab())
                }
              }

              override def onUpstreamFinish(): Unit =
                if (nextRow.isDefined) {
                  // wait for the element to be pulled
                  subStreamFinished = true
                } else {
                  next()
                }
            })
            val sourceWithHeartbeat = heartbeat(newState) match {
              case None    => source
              case Some(h) => Source.single(h).concat(source)
            }
            val graph = Source
              .fromGraph(sourceWithHeartbeat)
              .to(sinkIn.sink)
            interpreter.subFusingMaterializer.materialize(graph)
            sinkIn.pull()
          case (newState, None) =>
            state = newState
            completeStage()
        }
      }

      override def preStart(): Unit =
        // eager pull
        next()

      override def onPull(): Unit =
        nextRow match {
          case OptionVal.Some(row) =>
            pushAndUpdateState(row)
            nextRow = OptionVal.none[T]
            if (subStreamFinished) {
              next()
            } else {
              if (!sinkIn.isClosed && !sinkIn.hasBeenPulled) {
                sinkIn.pull()
              }
            }
          case OptionVal.None =>
            if (!subStreamFinished && sinkIn != null && !sinkIn.isClosed && !sinkIn.hasBeenPulled) {
              sinkIn.pull()
            }
        }

      setHandler(out, this)
    }
}
