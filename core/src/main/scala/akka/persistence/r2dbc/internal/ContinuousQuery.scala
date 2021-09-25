/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.util.OptionVal

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object ContinuousQuery {
  def apply[S, T](
      initialState: S,
      updateState: (S, T) => S,
      nextQuery: S => Option[Source[T, NotUsed]],
      threshold: Long,
      refreshInterval: FiniteDuration): Source[T, NotUsed] =
    Source.fromGraph(new ContinuousQuery[S, T](initialState, updateState, nextQuery, threshold, refreshInterval))

  private case object NextQuery
}

/**
 * INTERNAL API
 *
 * Keep running the Source's returned by `nextQuery` until None is returned Each time updating the state
 * @param initialState
 *   Initial state for first call to nextQuery
 * @param updateState
 *   Called for every element
 * @param nextQuery
 *   Called each time the previous source completes
 * @tparam S
 *   State type
 * @tparam T
 *   Element type
 */
@InternalApi
final private[r2dbc] class ContinuousQuery[S, T](
    initialState: S,
    updateState: (S, T) => S,
    nextQuery: S => Option[Source[T, NotUsed]],
    threshold: Long,
    refreshInterval: FiniteDuration)
    extends GraphStage[SourceShape[T]] {
  import ContinuousQuery._

  val out = Outlet[T]("spanner.out")
  override def shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogicWithLogging(shape) with OutHandler {
      var nextRow: OptionVal[T] = OptionVal.none[T]
      var sinkIn: SubSinkInlet[T] = _
      var state = initialState
      var nrElements = Long.MaxValue
      var subStreamFinished = false

      private def pushAndUpdateState(t: T): Unit = {
        state = updateState(state, t)
        nrElements += 1
        push(out, t)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case NextQuery => next()
      }

      def next(): Unit =
        if (nrElements <= threshold) {
          nrElements = Long.MaxValue
          scheduleOnce(NextQuery, refreshInterval)
        } else {
          nrElements = 0
          subStreamFinished = false
          val source = nextQuery(state)
          source match {
            case Some(source) =>
              sinkIn = new SubSinkInlet[T]("queryIn")
              sinkIn.setHandler(new InHandler {
                override def onPush(): Unit = {
                  if (!nextRow.isEmpty) {
                    throw new IllegalStateException(s"onPush called when we already have next row.")
                  }
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
              val graph = Source
                .fromGraph(source)
                .to(sinkIn.sink)
              interpreter.subFusingMaterializer.materialize(graph)
              sinkIn.pull()
            case None =>
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
            if (!subStreamFinished && !sinkIn.isClosed && !sinkIn.hasBeenPulled) {
              sinkIn.pull()
            }
        }

      setHandler(out, this)
    }
}
