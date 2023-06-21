/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.internal

import java.time.Instant

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class StartingFromSnapshotStage[Event](
    snapshotSource: Source[EventEnvelope[Event], NotUsed],
    primarySource: Map[String, (Long, Instant)] => Source[EventEnvelope[Event], NotUsed])
    extends GraphStage[SourceShape[EventEnvelope[Event]]] {

  val out: Outlet[EventEnvelope[Event]] = Outlet("out")

  override val shape: SourceShape[EventEnvelope[Event]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) { self =>
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            val snapshotHandler = new SnapshotHandler
            setHandler(out, snapshotHandler)

            subFusingMaterializer.materialize(
              snapshotSource.toMat(snapshotHandler.subSink.sink)(Keep.left),
              inheritedAttributes)
          }
        })

      class SnapshotHandler extends OutHandler with InHandler {
        private var snapshotOffsets = Map.empty[String, (Long, Instant)]

        val subSink = new SubSinkInlet[EventEnvelope[Event]]("snapshots")
        subSink.pull()
        subSink.setHandler(this)

        override def onPull(): Unit = {
          subSink.pull()
        }

        override def onPush(): Unit = {
          val env = subSink.grab()
          snapshotOffsets =
            snapshotOffsets.updated(env.persistenceId, env.sequenceNr -> toTimestampOffset(env.offset).timestamp)
          push(out, env)
        }

        override def onUpstreamFinish(): Unit = {
          val primaryHandler = new PrimaryHandler
          self.setHandler(out, primaryHandler)

          subFusingMaterializer.materialize(
            primarySource(snapshotOffsets).toMat(primaryHandler.subSink.sink)(Keep.left),
            inheritedAttributes)
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          subSink.cancel(cause)
          completeStage()
        }
      }

      class PrimaryHandler extends OutHandler with InHandler {
        val subSink = new SubSinkInlet[EventEnvelope[Event]]("snapshots")
        subSink.pull()
        subSink.setHandler(this)

        override def onPull(): Unit = {
          subSink.pull()
        }

        override def onPush(): Unit = {
          push(out, subSink.grab())
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          subSink.cancel(cause)
          completeStage()
        }
      }

    }

}
