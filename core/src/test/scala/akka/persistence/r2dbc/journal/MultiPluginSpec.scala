/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object MultiPluginSpec {
  val config: Config = ConfigFactory
    .parseString("""
    // #default-config
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    // #default-config

    // #second-config
    second-r2dbc = ${akka.persistence.r2dbc}
    second-r2dbc {
      # chose dialect unless using the same as the default
      # connection-factory = ${akka.persistence.r2dbc.postgres}
      connection-factory {
        # specific connection properties here

        # Note if using H2 and customizing table names you will need to repeat the custom table names
        # for the second config in this config block, see reference.conf for the table name config keys.
      }
      journal {
        # specific journal properties here
      }
      snapshot {
        # specific snapshot properties here
      }
      state {
        # specific durable state properties here
      }
      query {
        # specific query properties here
      }
    }
    // #second-config
    """)
    .withFallback(TestConfig.config)
    .resolve()

  object MyEntity {
    sealed trait Command
    final case class Persist(payload: String, replyTo: ActorRef[State]) extends Command
    type Event = String
    object State {
      def apply(): State = ""
    }
    type State = String

    def commandHandler(state: State, cmd: Command): Effect[Event, State] = {
      cmd match {
        case Persist(payload, replyTo) =>
          Effect
            .persist(payload)
            .thenReply(replyTo)(newState => newState)
      }
    }

    def eventHandler(state: State, evt: Event): State =
      state + evt

    def apply(persistenceId: PersistenceId): EventSourcedBehavior[Command, Event, State] = {
      // #withPlugins
      EventSourcedBehavior(persistenceId, emptyState = State(), commandHandler, eventHandler)
        .withJournalPluginId("second-r2dbc.journal")
        .withSnapshotPluginId("second-r2dbc.snapshot")
      // #withPlugins
    }
  }
}

class MultiPluginSpec
    extends ScalaTestWithActorTestKit(MultiPluginSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  import MultiPluginSpec.MyEntity

  override def typedSystem: ActorSystem[_] = system

  "Addition plugin config" should {

    "be supported for EventSourcedBehavior" in {
      val probe = createTestProbe[MyEntity.State]()
      val pid = PersistenceId.ofUniqueId(nextPid(nextEntityType()))
      val ref = spawn(MyEntity(pid))
      ref ! MyEntity.Persist("a", probe.ref)
      probe.expectMessage("a")
      ref ! MyEntity.Persist("b", probe.ref)
      probe.expectMessage("ab")
    }

  }
}
