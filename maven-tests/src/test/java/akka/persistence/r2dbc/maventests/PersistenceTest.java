/*
 * Copyright (C) 2026 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.maventests;

import static org.junit.Assert.assertEquals;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PersistenceTest {

  interface Command {}

  static final class Add implements Command {
    final String item;
    final ActorRef<String> replyTo;

    Add(String item, ActorRef<String> replyTo) {
      this.item = item;
      this.replyTo = replyTo;
    }
  }

  static final class Get implements Command {
    final ActorRef<List<String>> replyTo;

    Get(ActorRef<List<String>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  static final class Items extends EventSourcedBehavior<Command, String, List<String>> {
    Items(PersistenceId persistenceId) {
      super(persistenceId);
    }

    @Override
    public List<String> emptyState() {
      return new ArrayList<>();
    }

    @Override
    public CommandHandler<Command, String, List<String>> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(
              Add.class,
              add -> Effect().persist(add.item).thenReply(add.replyTo, state -> add.item))
          .onCommand(
              Get.class, (state, get) -> Effect().reply(get.replyTo, new ArrayList<>(state)))
          .build();
    }

    @Override
    public EventHandler<List<String>, String> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onAnyEvent(
              (state, item) -> {
                state.add(item);
                return state;
              });
    }
  }

  private static final Duration timeout = Duration.ofSeconds(20);
  private static ActorSystem<Void> system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create(Behaviors.empty(), "PersistenceTest");
  }

  @AfterClass
  public static void teardown() {
    system.terminate();
  }

  private <T> T ask(ActorRef<Command> ref, Function<ActorRef<T>, Command> createCommand)
      throws Exception {
    return AskPattern.ask(ref, createCommand::apply, timeout, system.scheduler())
        .toCompletableFuture()
        .get(timeout.getSeconds(), TimeUnit.SECONDS);
  }

  @Test
  public void persistAndRecoverEvents() throws Exception {
    PersistenceId persistenceId = PersistenceId.of("Items", UUID.randomUUID().toString());

    ActorRef<Command> items1 =
        system.systemActorOf(new Items(persistenceId), "items1", Props.empty());
    assertEquals("a", this.<String>ask(items1, replyTo -> new Add("a", replyTo)));
    assertEquals("b", this.<String>ask(items1, replyTo -> new Add("b", replyTo)));

    // a second instance replays the events from the database, items1 has no writes in flight
    ActorRef<Command> items2 =
        system.systemActorOf(new Items(persistenceId), "items2", Props.empty());
    assertEquals(Arrays.asList("a", "b"), this.<List<String>>ask(items2, Get::new));
  }
}
