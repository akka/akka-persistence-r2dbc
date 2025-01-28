/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home;

import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;

public class MultiPluginDocExample {

  static
  // #withPlugins
  public class MyEntity extends EventSourcedBehavior<MyEntity.Command, MyEntity.Event, MyEntity.State> {
    // #withPlugins
    public MyEntity(PersistenceId persistenceId) {
      super(persistenceId);
    }

    interface Command {
    }

    interface Event {
    }

    static class State {
    }

    @Override
    public State emptyState() {
      return new State();
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
      return newCommandHandlerBuilder().build();
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
      return newEventHandlerBuilder().build();
    }

    // #withPlugins
    @Override
    public String journalPluginId() {
      return "second-r2dbc.journal";
    }

    @Override
    public String snapshotPluginId() {
      return "second-r2dbc.snapshot";
    }
  }
  // #withPlugins

}
