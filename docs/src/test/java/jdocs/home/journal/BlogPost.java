/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.home.journal;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.Effect;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import java.util.Optional;

public class BlogPost extends EventSourcedBehavior<BlogPost.Command, BlogPost.Event, Optional<BlogPost.State>> {

  public interface Command {}

  public static class AddPost implements Command {
    final String postId;
    final String title;
    final String body;
    final ActorRef<Done> replyTo;

    public AddPost(String postId, String title, String body, ActorRef<Done> replyTo) {
      this.postId = postId;
      this.title = title;
      this.body = body;
      this.replyTo = replyTo;
    }
  }

  public static class ChangeBody implements Command {
    final String newBody;
    final ActorRef<Done> replyTo;

    public ChangeBody(String newBody, ActorRef<Done> replyTo) {
      this.newBody = newBody;
      this.replyTo = replyTo;
    }
  }

  public static class Publish implements Command {
    final ActorRef<Done> replyTo;

    public Publish(ActorRef<Done> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public interface Event {}

  public static class PostAdded implements Event {
    public final String postId;
    public final String title;
    public final String body;

    public PostAdded(String postId, String title, String body) {
      this.postId = postId;
      this.title = title;
      this.body = body;
    }
  }

  public static class BodyChanged implements Event {
    public final String postId;
    public final String newBody;

    public BodyChanged(String postId, String newBody) {
      this.postId = postId;
      this.newBody = newBody;
    }
  }

  public static class Published implements Event {
    public final String postId;

    public Published(String postId) {
      this.postId = postId;
    }
  }

  public static class State {
    final String postId;
    final String title;
    final String body;
    final boolean published;

    public State(String postId, String title, String body, boolean published) {
      this.postId = postId;
      this.title = title;
      this.body = body;
      this.published = published;
    }

    State withBody(String newBody) {
      return new State(postId, title, newBody, published);
    }

    State asPublished() {
      return new State(postId, title, body, true);
    }
  }

  public static Behavior<Command> create(PersistenceId persistenceId) {
    return new BlogPost(persistenceId);
  }

  private BlogPost(PersistenceId persistenceId) {
    super(persistenceId);
  }

  @Override
  public Optional<State> emptyState() {
    return Optional.empty();
  }

  @Override
  public CommandHandler<Command, Event, Optional<State>> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(AddPost.class, this::onAddPost)
        .onCommand(ChangeBody.class, this::onChangeBody)
        .onCommand(Publish.class, this::onPublish)
        .build();
  }

  private Effect<Event, Optional<State>> onAddPost(Optional<State> state, AddPost cmd) {
    return Effect()
        .persist(new PostAdded(cmd.postId, cmd.title, cmd.body))
        .thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
  }

  private Effect<Event, Optional<State>> onChangeBody(Optional<State> state, ChangeBody cmd) {
    if (state.isPresent()) {
      return Effect()
          .persist(new BodyChanged(state.get().postId, cmd.newBody))
          .thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
    } else {
      return Effect().none().thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
    }
  }

  private Effect<Event, Optional<State>> onPublish(Optional<State> state, Publish cmd) {
    if (state.isPresent()) {
      return Effect()
          .persist(new Published(state.get().postId))
          .thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
    } else {
      return Effect().none().thenRun(() -> cmd.replyTo.tell(Done.getInstance()));
    }
  }

  @Override
  public EventHandler<Optional<State>, Event> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(PostAdded.class, (state, evt) -> Optional.of(new State(evt.postId, evt.title, evt.body, false)))
        .onEvent(BodyChanged.class, (state, evt) -> state.map(s -> s.withBody(evt.newBody)))
        .onEvent(Published.class, (state, evt) -> state.map(State::asPublished))
        .build();
  }
}
