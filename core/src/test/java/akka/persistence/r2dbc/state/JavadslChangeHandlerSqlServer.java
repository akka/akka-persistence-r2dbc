/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state;

import akka.Done;
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.UpdatedDurableState;
import akka.persistence.r2dbc.session.javadsl.R2dbcSession;
import akka.persistence.r2dbc.state.javadsl.ChangeHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class JavadslChangeHandlerSqlServer implements ChangeHandler<String> {
  @Override
  public CompletionStage<Done> process(R2dbcSession session, DurableStateChange<String> change) {
    if (change instanceof UpdatedDurableState) {
      UpdatedDurableState<String> upd = (UpdatedDurableState<String>) change;
      return session
              .updateOne(
                      session
                              .createStatement("insert into changes_test (pid, rev, the_value) values (@pid, @rev, @theValue)")
                              .bind("@pid", upd.persistenceId())
                              .bind("@rev", upd.revision())
                              .bind("@theValue", upd.value()))
              .thenApply(n -> Done.getInstance());
    } else {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }
}
