/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.cleanup

import akka.actor.typed.ActorSystem
//#cleanup
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.persistence.r2dbc.cleanup.scaladsl.EventSourcedCleanup
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal

//#cleanup

object CleanupDocExample {

  implicit val system: ActorSystem[_] = ???

  //#cleanup
  val queries = PersistenceQuery(system).readJournalFor[CurrentPersistenceIdsQuery](R2dbcReadJournal.Identifier)
  val cleanup = new EventSourcedCleanup(system)

  //  how many persistence ids to operate on in parallel
  val persistenceIdParallelism = 10

  // forall persistence ids, delete all events before the snapshot
  queries
    .currentPersistenceIds()
    .mapAsync(persistenceIdParallelism)(pid => cleanup.cleanupBeforeSnapshot(pid))
    .run()

  //#cleanup

}
