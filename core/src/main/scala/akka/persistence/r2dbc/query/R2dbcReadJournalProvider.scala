/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.scaladsl.ReadJournal
import com.typesafe.config.Config

final class R2dbcReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournalProvider {

  private val scaladslReadJournalInstance =
    new scaladsl.R2dbcReadJournal(system, config, cfgPath)

  override def scaladslReadJournal(): ReadJournal = scaladslReadJournalInstance

  private val javadslReadJournalInstance =
    new javadsl.R2dbcReadJournal(scaladslReadJournalInstance)

  override def javadslReadJournal(): javadsl.R2dbcReadJournal = javadslReadJournalInstance

}
