/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import akka.actor.ExtendedActorSystem
import akka.persistence.state.DurableStateStoreProvider
import com.typesafe.config.Config
import akka.persistence.state.javadsl.{ DurableStateStore => JDurableStateStore }
import akka.persistence.state.scaladsl.DurableStateStore

class R2dbcDurableStateStoreProvider[A](system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateStoreProvider {

  override def scaladslDurableStateStore(): DurableStateStore[Any] =
    new scaladsl.R2dbcDurableStateStore(system, config, cfgPath)

  override def javadslDurableStateStore(): JDurableStateStore[AnyRef] =
    new javadsl.R2dbcDurableStateStore[AnyRef](new scaladsl.R2dbcDurableStateStore(system, config, cfgPath))(
      system.dispatcher)
}
