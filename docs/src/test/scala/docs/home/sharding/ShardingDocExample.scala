/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.home.sharding

//#sharding-init
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.SliceRangeShardAllocationStrategy
import akka.cluster.sharding.typed.SliceRangeShardAllocationStrategy.ShardBySliceMessageExtractor
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.Persistence

//#sharding-init

object ShardingDocExample {
  object DeviceEntity {
    sealed trait Command

    val EntityKey: EntityTypeKey[Command] =
      EntityTypeKey[Command]("DeviceEntity")

    def apply(deviceId: String): Behavior[Command] = ???
  }

  val system: ActorSystem[_] = ???

  //#sharding-init
  ClusterSharding(system).init(
    Entity(DeviceEntity.EntityKey)(entityContext => DeviceEntity(entityContext.entityId))
      .withMessageExtractor(
        new ShardBySliceMessageExtractor[DeviceEntity.Command](DeviceEntity.EntityKey.name, Persistence(system)))
      .withAllocationStrategy(new SliceRangeShardAllocationStrategy(10, 0.1)))
  //#sharding-init

}
