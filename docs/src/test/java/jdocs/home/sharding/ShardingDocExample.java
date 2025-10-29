/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.home.sharding;

// #sharding-init
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.cluster.sharding.typed.SliceRangeShardAllocationStrategy;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.persistence.Persistence;

// #sharding-init

public class ShardingDocExample {
  public static class DeviceEntity {
    public interface Command {}

    public static final EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "device");

    public static Behavior<Command> create(String deviceId) {
      return null;
    }
  }

  public static void example() {
    ActorSystem<?> system = null;

    // #sharding-init
    ClusterSharding.get(system)
        .init(
            Entity.of(
                    DeviceEntity.ENTITY_TYPE_KEY,
                    entityContext -> DeviceEntity.create(entityContext.getEntityId()))
                .withMessageExtractor(
                    new SliceRangeShardAllocationStrategy.ShardBySliceMessageExtractor<>(
                        DeviceEntity.ENTITY_TYPE_KEY.name(), Persistence.get(system)))
                .withAllocationStrategy(new SliceRangeShardAllocationStrategy(10, 0.1)));
    // #sharding-init
  }
}
