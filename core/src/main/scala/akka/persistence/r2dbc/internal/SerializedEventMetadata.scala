/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])
