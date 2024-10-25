/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.state

// #query
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.serialization.SerializationExtension

class BlogPostQuery(system: ActorSystem[_]) {

  private val findByTitleSql =
    "SELECT state_ser_id, state_ser_manifest, state_payload " +
    "FROM durable_state_blog_post " +
    "WHERE title = $1"

  def findByTitle(title: String): Future[IndexedSeq[BlogPost.State]] = {
    R2dbcSession.withSession(system) { session =>
      session.select(session.createStatement(findByTitleSql).bind(0, title)) { row =>
        val serializerId = row.get("state_ser_id", classOf[java.lang.Integer])
        val serializerManifest = row.get("state_ser_manifest", classOf[String])
        val payload = row.get("state_payload", classOf[Array[Byte]])
        val state = SerializationExtension(system)
          .deserialize(payload, serializerId, serializerManifest)
          .get
          .asInstanceOf[BlogPost.State]
        state
      }
    }
  }

}
// #query
