/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.home.journal

// #query
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.serialization.SerializationExtension

class BlogPostQuery(system: ActorSystem[_]) {

  private val findByTitleSql =
    "SELECT persistence_id, seq_nr, event_ser_id, event_ser_manifest, event_payload " +
    "FROM event_journal_blogpost " +
    "WHERE title = $1 AND deleted = false " +
    "ORDER BY persistence_id, seq_nr"

  def findByTitle(title: String): Future[IndexedSeq[BlogPost.Event]] = {
    R2dbcSession.withSession(system) { session =>
      session.select(session.createStatement(findByTitleSql).bind(0, title)) { row =>
        val serializerId = row.get("event_ser_id", classOf[java.lang.Integer])
        val serializerManifest = row.get("event_ser_manifest", classOf[String])
        val payload = row.get("event_payload", classOf[Array[Byte]])
        val event = SerializationExtension(system)
          .deserialize(payload, serializerId, serializerManifest)
          .get
          .asInstanceOf[BlogPost.Event]
        event
      }
    }
  }

}
// #query
