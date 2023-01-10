/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package demo

import akka.persistence.r2dbc.state.scaladsl.DaoExtension

object DemoAdditionalColumn {
  class CustomDurableState extends DaoExtension {
    override def tableName(entityType: String): String = {
      if (entityType == BlogPost.EntityTypeName)
        "durable_state_blog_post"
      else
        super.tableName(entityType)
    }

    override def additionalColumns(entityType: String): IndexedSeq[DaoExtension.AdditionalColumn[_]] =
      if (entityType == BlogPost.EntityTypeName) {
        val titleColumn =
          DaoExtension.AdditionalColumn[BlogPost.State](
            "title",
            stateUpsert =>
              stateUpsert.value match {
                case BlogPost.BlankState =>
                  DaoExtension.BindNull(classOf[String])
                case s: BlogPost.DraftState =>
                  s.content.title
                case _: BlogPost.PublishedState =>
                  DaoExtension.Skip
              })
        Vector(titleColumn)
      } else {
        super.additionalColumns(entityType)
      }
  }

}
