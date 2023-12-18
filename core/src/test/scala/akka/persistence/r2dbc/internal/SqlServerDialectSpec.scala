/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.persistence.r2dbc.internal.sqlserver.SqlServerDialectHelper
import com.typesafe.config.ConfigFactory
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Assertions._

class SqlServerDialectSpec extends AnyWordSpec with TestSuite with Matchers {

  "Helper" should {
    "throw if tag contains separator character" in {
      val conf = ConfigFactory.parseString("""{
          |  tag-separator = "|"
          |}
          |""".stripMargin)
      val tag = "some|tag"
      assertThrows[IllegalArgumentException] {
        SqlServerDialectHelper(conf).tagsToDb(Set(tag))
      }
    }

  }
}
