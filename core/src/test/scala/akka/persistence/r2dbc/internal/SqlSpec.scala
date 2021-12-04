/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SqlSpec extends AnyWordSpec with TestSuite with Matchers {
  import akka.persistence.r2dbc.Sql.Interpolation

  "SQL string interpolation" should {
    "replace ? bind parameters with numbered $" in {
      sql"select * from bar where a = ?" shouldBe "select * from bar where a = $1"
      sql"select * from bar where a = ? and b = ? and c = ?" shouldBe "select * from bar where a = $1 and b = $2 and c = $3"
      sql"select * from bar" shouldBe "select * from bar"
    }

    "work together with standard string interpolation" in {
      val table = "foo"
      sql"select * from $table where a = ?" shouldBe "select * from foo where a = $1"
    }

    "replace bind parameters after standard string interpolation" in {
      val where = "where a = ? and b = ?"
      sql"select * from foo $where" shouldBe "select * from foo where a = $1 and b = $2"
    }

    "trim line breaks" in {
      val table = "foo"
      sql"""
        select * from $table where
          a = ? and
          b = ?
        """ shouldBe "select * from foo where a = $1 and b = $2"
    }
  }

}
