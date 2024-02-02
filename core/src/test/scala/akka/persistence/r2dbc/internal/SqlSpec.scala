/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.persistence.r2dbc.internal.codec.IdentityAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.SqlServerQueryAdapter
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SqlSpec extends AnyWordSpec with TestSuite with Matchers {
  import Sql.Interpolation
  "SQL string interpolation" should {
    implicit val queryAdapter: QueryAdapter = IdentityAdapter
    "replace ? bind parameters with numbered $ (avoiding escaped ones)" in {
      sql"select * from bar where a = ? and qa = 'Question?? Answer!'" shouldBe "select * from bar where a = $1 and qa = 'Question? Answer!'"
      sql"select * from bar where a = ? and b = ? and jsonb ?? 'status' and c = ?" shouldBe "select * from bar where a = $1 and b = $2 and jsonb ? 'status' and c = $3"
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

  "SQL string interpolation for sqlserver" should {
    implicit val queryAdapter: QueryAdapter = SqlServerQueryAdapter

    "replace $ bind parameters with numbered @ (avoiding escaped ones)" in {
      sql"select * from bar where a = ? and qa = 'Question?? Answer!'" shouldBe "select * from bar where a = @p1 and qa = 'Question? Answer!'"
      sql"select * from bar where a = ? and b = ? and jsonb ?? 'status' and c = ?" shouldBe "select * from bar where a = @p1 and b = @p2 and jsonb ? 'status' and c = @p3"
      sql"select * from bar" shouldBe "select * from bar"
    }

    "work together with standard string interpolation" in {
      val table = "foo"
      sql"select * from $table where a = ?" shouldBe "select * from foo where a = @p1"
    }

    "replace bind parameters after standard string interpolation" in {
      val where = "where a = ? and b = ?"
      sql"select * from foo $where" shouldBe "select * from foo where a = @p1 and b = @p2"
    }

    "trim line breaks" in {
      val table = "foo"
      sql"""
    select * from $table where
      a = ? and
      b = ?
    """ shouldBe "select * from foo where a = @p1 and b = @p2"
    }

  }

}
