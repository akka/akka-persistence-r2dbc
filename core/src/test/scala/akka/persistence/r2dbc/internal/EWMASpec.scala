/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.duration._

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Original source code taken from
 * https://github.com/akka/akka/blob/main/akka-cluster-metrics/src/test/scala/akka/cluster/metrics/EWMASpec.scala
 */
class EWMASpec extends AnyWordSpec with TestSuite with Matchers {

  "EWMA" should {

    "calcualate same ewma for constant values" in {
      val ds = EWMA(value = 100.0, alpha = 0.18) :+
        100.0 :+ 100.0 :+ 100.0
      ds.value should ===(100.0 +- 0.001)
    }

    "calcualate correct ewma for normal decay" in {
      val d0 = EWMA(value = 1000.0, alpha = 2.0 / (1 + 10))
      d0.value should ===(1000.0 +- 0.01)
      val d1 = d0 :+ 10.0
      d1.value should ===(820.0 +- 0.01)
      val d2 = d1 :+ 10.0
      d2.value should ===(672.73 +- 0.01)
      val d3 = d2 :+ 10.0
      d3.value should ===(552.23 +- 0.01)
      val d4 = d3 :+ 10.0
      d4.value should ===(453.64 +- 0.01)

      val dn = (1 to 100).foldLeft(d0)((d, _) => d :+ 10.0)
      dn.value should ===(10.0 +- 0.1)
    }

    "calculate ewma for alpha 1.0, max bias towards latest value" in {
      val d0 = EWMA(value = 100.0, alpha = 1.0)
      d0.value should ===(100.0 +- 0.01)
      val d1 = d0 :+ 1.0
      d1.value should ===(1.0 +- 0.01)
      val d2 = d1 :+ 57.0
      d2.value should ===(57.0 +- 0.01)
      val d3 = d2 :+ 10.0
      d3.value should ===(10.0 +- 0.01)
    }

    "calculate alpha from half-life and collect interval" in {
      // according to https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
      val expectedAlpha = 0.1
      // alpha = 2.0 / (1 + N)
      val n = 19
      val halfLife = n.toDouble / 2.8854
      val collectInterval = 1.second
      val halfLifeDuration = (halfLife * 1000).millis
      EWMA.alpha(halfLifeDuration, collectInterval) should ===(expectedAlpha +- 0.001)
    }

    "calculate sane alpha from short half-life" in {
      val alpha = EWMA.alpha(1.millis, 3.seconds)
      alpha should be <= 1.0
      alpha should be >= 0.0
      alpha should ===(1.0 +- 0.001)
    }

    "calculate sane alpha from long half-life" in {
      val alpha = EWMA.alpha(1.day, 3.seconds)
      alpha should be <= 1.0
      alpha should be >= 0.0
      alpha should ===(0.0 +- 0.001)
    }

  }
}
