/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpecLike

object ContinuousQuerySpec {
  class Results[T](results: Source[T, NotUsed]*) {
    private var r = results.toList
    def next(): Option[Source[T, NotUsed]] = {
      r match {
        case x :: xs =>
          r = xs
          Some(x)
        case Nil => None
      }
    }
  }
}

class ContinuousQuerySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with ScalaFutures with LogCapturing {
  import ContinuousQuerySpec.Results
  implicit val as: ActorSystem = system.classicSystem

  "ContinuousQuery" should {
    "work for initial query" in {
      val results = new Results(Source(List("one", "two", "three")))
      ContinuousQuery[String, String](
        "dogs",
        (_, _) => "cats",
        _ => Some(1.second),
        (state, nrElementsInPrevious) => {
          if (state == "dogs") nrElementsInPrevious shouldBe -1 // initial query
          else nrElementsInPrevious shouldBe 3
          results.next()
        }).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three")
    }
    "complete if none returned" in {
      ContinuousQuery[String, String]("cats", (_, _) => "cats", _ => Some(1.second), (_, _) => None)
        .runWith(Sink.seq)
        .futureValue shouldEqual Nil
    }
    "execute next query on complete" in {
      val results = new Results(Source(List("one", "two")), Source(List("three", "four")))
      ContinuousQuery[String, String](
        "dogs",
        (_, _) => "cats",
        _ => Some(1.second),
        (state, nrElementsInPrevious) => {
          if (state == "dogs") nrElementsInPrevious shouldBe -1 // initial query
          else nrElementsInPrevious shouldBe 2
          results.next()
        }).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three", "four")
    }

    "buffer element if no demand" in {
      val results = new Results(Source(List("one", "two")), Source(List("three", "four")))
      val sub =
        ContinuousQuery[String, String]("cats", (_, _) => "cats", _ => Some(1.second), (state, _) => results.next())
          .runWith(TestSink.probe[String])

      sub
        .request(1)
        .expectNext("one")
        .expectNoMessage()
        .request(1)
        .expectNext("two")
        .request(3)
        .expectNext("three")
        .expectNext("four")
        .expectComplete()
    }

    "fails if subsstream fails" in {
      val t = new RuntimeException("oh dear")
      val results = new Results(Source(List(() => "one", () => "two")), Source(List(() => "three", () => throw t)))
      val sub =
        ContinuousQuery[String, () => String](
          "cats",
          (_, _) => "cats",
          _ => Some(1.second),
          (state, _) => results.next())
          .map(_.apply())
          .runWith(TestSink.probe)

      sub
        .requestNext("one")
        .requestNext("two")
        .requestNext("three")
        .request(1)
        .expectError(t)
    }

    "should pull something something" in {
      val results = new Results(Source(List("one", "two", "three")))
      val sub =
        ContinuousQuery[String, String]("cats", (_, _) => "cats", _ => Some(1.second), (state, _) => results.next())
          .runWith(TestSink.probe[String])

      // give time for the startup to do the pull the buffer the element
      Thread.sleep(500)
      sub.request(1)
      sub.expectNext("one")

      sub.expectNoMessage(1.second)

      sub.request(3)
      sub.expectNext("two")
      sub.expectNext("three")
    }

  }
}
