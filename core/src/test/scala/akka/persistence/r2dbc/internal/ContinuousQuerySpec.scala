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

class ContinuousQuerySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with ScalaFutures with LogCapturing {
  implicit val as: ActorSystem = system.classicSystem
  "ContinuousQuery" should {
    "work for initial query" in {
      ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two", "three"))),
        0,
        1.second).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three")
    }
    "complete if none returned" in {
      ContinuousQuery[String, String]("cats", (_, _) => "cats", _ => None, 0, 1.second)
        .runWith(Sink.seq)
        .futureValue shouldEqual Nil
    }
    "execute next query on complete" in {
      ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two")), Source(List("three", "four"))),
        1,
        1.second).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three", "four")
    }

    "buffer element if no demand" in {
      val sub = ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two")), Source(List("three", "four"))),
        0,
        1.second).runWith(TestSink.probe[String])

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
      val sub = ContinuousQuery[String, () => String](
        "cats",
        (_, _) => "cats",
        results(Source(List(() => "one", () => "two")), Source(List(() => "three", () => throw t))),
        0,
        1.second).map(_.apply()).runWith(TestSink.probe)

      sub
        .requestNext("one")
        .requestNext("two")
        .requestNext("three")
        .request(1)
        .expectError(t)
    }

    "should pull something something" in {
      val sub = ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two", "three"))),
        0,
        1.second).runWith(TestSink.probe[String])

      // give time for the startup to do the pull the buffer the element
      Thread.sleep(500)
      sub.request(1)
      sub.expectNext("one")

      sub.expectNoMessage(1.second)

      sub.request(3)
      sub.expectNext("two")
      sub.expectNext("three")
    }

    "update state every element" in {
      pending
    }

    def results[T](results: Source[T, NotUsed]*): String => Option[Source[T, NotUsed]] = {
      var r = results.toList
      def next(state: String): Option[Source[T, NotUsed]] =
        r match {
          case x :: xs =>
            r = xs
            Some(x)
          case Nil => None
        }
      next
    }
  }
}
