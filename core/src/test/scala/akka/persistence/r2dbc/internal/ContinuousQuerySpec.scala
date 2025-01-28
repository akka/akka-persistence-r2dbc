/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
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

  final case class State(value: String, elementCount: Int)
}

class ContinuousQuerySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with ScalaFutures with LogCapturing {
  import ContinuousQuerySpec.{ Results, State }
  implicit val as: ActorSystem = system.classicSystem

  "ContinuousQuery" should {
    "work for initial query" in {
      val results = new Results(Source(List("one", "two", "three")))
      ContinuousQuery[State, String](
        initialState = State("dogs", 0),
        updateState = (state, _) => state.copy(value = "cats", state.elementCount + 1),
        delayNextQuery = state => Some(1.second),
        nextQuery = state => {
          if (state.value == "dogs") state.elementCount shouldBe 0 // initial query
          else state.elementCount shouldBe 3
          state.copy(elementCount = 0) -> results.next()
        }).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three")
    }
    "complete if none returned" in {
      ContinuousQuery[State, String](
        initialState = State("cats", 0),
        updateState = (state, _) => state.copy(value = "cats"),
        delayNextQuery = state => Some(1.second),
        nextQuery = state => state -> None)
        .runWith(Sink.seq)
        .futureValue shouldEqual Nil
    }
    "execute next query on complete" in {
      val results = new Results(Source(List("one", "two")), Source(List("three", "four")))
      ContinuousQuery[State, String](
        initialState = State("dogs", 0),
        updateState = (state, _) => state.copy(value = "cats", state.elementCount + 1),
        delayNextQuery = state => Some(1.second),
        nextQuery = state => {
          if (state.value == "dogs") state.elementCount shouldBe 0 // initial query
          else state.elementCount shouldBe 2
          state.copy(elementCount = 0) -> results.next()
        }).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three", "four")
    }

    "buffer element if no demand" in {
      val results = new Results(Source(List("one", "two")), Source(List("three", "four")))
      val sub =
        ContinuousQuery[State, String](
          initialState = State("cats", 0),
          updateState = (state, _) => state.copy(value = "cats"),
          delayNextQuery = state => Some(1.second),
          nextQuery = state => state -> results.next())
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
        ContinuousQuery[State, () => String](
          initialState = State("cats", 0),
          updateState = (state, _) => state.copy(value = "cats"),
          delayNextQuery = state => Some(1.second),
          nextQuery = state => state -> results.next())
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
        ContinuousQuery[State, String](
          initialState = State("cats", 0),
          updateState = (state, _) => state.copy(value = "cats"),
          delayNextQuery = state => Some(1.second),
          nextQuery = state => state -> results.next())
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

    "have utility to adjust delay depending on rows from previous query" in {
      ContinuousQuery.adjustNextDelay(0, 100, 3000.millis) shouldBe Some(3000.millis)
      ContinuousQuery.adjustNextDelay(3, 100, 3000.millis) shouldBe Some(3000.millis)
      ContinuousQuery.adjustNextDelay(10, 100, 3000.millis) shouldBe Some(3000.millis)
      ContinuousQuery.adjustNextDelay(11, 100, 3000.millis) shouldBe Some(1500.millis)
      ContinuousQuery.adjustNextDelay(50, 100, 3000.millis) shouldBe Some(1500.millis)
      ContinuousQuery.adjustNextDelay(89, 100, 3000.millis) shouldBe Some(1500.millis)
      ContinuousQuery.adjustNextDelay(90, 100, 3000.millis) shouldBe None
      ContinuousQuery.adjustNextDelay(99, 100, 3000.millis) shouldBe None
      ContinuousQuery.adjustNextDelay(100, 100, 3000.millis) shouldBe None

      // unreasonable small buffer-size
      ContinuousQuery.adjustNextDelay(0, 5, 3000.millis) shouldBe Some(3000.millis)
      ContinuousQuery.adjustNextDelay(1, 5, 3000.millis) shouldBe Some(3000.millis)
      ContinuousQuery.adjustNextDelay(2, 5, 3000.millis) shouldBe Some(1500.millis)
      ContinuousQuery.adjustNextDelay(4, 5, 3000.millis) shouldBe None
      ContinuousQuery.adjustNextDelay(5, 5, 3000.millis) shouldBe None
    }

  }
}
