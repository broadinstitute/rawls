package org.broadinstitute.dsde.rawls.coordination

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.coordination.CoordinatedDataSourceActorSpec._
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.TransactionIsolation

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

class CoordinatedDataSourceActorSpec
    extends TestKit(ActorSystem("CoordinatedDataSourceActorSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with TableDrivenPropertyChecks
    with MockitoSugar {
  behavior of "CoordinatedDataSourceActor"

  import system.dispatcher

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  private val tests = Table(
    ("description", "function", "expected"),
    (
      "return a normal result",
      () => 42,
      Success(42)
    ),
    (
      "not lose errors when they occur",
      () => throw new RuntimeException("expected") with NoStackTrace,
      Failure(new RuntimeException("expected"))
    ),
    (
      "not wait for results that arrive too late",
      () => {
        Thread.sleep(10.seconds.toMillis)
        "i'm running a bit late"
      },
      Failure(new TimeoutException("Future timed out after [5 seconds]"))
    )
  )

  forAll(tests) { (description, function, expected) =>
    it should description in {
      val timeout = 5.seconds
      val testActor = TestActorRef(CoordinatedDataSourceActor.props())
      implicit val implicitAskTimeout: Timeout = Timeout(timeout * 2)
      val mockSlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
      val mockDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]](RETURNS_SMART_NULLS)
      val transactionIsolation = TransactionIsolation.RepeatableRead
      when(mockSlickDataSource.inTransaction(mockDataAccessFunction, transactionIsolation))
        .thenReturn(Future(function()))
      val future = testActor ?
        CoordinatedDataSourceActor.Run(
          mockSlickDataSource,
          mockDataAccessFunction,
          transactionIsolation,
          Deadline.now + timeout,
          timeout
        )
      Await.ready(future, 30.seconds)
      expected match {
        case Success(expectedSuccess) =>
          val Success(actualSuccess) = future.value.get
          actualSuccess should be(expectedSuccess)
        case Failure(expectedFailure) =>
          val Failure(actualFailure) = future.value.get
          expectedFailure.getClass.isAssignableFrom(actualFailure.getClass) should be(true)
          actualFailure.getMessage should be(expectedFailure.getMessage)
      }
    }
  }

  it should "should run serially" in {
    val individualSleep = 100.millis
    val individualWaitTimeout = 1.second
    val numTested = 100
    implicit val implicitAskTimeout: Timeout = Timeout(individualWaitTimeout * numTested * 2)
    val testActor = TestActorRef(CoordinatedDataSourceActor.props())
    val mockSlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    val mockDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]](RETURNS_SMART_NULLS)
    val transactionIsolation = TransactionIsolation.RepeatableRead

    val quickening = new Quickening(individualSleep)
    when(mockSlickDataSource.inTransaction(mockDataAccessFunction, transactionIsolation))
      .thenAnswer(_ => Future(quickening.highlander()))

    val futures = (0 until numTested) map { _ =>
      testActor ?
        CoordinatedDataSourceActor.Run(
          mockSlickDataSource,
          mockDataAccessFunction,
          transactionIsolation,
          Deadline.now + implicitAskTimeout.duration,
          individualWaitTimeout
        )
    }
    val future = Future.sequence(futures)
    Await.ready(future, 30.seconds)
    val Success(actualSuccess) = future.value.get
    val unit: Unit = ()
    actualSuccess should contain only unit
    quickening.total should be(numTested)
  }

  it should "should run not run jobs past their start deadline" in {
    val individualSleep = 100.millis
    val startTimeout = 100.millis
    val individualWaitTimeout = 1.second
    val numTested = 100
    implicit val implicitAskTimeout: Timeout = Timeout(individualWaitTimeout * numTested * 2) // Lots of time to wait
    val testActor = TestActorRef(CoordinatedDataSourceActor.props())
    val mockSlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    val mockDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]](RETURNS_SMART_NULLS)
    val slowDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]](RETURNS_SMART_NULLS)
    val transactionIsolation = TransactionIsolation.RepeatableRead

    val quickening = new Quickening(individualSleep)
    when(mockSlickDataSource.inTransaction(mockDataAccessFunction, transactionIsolation))
      .thenAnswer(_ => Future(quickening.highlander()))
    when(mockSlickDataSource.inTransaction(slowDataAccessFunction, transactionIsolation))
      .thenAnswer(_ => Future(Thread.sleep(1.second.toMillis)))

    // Create the runs with short deadlines first
    val deadlineRuns = (0 until numTested) map { _ =>
      CoordinatedDataSourceActor.Run(
        mockSlickDataSource,
        mockDataAccessFunction,
        transactionIsolation,
        Deadline.now + startTimeout,
        individualWaitTimeout
      )
    }

    // Then go ask the actor to run something ssslllooowww
    testActor ?
      CoordinatedDataSourceActor.Run(
        mockSlickDataSource,
        slowDataAccessFunction,
        transactionIsolation,
        Deadline.now + individualSleep,
        individualSleep * numTested
      )

    // Now go back and ask to run the now expired deadlines
    val futures = deadlineRuns map {
      testActor ? _
    } map {
      _ recover { case _: CoordinatedDataSourceActor.StartDeadlineException =>
        "hit start deadline"
      }
    }
    val future = Future.sequence(futures)
    Await.ready(future, 30.seconds)
    val Success(actualSuccess) = future.value.get
    actualSuccess should contain only "hit start deadline"
    quickening.total should be(0)
  }
}

object CoordinatedDataSourceActorSpec {
  class Quickening(sleep: FiniteDuration) {
    private var counter = 0
    var total = 0

    def highlander(): Unit = {
      total += 1
      counter += 1
      Thread.sleep(sleep.toMillis)
      val count = counter
      counter -= 1
      if (count != 1) {
        throw new RuntimeException(s"Highlander count was expected to be one but got $count")
      }
    }
  }
}
