package org.broadinstitute.dsde.rawls.coordination

import akka.actor.{ActorSystem, Status}
import akka.pattern.AskTimeoutException
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.mockito.Mockito.RETURNS_SMART_NULLS
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.TransactionIsolation

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

class CoordinatedDataSourceAccessSpec
    extends TestKit(ActorSystem("CoordinatedDataSourceAccessSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with TableDrivenPropertyChecks
    with MockitoSugar {
  behavior of "CoordinatedDataSourceAccess"

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  private val defaultTimeout = 5.seconds

  private val tests = Table(
    ("description", "function", "expected"),
    (
      "return a normal result",
      () => 42,
      Right(42)
    ),
    (
      "not lose errors when they occur",
      () => Status.Failure(new RuntimeException("expected") with NoStackTrace),
      Left(classOf[RuntimeException])
    ),
    (
      "not wait for results that arrive too late",
      () => {
        Thread.sleep((defaultTimeout * 2).toMillis); "i'm running a bit late"
      },
      Left(classOf[AskTimeoutException])
    )
  )

  forAll(tests) { (description, function, expected) =>
    it should description in {
      val testProbe = TestProbe("CoordinatedDataSourceAccessProbe")
      val mockSlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
      val mockDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]](RETURNS_SMART_NULLS)
      val testAccess = new CoordinatedDataSourceAccess(
        slickDataSource = mockSlickDataSource,
        dataSourceActor = testProbe.ref,
        starTimeout = defaultTimeout,
        waitTimeout = defaultTimeout,
        askTimeout = defaultTimeout
      )
      val future = testAccess.inTransaction[Any](mockDataAccessFunction)
      testProbe.expectMsgPF(defaultTimeout) {
        case CoordinatedDataSourceActor.Run(
              runSlickDataSource,
              runDataAccessFunction,
              runTransactionIsolation,
              _, // Do not have a good way of validating the deadline without passing the start time in
              runWaitTimeout
            ) =>
          runSlickDataSource should be(mockSlickDataSource)
          runDataAccessFunction should be(mockDataAccessFunction)
          runTransactionIsolation should be(TransactionIsolation.RepeatableRead)
          runWaitTimeout should be(runWaitTimeout)
      }
      testProbe.reply(function())
      Await.ready(future, 30.seconds)
      expected match {
        case Right(expectedSuccess) =>
          val Success(actualSuccess) = future.value.get
          actualSuccess should be(expectedSuccess)
        case Left(expectedFailureClass) =>
          val Failure(actualFailure) = future.value.get
          expectedFailureClass.isAssignableFrom(actualFailure.getClass) should be(true)
      }
    }
  }
}
