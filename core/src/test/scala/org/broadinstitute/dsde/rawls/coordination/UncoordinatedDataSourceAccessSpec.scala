package org.broadinstitute.dsde.rawls.coordination

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import slick.jdbc.TransactionIsolation

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

class UncoordinatedDataSourceAccessSpec
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with TableDrivenPropertyChecks
    with MockitoSugar {
  behavior of "UncoordinatedDataSourceAccess"

  import scala.concurrent.ExecutionContext.Implicits.global

  private val tests = Table(
    ("description", "function", "expected"),
    (
      "return a normal result",
      () => 42,
      Success(42),
    ),
    (
      "not lose errors when they occur",
      () => throw new RuntimeException("expected") with NoStackTrace,
      Failure(new RuntimeException("expected")),
    ),
    (
      "still wait for results that arrive late",
      () => { Thread.sleep(10.seconds.toMillis); "i'm running a bit late" },
      Success("i'm running a bit late"),
    ),
  )

  forAll(tests) { (description, function, expected) =>
    it should description in {
      val mockSlickDataSource = mock[SlickDataSource]
      val mockDataAccessFunction = mock[DataAccess => ReadWriteAction[Any]]
      val transactionIsolation = TransactionIsolation.RepeatableRead
      when(mockSlickDataSource.inTransaction(mockDataAccessFunction, transactionIsolation))
        .thenReturn(Future(function()))
      val testAccess = new UncoordinatedDataSourceAccess(mockSlickDataSource)
      val future = testAccess.inTransaction[Any](mockDataAccessFunction, transactionIsolation)
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
}
