package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{PendingBucketDeletionRecord, TestDriverComponent}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class BucketDeletionMonitorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with MockitoSugar
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually
    with ScalaFutures {
  def this() = this(ActorSystem("BucketDeletionMonitorSpec"))

  override def beforeAll(): Unit =
    super.beforeAll()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "BucketDeletionMonitor" should "delete buckets" in {
    val emptyBucketName = "empty-bucket"
    val nonEmptyBucketName = "nonempty-bucket"
    val errorBucketName = "error-bucket"

    runAndWait(pendingBucketDeletionQuery.save(emptyBucketName))
    runAndWait(pendingBucketDeletionQuery.save(nonEmptyBucketName))
    runAndWait(pendingBucketDeletionQuery.save(errorBucketName))

    val mockGoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    when(mockGoogleServicesDAO.deleteBucket(emptyBucketName)).thenReturn(Future.successful(true))
    when(mockGoogleServicesDAO.deleteBucket(nonEmptyBucketName)).thenReturn(Future.successful(false))
    when(mockGoogleServicesDAO.deleteBucket(errorBucketName)).thenReturn(Future.failed(new RuntimeException(":(")))

    system.actorOf(BucketDeletionMonitor.props(slickDataSource, mockGoogleServicesDAO, 0 seconds, 1 second))

    // `eventually` now requires an implicit `Retrying` instance. When the statement inside returns future, it'll
    // try to use `Retrying[Future[T]]`, which gets weird when we're using mockito together with it.
    // Hence adding ascribing [Unit] explicitly here so that `eventually` will use `Retrying[Unit]`
    eventually[Unit](timeout = timeout(Span(10, Seconds))) {
      verify(mockGoogleServicesDAO, times(1)).deleteBucket(emptyBucketName)
      verify(mockGoogleServicesDAO, Mockito.atLeast(5)).deleteBucket(nonEmptyBucketName)
      verify(mockGoogleServicesDAO, Mockito.atLeast(5)).deleteBucket(errorBucketName)
      ()
    }

    val pendingDeletes = runAndWait(pendingBucketDeletionQuery.list())
    pendingDeletes should not contain (PendingBucketDeletionRecord(emptyBucketName))
    pendingDeletes should contain(PendingBucketDeletionRecord(nonEmptyBucketName))
    pendingDeletes should contain(PendingBucketDeletionRecord(errorBucketName))

  }
}
