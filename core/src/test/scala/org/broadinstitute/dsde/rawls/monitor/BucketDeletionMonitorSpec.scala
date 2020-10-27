package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{PendingBucketDeletionRecord, TestDriverComponent}
import org.scalatest.time.{Seconds, Span, Milliseconds}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class BucketDeletionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually {
  implicit val cs = IO.contextShift(global)
  def this() = this(ActorSystem("BucketDeletionMonitorSpec"))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

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

    system.actorOf(BucketDeletionMonitor.props(slickDataSource, mockGoogleServicesDAO, 0 seconds, 100 milliseconds))

    eventually(timeout = timeout(Span(2, Seconds)), interval = interval(Span(90, Milliseconds))) {
      verify(mockGoogleServicesDAO, times(1)).deleteBucket(emptyBucketName)
      verify(mockGoogleServicesDAO, Mockito.atLeast(5)).deleteBucket(nonEmptyBucketName)
      verify(mockGoogleServicesDAO, Mockito.atLeast(5)).deleteBucket(errorBucketName)
    }

    val pendingDeletes = runAndWait(pendingBucketDeletionQuery.list())
    pendingDeletes should not contain (PendingBucketDeletionRecord(emptyBucketName))
    pendingDeletes should contain (PendingBucketDeletionRecord(nonEmptyBucketName))
    pendingDeletes should contain (PendingBucketDeletionRecord(errorBucketName))

  }
}
