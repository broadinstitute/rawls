package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{PendingBucketDeletionRecord, TestDriverComponent}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.verification.VerificationMode
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.concurrent.Eventually

class BucketDeletionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually {
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

    val mockGoogleServicesDAO = mock[GoogleServicesDAO]
    when(mockGoogleServicesDAO.deleteBucket(emptyBucketName)).thenReturn(Future.successful(true))
    when(mockGoogleServicesDAO.deleteBucket(nonEmptyBucketName)).thenReturn(Future.successful(false))
    when(mockGoogleServicesDAO.deleteBucket(errorBucketName)).thenReturn(Future.failed(new RuntimeException(":(")))

    system.actorOf(BucketDeletionMonitor.props(slickDataSource, mockGoogleServicesDAO, 0 seconds, 100 milliseconds))
    
    implicit val patienceConfig = PatienceConfig(timeout = 1 second)

    eventually {
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
