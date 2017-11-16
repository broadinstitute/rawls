package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.CheckAll
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.dataaccess.slick.PendingBucketDeletionRecord
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

object BucketDeletionMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new BucketDeletionMonitor(datasource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait BucketDeletionsMessage
  case object CheckAll extends BucketDeletionsMessage
}

class BucketDeletionMonitor(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {

  context.system.scheduler.schedule(initialDelay, pollInterval, self, CheckAll)

  override def receive = {
    case CheckAll => checkAll()
  }

  private def checkAll() = {
    val checkFuture = for {
      deleteRecords <- datasource.inTransaction { dataAccess =>
        dataAccess.pendingBucketDeletionQuery.list()
      }

      _ <- Future.traverse(deleteRecords) { deleteRecord =>
        deleteBucket(deleteRecord.bucket)
      }
    } yield ()


    checkFuture.onFailure {
      // there was a failure, log it and it will retry later
      case t: Throwable => logger.error("Error checking buckets to delete", t)
    }
  }

  private def deleteBucket(bucketName: String): Future[Boolean] = {
    val deleteFuture = for {
      deleted <- gcsDAO.deleteBucket(bucketName)
      _ <- if (deleted) deleteBucketDeletionRecord(bucketName) else Future.successful(())
    } yield {
      logger.info(s"attempt to delete bucket $bucketName - $deleted")
      deleted
    }

    deleteFuture.recover {
      case t: Throwable =>
        logger.error(s"failure deleting bucket $bucketName", t)
        false
    }
  }

  private def deleteBucketDeletionRecord(bucketName: String): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.delete(PendingBucketDeletionRecord(bucketName))
    } map { _ => Unit }
  }
}