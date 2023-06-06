package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.PendingBucketDeletionRecord
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.CheckAll

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object BucketDeletionMonitor {
  def props(datasource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            initialDelay: FiniteDuration,
            pollInterval: FiniteDuration
  )(implicit executionContext: ExecutionContext): Props =
    Props(new BucketDeletionMonitor(datasource, gcsDAO, initialDelay, pollInterval))

  sealed trait BucketDeletionsMessage
  case object CheckAll extends BucketDeletionsMessage
}

class BucketDeletionMonitor(datasource: SlickDataSource,
                            gcsDAO: GoogleServicesDAO,
                            initialDelay: FiniteDuration,
                            pollInterval: FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends Actor
    with LazyLogging {

  context.system.scheduler.schedule(initialDelay, pollInterval, self, CheckAll)

  override def receive = { case CheckAll =>
    checkAll()
  }

  private def checkAll() = {
    val checkFuture = for {
      deleteRecords <- datasource.inTransaction { dataAccess =>
        dataAccess.pendingBucketDeletionQuery.list()
      }

      // This used to be Future.traverse but that runs all the futures right now, concurrently. When there are 100s or
      // 1000s of buckets to delete this causes a problem by overwhelming the thread pool. Switching to use cats
      // traverse and IO makes this all go serially which is ok in this background process.
      _ <- deleteRecords.toList
        .traverse { deleteRecord =>
          IO.fromFuture(IO(deleteBucket(deleteRecord.bucket)))
        }
        .unsafeToFuture()
    } yield ()

    checkFuture.failed.foreach {
      // there was a failure, log it and it will retry later
      t: Throwable => logger.error("Error checking buckets to delete", t)
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

    deleteFuture.recover { case t: Throwable =>
      logger.error(s"failure deleting bucket $bucketName", t)
      false
    }
  }

  private def deleteBucketDeletionRecord(bucketName: String): Future[Unit] =
    datasource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.delete(PendingBucketDeletionRecord(bucketName))
    }.void
}
