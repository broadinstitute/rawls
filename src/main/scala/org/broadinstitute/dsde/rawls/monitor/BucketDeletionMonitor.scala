package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.pattern._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{RawlsTransaction, GoogleServicesDAO, DataSource, DbContainerDAO}
import org.broadinstitute.dsde.rawls.model.PendingBucketDeletions
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.{BucketDeleted, DeleteBucket}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadAction
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.dataaccess.slick.PendingBucketDeletionRecord
import com.typesafe.scalalogging.slf4j.LazyLogging

object BucketDeletionMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext): Props = {
    Props(new BucketDeletionMonitor(datasource, gcsDAO))
  }

  sealed trait BucketDeletionsMessage
  case class DeleteBucket(bucketName: String) extends BucketDeletionsMessage
  case class BucketDeleted(bucketName: String) extends BucketDeletionsMessage
}

class BucketDeletionMonitor(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {

  override def receive = {
    case DeleteBucket(bucketName) => deleteBucket(bucketName) pipeTo self
    case BucketDeleted(bucketName) => deleteBucketDeletionRecord(bucketName) pipeTo self
    
    case Unit => // successful future
    case Status.Failure(t) => logger.error("failure in BucketDeletionMonitor", t)
  }

  private def deleteBucket(bucketName: String): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.save(PendingBucketDeletionRecord(bucketName))
    } map { _ =>
      gcsDAO.deleteBucket(bucketName, self)
    }
  }

  private def deleteBucketDeletionRecord(bucketName: String): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.delete(PendingBucketDeletionRecord(bucketName))
    } map { _ => Unit }
  }
}