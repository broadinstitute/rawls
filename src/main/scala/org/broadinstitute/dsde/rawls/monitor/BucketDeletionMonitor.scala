package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{RawlsTransaction, GoogleServicesDAO, DataSource, DbContainerDAO}
import org.broadinstitute.dsde.rawls.model.PendingBucketDeletions
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.{BucketDeleted, DeleteBucket}

object BucketDeletionMonitor {
  def props(datasource: DataSource, containerDAO: DbContainerDAO, gcsDAO: GoogleServicesDAO): Props = {
    Props(new BucketDeletionMonitor(datasource, containerDAO, gcsDAO))
  }

  sealed trait BucketDeletionsMessage
  case class DeleteBucket(bucketName: String) extends BucketDeletionsMessage
  case class BucketDeleted(bucketName: String) extends BucketDeletionsMessage
}

class BucketDeletionMonitor(datasource: DataSource, containerDAO: DbContainerDAO, gcsDAO: GoogleServicesDAO) extends Actor {

  override def receive = {
    case DeleteBucket(bucketName) => deleteBucket(bucketName)
    case BucketDeleted(bucketName) => recordBucketDeletion(bucketName)
  }

  private def getPendingDeletions(txn: RawlsTransaction) =
    containerDAO.workspaceDAO.loadPendingBucketDeletions(txn) getOrElse {
      throw new RawlsException("Cannot find the list of buckets pending deletion")
    }

  private def deleteBucket(bucketName: String) = {
    datasource.inTransaction() { txn =>
      val pbd = getPendingDeletions(txn)
      containerDAO.workspaceDAO.savePendingBucketDeletions(pbd.copy(buckets = pbd.buckets + bucketName), txn)
    }
    gcsDAO.deleteBucket(bucketName, self)
  }

  private def recordBucketDeletion(bucketName: String) = {
    datasource.inTransaction() { txn =>
      val pbd = getPendingDeletions(txn)
      containerDAO.workspaceDAO.savePendingBucketDeletions(pbd.copy(buckets = pbd.buckets - bucketName), txn)
    }
  }
}