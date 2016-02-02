package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorRef
import akka.pattern._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, DbContainerDAO, DataSource}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.{PendingBucketDeletions, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def restartMonitors(dataSource: DataSource, containerDAO: DbContainerDAO, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef) = {
    startBucketDeletionMonitor(dataSource, containerDAO, bucketDeletionMonitor)
    startSubmissionMonitor(dataSource, containerDAO, gcsDAO, submissionSupervisor)
  }

  private def startBucketDeletionMonitor(dataSource: DataSource, containerDAO: DbContainerDAO, bucketDeletionMonitor: ActorRef) = {
    dataSource.inTransaction() { txn =>
      containerDAO.workspaceDAO.loadPendingBucketDeletions(txn) match {
        case Some(pendingDeletions) => pendingDeletions.buckets foreach {
          bucketDeletionMonitor ! DeleteBucket(_)
        }
        case None => containerDAO.workspaceDAO.savePendingBucketDeletions(PendingBucketDeletions(Set.empty), txn)
      }
    }
  }

  private def startSubmissionMonitor(dataSource: DataSource, containerDAO: DbContainerDAO, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef) = {
    dataSource.inTransaction() { txn =>
      containerDAO.submissionDAO.listAllActiveSubmissions(txn) foreach { activeSub =>
        val wsName = WorkspaceName(activeSub.workspaceNamespace, activeSub.workspaceName)
        val submitter = activeSub.submission.submitter
        val subId = activeSub.submission.submissionId

        val subStartMessage = gcsDAO.getUserCredentials(submitter) map { credentialOpt =>
          credentialOpt match {
            case None => throw new RawlsException(s"Cannot start Submission Monitor because credentials were not retrieved for user ${submitter.userSubjectId.value}, submitter of ${subId}")
            case Some(credential) => SubmissionStarted(wsName, subId, credential)
          }
        }

        subStartMessage onComplete {
          case Success(message) => submissionSupervisor ! message
          case Failure(throwable) => logger.error(s"Error restarting submission monitor for submission ${subId}", throwable)
        }
      }
    }
  }
}