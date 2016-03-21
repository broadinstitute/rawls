package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorRef
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource

// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def restartMonitors(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef) = {
    startBucketDeletionMonitor(dataSource, bucketDeletionMonitor)
    startSubmissionMonitor(dataSource, gcsDAO, submissionSupervisor)
  }

  private def startBucketDeletionMonitor(dataSource: SlickDataSource, bucketDeletionMonitor: ActorRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.list() map { _.map { pbd =>
          bucketDeletionMonitor ! DeleteBucket(pbd.bucket)
        }
      }
    } onFailure {
      case t: Throwable => logger.error("Error starting bucket deletion monitor", t)
    }
  }

  private def startSubmissionMonitor(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.listAllActiveSubmissions() map { _.map { activeSub =>
        val wsName = WorkspaceName(activeSub.workspaceNamespace, activeSub.workspaceName)
        val submitter = activeSub.submission.submitter
        val subId = activeSub.submission.submissionId

        val subStartMessage = gcsDAO.getUserCredentials(submitter) map {
          case None => throw new RawlsException(s"Cannot start Submission Monitor because credentials were not retrieved for user ${submitter.userSubjectId.value}, submitter of ${subId}")
          case Some(credential) => SubmissionStarted(wsName, subId, credential)
        }

        subStartMessage onComplete {
          case Success(message) => submissionSupervisor ! message
          case Failure(throwable) => logger.error(s"Error restarting submission monitor for submission ${subId}", throwable)
        }
      }}
    } onFailure {
      case t: Throwable => logger.error("Error starting submission monitor", t)
    }
  }
}