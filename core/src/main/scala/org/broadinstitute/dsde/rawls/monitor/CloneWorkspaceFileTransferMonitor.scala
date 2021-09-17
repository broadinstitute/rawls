package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.google.api.services.storage.model.StorageObject
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CloneWorkspaceFileTransferMonitor.CheckAll

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps


object CloneWorkspaceFileTransferMonitor {
  def props(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props = {
    Props(new CloneWorkspaceFileTransferMonitorActor(dataSource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait CloneWorkspaceFileTransferMonitorMessage
  case object CheckAll extends CloneWorkspaceFileTransferMonitorMessage
}

class CloneWorkspaceFileTransferMonitorActor(val dataSource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val initialDelay: FiniteDuration, val pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]) extends Actor with LazyLogging {

  context.system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, self, CheckAll)

  override def receive = {
    case CheckAll => checkAll()
  }

  private def checkAll() = {
    for {
      pendingTransfers <- dataSource.inTransaction { dataAccess =>
        dataAccess.cloneWorkspaceFileTransferQuery.listPendingTransfers()
      }
      _ <- pendingTransfers.toList.traverse { pendingTransfer =>
        IO.fromFuture(IO(copyBucketFiles(pendingTransfer)))
      }.unsafeToFuture()
    } yield()
  }

  private def copyBucketFiles(pendingCloneWorkspaceFileTransfer: PendingCloneWorkspaceFileTransfer): Future[List[Option[StorageObject]]] = {
    for {
      objectsToCopy <- gcsDAO.listObjectsWithPrefix(pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName, pendingCloneWorkspaceFileTransfer.copyFilesWithPrefix, Option(pendingCloneWorkspaceFileTransfer.destWorkspaceGoogleProjectId)).recoverWith {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) => {
          logger.warn(s"403 received when listing objects in ${pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName} before copying to ${pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName}: ${e.errorReport.message}")
          Future.failed(e)
        }
      }
      copiedObjects <- Future.traverse(objectsToCopy) { objectToCopy =>
        gcsDAO.copyFile(pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName, objectToCopy.getName, pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName, objectToCopy.getName, Option(pendingCloneWorkspaceFileTransfer.destWorkspaceGoogleProjectId)).recoverWith {
          case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) => {
            logger.warn(s"403 received when copying [${pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName}/${objectToCopy}] to [${pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName}]: $e")
            Future.failed(e)
          }
        }
      }
      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.cloneWorkspaceFileTransferQuery.delete(pendingCloneWorkspaceFileTransfer.destWorkspaceId)
      }
    } yield copiedObjects
  }
}

final case class CloneWorkspaceFileTransferMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
