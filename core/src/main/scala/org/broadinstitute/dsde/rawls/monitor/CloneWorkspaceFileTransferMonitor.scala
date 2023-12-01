package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.client.http.HttpResponseException
import com.google.api.services.storage.model.StorageObject
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CloneWorkspaceFileTransferMonitor.CheckAll
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object CloneWorkspaceFileTransferMonitor {
  def props(dataSource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            initialDelay: FiniteDuration,
            pollInterval: FiniteDuration
  )(implicit executionContext: ExecutionContext): Props =
    Props(new CloneWorkspaceFileTransferMonitorActor(dataSource, gcsDAO, initialDelay, pollInterval))

  sealed trait CloneWorkspaceFileTransferMonitorMessage
  case object CheckAll extends CloneWorkspaceFileTransferMonitorMessage
}

class CloneWorkspaceFileTransferMonitorActor(val dataSource: SlickDataSource,
                                             val gcsDAO: GoogleServicesDAO,
                                             val initialDelay: FiniteDuration,
                                             val pollInterval: FiniteDuration
)(implicit executionContext: ExecutionContext)
    extends Actor
    with LazyLogging {

  context.system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, self, CheckAll)

  override def receive = { case CheckAll =>
    checkAll(context.dispatcher)
  }

  private def checkAll(implicit executionContext: ExecutionContext) =
    for {
      pendingTransfers <- dataSource.inTransaction { dataAccess =>
        dataAccess.cloneWorkspaceFileTransferQuery.listPendingTransfers()
      }
      _ <- pendingTransfers.toList
        .traverse { pendingTransfer =>
          IO.fromFuture(IO(attemptTransfer(pendingTransfer))).attempt.map {
            case Left(e) =>
              // We do not want to throw e here. traverse stops executing as soon as it encounters a Failure, but we
              // want to continue traversing the list to transfer the rest of the buckets even if one of the
              // copy operations fails.
              logger.warn(
                s"Failed to copy files from ${pendingTransfer.sourceWorkspaceBucketName} to ${pendingTransfer.destWorkspaceBucketName}",
                e
              )
              List.empty
            case Right(res) => res
          }
        }
        .unsafeToFuture()
    } yield ()

  private def attemptTransfer(
    pendingTransfer: PendingCloneWorkspaceFileTransfer
  )(implicit executionContext: ExecutionContext): Future[List[Option[StorageObject]]] = {
    val transferExpired = pendingTransfer.created.isBefore(DateTime.now().minusDays(1))
    for {
      copiedObjects <-
        if (!transferExpired) copyBucketFiles(pendingTransfer)
        else {
          logger.warn(
            s"File transfer from ${pendingTransfer.sourceWorkspaceBucketName} to ${pendingTransfer.destWorkspaceBucketName} did not succeed within allowed time and will no longer be retried. [workspaceId=${pendingTransfer.destWorkspaceId}]"
          )
          Future.successful(List.empty)
        }

      _ <- markTransferAsComplete(pendingTransfer, transferSucceeded = !transferExpired)
    } yield copiedObjects
  }

  private def copyBucketFiles(
    pendingCloneWorkspaceFileTransfer: PendingCloneWorkspaceFileTransfer
  )(implicit executionContext: ExecutionContext): Future[List[Option[StorageObject]]] =
    for {
      objectsToCopy <- gcsDAO
        .listObjectsWithPrefix(
          pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName,
          pendingCloneWorkspaceFileTransfer.copyFilesWithPrefix,
          Option(pendingCloneWorkspaceFileTransfer.destWorkspaceGoogleProjectId)
        )
        .recoverWith {
          case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
            logger.warn(
              s"403 received when listing objects in ${pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName} before copying to ${pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName}"
            )
            Future.failed(e)
        }
      copiedObjects <- Future.traverse(objectsToCopy) { objectToCopy =>
        gcsDAO
          .copyFile(
            pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName,
            objectToCopy.getName,
            pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName,
            objectToCopy.getName,
            Option(pendingCloneWorkspaceFileTransfer.destWorkspaceGoogleProjectId)
          )
          .recoverWith {
            case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
              logger.warn(
                s"403 received when copying [${pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName}/${objectToCopy.getName}] to [${pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName}]"
              )
              Future.failed(e)
          }
      }
      _ = logger.info(
        s"successfully copied files with prefix ${pendingCloneWorkspaceFileTransfer.copyFilesWithPrefix} from ${pendingCloneWorkspaceFileTransfer.sourceWorkspaceBucketName} to ${pendingCloneWorkspaceFileTransfer.destWorkspaceBucketName}"
      )
    } yield copiedObjects

  private def markTransferAsComplete(
    pendingCloneWorkspaceFileTransfer: PendingCloneWorkspaceFileTransfer,
    transferSucceeded: Boolean
  ): Future[Unit] = {
    val currentTime = DateTime.now()

    dataSource.inTransaction { dataAccess =>
      for {
        _ <- dataAccess.cloneWorkspaceFileTransferQuery.update(
          pendingCloneWorkspaceFileTransfer.copy(finished = currentTime.some,
                                                 outcome = if (transferSucceeded) "Success".some else "Failure".some
          )
        )
        _ <- dataAccess.workspaceQuery.updateCompletedCloneWorkspaceFileTransfer(
          pendingCloneWorkspaceFileTransfer.destWorkspaceId,
          currentTime.toDate
        )
      } yield ()
    }
  }
}

final case class CloneWorkspaceFileTransferMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
