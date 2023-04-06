package org.broadinstitute.dsde.rawls.fastpass

import akka.actor.{Actor, Props}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.fastpass.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, Workspace}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import slick.dbio.DBIO

import scala.concurrent.Future
import scala.language.postfixOps
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

object FastPassMonitor {
  sealed trait FastPassMonitorMessage
  case object DeleteExpiredGrants extends FastPassMonitorMessage
  def props(dataSource: SlickDataSource, googleIamDAO: GoogleIamDAO, googleStorageDao: GoogleStorageDAO)(implicit
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Props = Props(
    new FastPassMonitor(dataSource, googleIamDAO, googleStorageDao)
  )
}

class FastPassMonitor private (dataSource: SlickDataSource,
                               googleIamDAO: GoogleIamDAO,
                               googleStorageDao: GoogleStorageDAO
)(implicit openTelemetry: OpenTelemetryMetrics[IO])
    extends Actor
    with LazyLogging {
  import context.dispatcher

  private val openTelemetryTags: Map[String, String] = Map("service" -> "FastPassMonitor")
  override def receive: Receive = { case DeleteExpiredGrants =>
    for {
      _ <- openTelemetry.incrementCounter("fastpass-monitor-sweeper-start", tags = openTelemetryTags).unsafeToFuture()
      _ <- deleteExpiredGrants()
    } yield ()
  }

  /*
   * Remove google roles for expired grants and delete the grants from the database.
   * This is done in a transaction so that if any of the google calls fail, the grants are not deleted.
   * In order to reduce the number of api and db calls, we group the grants by workspaceId, accountEmail, and resourceType.
   */
  private def deleteExpiredGrants(): Future[Unit] =
    for {
      grantsGroupedByEmail <- findFastPassGrantsToRemove()
    } yield Future.sequence(removeFastPassGrants(grantsGroupedByEmail))

  private def findFastPassGrantsToRemove(): Future[Iterable[((Workspace, WorkbenchEmail), Seq[FastPassGrant])]] =
    dataSource.inTransaction { dataAccess =>
      for {
        expiredGrants <- dataAccess.fastPassGrantQuery.findExpiredFastPassGrants()
        _ = logger.info(s"Found ${expiredGrants.size} FastPass grants to clean up")
        groupedByWorkspaceId = expiredGrants.groupBy(_.workspaceId)
        groupedByWorkspace <- DBIO.sequence(groupedByWorkspaceId.map { case (workspaceId, workspaceGrants) =>
          dataAccess.workspaceQuery.findByIdOrFail(workspaceId).map(workspace => workspace -> workspaceGrants)
        })
        _ = groupedByWorkspace.foreach(t =>
          logger.info(s"Found ${t._2.size} FastPass grants in ${t._1.toWorkspaceName} to clean up")
        )
        groupedByEmail = groupedByWorkspace.flatMap { case (workspace, workspaceGrants) =>
          workspaceGrants.groupBy(_.accountEmail).map { case (email, grants) => (workspace, email) -> grants }
        }
        _ = logger.info(s"Found ${groupedByEmail.size} emails to remove")
      } yield groupedByEmail
    }

  private def removeFastPassGrants(
    groupedFastPassGrants: Iterable[((Workspace, WorkbenchEmail), Seq[FastPassGrant])]
  ): Iterable[Future[Unit]] =
    groupedFastPassGrants.map { case ((workspace, workbenchEmail), grantsByEmail) =>
      dataSource.inTransaction { dataAccess =>
        logger.info(
          s"Removing ${grantsByEmail.size} FastPass grants for ${workbenchEmail.value} in ${workspace.toWorkspaceName}"
        )
        FastPassService
          .removeFastPassGrantsInWorkspaceProject(grantsByEmail,
                                                  workspace.googleProjectId,
                                                  dataAccess,
                                                  googleIamDAO,
                                                  googleStorageDao,
                                                  None
          )
          .cleanUp {
            case Some(e) =>
              logger.error(
                s"Encountered an error while removing FastPass grants for ${workbenchEmail.value}. Continuing sweep.",
                e
              )
              DBIO.successful()
            case None => DBIO.successful()
          }
      }
    }
}
