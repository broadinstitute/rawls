package org.broadinstitute.dsde.rawls.fastpass

import akka.actor.{Actor, Props}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.fastpass.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, Workspace}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import slick.dbio.DBIO

import scala.concurrent.Future
import scala.language.postfixOps

object FastPassMonitor {
  sealed trait FastPassMonitorMessage
  case object DeleteExpiredGrants extends FastPassMonitorMessage
  def props(dataSource: SlickDataSource, googleIamDao: GoogleIamDAO, googleStorageDao: GoogleStorageDAO)(implicit
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Props = Props(
    new FastPassMonitor(dataSource, googleIamDao, googleStorageDao)
  )
}

class FastPassMonitor private (dataSource: SlickDataSource,
                               googleIamDao: GoogleIamDAO,
                               googleStorageDao: GoogleStorageDAO
)(implicit openTelemetry: OpenTelemetryMetrics[IO])
    extends Actor
    with LazyLogging {
  import context.dispatcher

  private val openTelemetryTags: Map[String, String] = Map("service" -> "FastPassMonitor")
  override def receive: Receive = { case DeleteExpiredGrants =>
    openTelemetry.incrementCounter("fastpass-monitor-receive-msg", tags = openTelemetryTags).unsafeToFuture()
    deleteExpiredGrants()
  }

  private def deleteExpiredGrants(): Future[ReadWriteAction[Unit]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.fastPassGrantQuery
        .findExpiredFastPassGrants()
        .flatMap { expiredGrants =>
          logger.info(s"Found ${expiredGrants.size} total expired grants")
          // Convert to stream and group by workspaceId
          DBIO.sequence(expiredGrants.to(LazyList).groupBy(_.workspaceId).map { case (workspaceId, workspaceGrants) =>
            logger.info(s"Found ${workspaceGrants.size} expired grants for workspace $workspaceId")
            // Query workspace info then group by accountEmail
            dataAccess.workspaceQuery.findByIdOrFail(workspaceId).flatMap { workspace =>
              DBIO.sequence(workspaceGrants.groupBy(_.accountEmail).map { case (_, accountEmailGrants) =>
                // Remove grants for a given workspace and accountEmail
                removeGrantsForAccountEmailInWorkspace(dataAccess, workspace, accountEmailGrants)
              })
            }
          })
        }
        .map(_ => DBIO.successful(()))
    }

  private def removeGrantsForAccountEmailInWorkspace(dataAccess: DataAccess,
                                                     workspace: Workspace,
                                                     grants: Iterable[FastPassGrant]
  ): ReadWriteAction[Unit] = {
    // Projects and buckets need different api calls, so group by resourceType
    grants.groupBy(_.resourceType).map { case (resourceType, resourceTypeGrants) =>
      val organizationRoles = resourceTypeGrants.map(_.organizationRole).toSet
      // The grouped resourceTypeGrants are the same except for roles, so we can just take the first one
      val resourceTypeGrant = resourceTypeGrants.head
      // For a given resource type, remove the roles for the accountEmail
      resourceType match {
        case IamResourceTypes.Project =>
          googleIamDao.removeRoles(GoogleProject(workspace.googleProjectId.value),
                                   WorkbenchEmail(resourceTypeGrant.accountEmail.value),
                                   resourceTypeGrant.accountType,
                                   organizationRoles
          )
        case IamResourceTypes.Bucket =>
          googleStorageDao.removeIamRoles(
            GcsBucketName(workspace.bucketName),
            WorkbenchEmail(resourceTypeGrant.accountEmail.value),
            resourceTypeGrant.accountType,
            organizationRoles,
            userProject = Some(GoogleProject(workspace.googleProjectId.value))
          )
        case _ => throw new RuntimeException(s"Unsupported resource type ${resourceTypeGrant.resourceType}")
      }
      openTelemetry.incrementCounter("fastpass-monitor-google-call", tags = openTelemetryTags).unsafeToFuture()
    }
    // Once all the api calls have been made remove all the corresponding grants from the db
    DBIO.from(
      openTelemetry.incrementCounter("fastpass-monitor-grant-delete", tags = openTelemetryTags).unsafeToFuture()
    )
    DBIO.sequence(grants.map(grant => dataAccess.fastPassGrantQuery.delete(grant.id)))
  }.map(_ => DBIO.successful(()))
}
