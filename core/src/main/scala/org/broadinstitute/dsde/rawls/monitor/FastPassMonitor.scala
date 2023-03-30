package org.broadinstitute.dsde.rawls.monitor
import akka.actor.{Actor, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, Workspace}
import org.broadinstitute.dsde.rawls.monitor.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{GoogleSubjectId, WorkbenchEmail}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import java.util.UUID
import scala.collection.immutable
import scala.concurrent.Future
import scala.language.postfixOps

object FastPassMonitor {
  sealed trait FastPassMonitorMessage
  case object DeleteExpiredGrants extends FastPassMonitorMessage
  def props(dataSource: SlickDataSource, googleIamDao: GoogleIamDAO, googleStorageDao: GoogleStorageDAO): Props = Props(
    new FastPassMonitor(dataSource, googleIamDao, googleStorageDao)
  )
}

class FastPassMonitor private (dataSource: SlickDataSource,
                               googleIamDao: GoogleIamDAO,
                               googleStorageDao: GoogleStorageDAO
) extends Actor
    with LazyLogging {
  import context.dispatcher
  override def receive: Receive = { case DeleteExpiredGrants =>
    deleteExpiredGrants()
  }

  private def deleteExpiredGrants(): Future[ReadWriteAction[Unit]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.fastPassGrantQuery
        .findExpiredFastPassGrants()
        .flatMap { expiredGrants =>
          logger.info(s"Found ${expiredGrants.size} total expired grants")
          DBIO.sequence(expiredGrants.to(LazyList).groupBy(_.workspaceId).map { case (workspaceId, workspaceGrants) =>
            logger.info(s"Found ${workspaceGrants.size} expired grants for workspace $workspaceId")
            dataAccess.workspaceQuery.findByIdOrFail(workspaceId).flatMap { workspace =>
              DBIO.sequence(workspaceGrants.groupBy(_.accountEmail).map { case (_, accountEmailGrants) =>
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
    grants.groupBy(_.resourceType).map { case (resourceType, resourceTypeGrants) =>
      val organizationRoles = resourceTypeGrants.map(_.organizationRole).toSet
      // The grouped resourceTypeGrants are the same except for roles, so we can just take the first one
      val resourceTypeGrant = resourceTypeGrants.head
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
    }
    DBIO.sequence(grants.map(grant => dataAccess.fastPassGrantQuery.delete(grant.id)))
  }.map(_ => DBIO.successful(()))
}
