package org.broadinstitute.dsde.rawls.monitor
import akka.actor.{Actor, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.model.FastPassGrant
import org.broadinstitute.dsde.rawls.monitor.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{GoogleSubjectId, WorkbenchEmail}

import java.util.UUID
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

  private def deleteExpiredGrants(): Unit =
    dataSource.inTransaction { dataAccess =>
      dataAccess.fastPassGrantQuery.findExpiredFastPassGrants().map { expiredGrants =>
        logger.info(s"Found ${expiredGrants.size} total expired grants")
        expiredGrants.to(LazyList).groupBy(_.workspaceId).foreach { case (workspaceId, workspaceGrants) =>
          logger.info(s"Found ${workspaceGrants.size} expired grants for workspace $workspaceId")
          dataAccess.workspaceQuery.findV2WorkspaceByIdQuery(UUID.fromString(workspaceId)).map { workspace =>
            workspaceGrants.groupBy(_.accountEmail).foreach { case (accountEmail, accountEmailGrants) =>
              logger.info(
                s"Removing ${accountEmailGrants.size} grants for ${accountEmail} from workspace ${workspace.namespace}/${workspace.name}"
              )
              removeGrantsForAccountEmailInWorkspace(dataAccess,
                                                     GoogleSubjectId(workspace.googleProjectId.toString()),
                                                     workspace.bucketName.toString(),
                                                     accountEmailGrants
              )
            }
          }
        }
      }
    }

  private def removeGrantsForAccountEmailInWorkspace(dataAccess: DataAccess,
                                                     googleSubjectId: GoogleSubjectId,
                                                     bucketName: String,
                                                     grants: Iterable[FastPassGrant]
  ): Unit =
    grants.groupBy(_.resourceType).foreach { case (resourceType, resourceTypeGrants) =>
      val organizationRoles = resourceTypeGrants.map(_.organizationRole).toSet
      resourceTypeGrants foreach { resourceTypeGrant =>
        resourceType match {
          case IamResourceTypes.Project =>
            googleIamDao.removeRoles(GoogleProject(googleSubjectId.value),
                                     WorkbenchEmail(resourceTypeGrant.accountEmail.value),
                                     resourceTypeGrant.accountType,
                                     organizationRoles
            )
          case IamResourceTypes.Bucket =>
            googleStorageDao.removeIamRoles(GcsBucketName(bucketName),
                                            WorkbenchEmail(resourceTypeGrant.accountEmail.value),
                                            resourceTypeGrant.accountType,
                                            organizationRoles
            )
          case _ => throw new RuntimeException(s"Unsupported resource type ${resourceTypeGrant.resourceType}")
        }
        dataAccess.fastPassGrantQuery.delete(resourceTypeGrant.id)
      }
    }
}
