package org.broadinstitute.dsde.rawls.monitor
import akka.actor.{Actor, Props}
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, GcpResourceTypes, MemberTypes, Workspace}
import org.broadinstitute.dsde.rawls.monitor.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import scala.language.postfixOps

object FastPassMonitor {
  sealed trait FastPassMonitorMessage
  case object DeleteExpiredGrants extends FastPassMonitorMessage
  def props(dataAccess: DataAccess, googleIamDao: GoogleIamDAO, googleStorageDao: GoogleStorageDAO): Props = Props(
    new FastPassMonitor(dataAccess, googleIamDao, googleStorageDao)
  )
}

class FastPassMonitor private (dataAccess: DataAccess, googleIamDao: GoogleIamDAO, googleStorageDao: GoogleStorageDAO)
    extends Actor {
  import context.dispatcher
  override def receive: Receive = { case DeleteExpiredGrants =>
    deleteExpiredGrants()
  }

  private def deleteExpiredGrants(): Unit =
    dataAccess.fastPassGrantQuery.findExpiredFastPassGrants().map { grants =>
      grants.to(LazyList).groupBy(_.workspaceId).foreach { case (workspaceId, grants) =>
        dataAccess.workspaceQuery.findById(workspaceId).map { maybeWorkspace =>
          val workspace = maybeWorkspace.getOrElse(throw new RuntimeException(s"Could not find workspace $workspaceId"))
          grants.groupBy(_.accountEmail).foreach { case (_, grants) =>
            removeGrantsForAccountEmailInWorkspace(workspace, grants)
          }
        }
      }
    }

  private def removeGrantsForAccountEmailInWorkspace(workspace: Workspace, grants: LazyList[FastPassGrant]): Unit = {
    val organizationRoles = grants.map(_.organizationRole).toSet
    grants foreach { grant =>
      val memberType: MemberType = matchGrantMemberType(grant)
      grant.resourceType match {
        case GcpResourceTypes.Project =>
          googleIamDao.removeIamRoles(GoogleProject(workspace.googleProjectId.value),
                                      WorkbenchEmail(grant.accountEmail.value),
                                      memberType,
                                      organizationRoles
          )
        case GcpResourceTypes.Bucket =>
          googleStorageDao.removeIamRoles(GcsBucketName(workspace.bucketName),
                                          WorkbenchEmail(grant.accountEmail.value),
                                          memberType,
                                          organizationRoles
          )
        case _ => throw new RuntimeException(s"Unsupported resource type ${grant.resourceType}")
      }
      dataAccess.fastPassGrantQuery.delete(grant.id)
    }
  }

  private def matchGrantMemberType(grant: FastPassGrant) = {
    val memberType = grant.accountType match {
      case MemberTypes.User           => MemberType.User
      case MemberTypes.ServiceAccount => MemberType.ServiceAccount
      case _                          => throw new RuntimeException(s"Unsupported member type ${grant.accountType}")
    }
    memberType
  }
}
