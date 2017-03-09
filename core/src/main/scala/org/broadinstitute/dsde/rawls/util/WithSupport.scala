package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import spray.http.StatusCodes

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Try, Failure, Success}

//Well, this is a joke.
trait MethodWiths {
  val methodRepoDAO: MethodRepoDAO
  val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  def withMethodConfig(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, dataAccess: DataAccess)(op: (MethodConfiguration) => ReadWriteAction[PerRequestMessage])(implicit executionContext: ExecutionContext): ReadWriteAction[PerRequestMessage] = {
    dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfigurationNamespace, methodConfigurationName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exist in ${workspaceContext}")))
      case Some(methodConfiguration) => op(methodConfiguration)
    }
  }

  def withMethod[T](methodNamespace: String, methodName: String, methodVersion: Int, userInfo: UserInfo)(op: (AgoraEntity) => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    DBIO.from(methodRepoDAO.getMethod(methodNamespace, methodName, methodVersion, userInfo)).asTry.flatMap { agoraEntityOption => agoraEntityOption match {
      case Success(None) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodNamespace}/${methodName}/${methodVersion} from method repo.")))
      case Success(Some(agoraEntity)) => op(agoraEntity)
      case Failure(throwable) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.",methodRepoDAO.toErrorReport(throwable))))
    }}
  }

  def withWdl[T](method: AgoraEntity)(op: (String) => ReadWriteAction[T]): ReadWriteAction[T] = {
    method.payload match {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
      case Some(wdl) => op(wdl)
    }
  }

  def withMethodInputs[T](methodConfig: MethodConfiguration, userInfo: UserInfo)(op: (String, Seq[MethodInput]) => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    val methodRepoMethod = methodConfig.methodRepoMethod
    withMethod(methodRepoMethod.methodNamespace, methodRepoMethod.methodName, methodRepoMethod.methodVersion, userInfo) { method =>
      withWdl(method) { wdl => MethodConfigResolver.gatherInputs(methodConfig,wdl) match {
        case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
        case Success(methodInputs) => op(wdl,methodInputs)
      }}
    }
  }
}

trait UserWiths {
  val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  def withUser[T](rawlsUserRef: RawlsUserRef, dataAccess: DataAccess)(op: RawlsUser => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.rawlsUserQuery.load(rawlsUserRef) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"user [${rawlsUserRef.userSubjectId.value}] not found")))
      case Some(user) => op(user)
    }
  }

  def withUser[T](userEmail: RawlsUserEmail, dataAccess: DataAccess)(op: RawlsUser => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.rawlsUserQuery.loadUserByEmail(userEmail) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"user [${userEmail.value}] not found")))
      case Some(user) => op(user)
    }
  }

  def withGroup[T](rawlsGroupRef: RawlsGroupRef, dataAccess: DataAccess)(op: RawlsGroup => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.rawlsGroupQuery.load(rawlsGroupRef) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found")))
      case Some(group) => op(group)
    }
  }

  def withBillingProject[T](projectName: RawlsBillingProjectName, dataAccess: DataAccess)(op: RawlsBillingProject => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"billing project [${projectName.value}] not found")))
      case Some(project) => op(project)
    }
  }

  def withManagedGroupOwnerAccess[T](managedGroupRef: ManagedGroupRef, userRef: RawlsUserRef, dataAccess: DataAccess)(op: ManagedGroupFull => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.managedGroupQuery.loadFull(managedGroupRef) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${managedGroupRef.usersGroupName.value}] not found")))
      case Some(managedGroup) =>
        dataAccess.rawlsGroupQuery.isGroupMember(managedGroup.ownersGroup, userRef) flatMap {
          case true => op(managedGroup)
          case false =>
            // figure out what error to show - if they are a user show 403 otherwise 404
            dataAccess.rawlsGroupQuery.isGroupMember(managedGroup.ownersGroup, userRef) flatMap {
              case true => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "unauthorized")))
              case false => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${managedGroupRef.usersGroupName.value}] not found")))
            }
        }
    }
  }

}
