package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.expressions.{ExpressionValidator, SlickExpressionParser}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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

  def withMethod[T](method: MethodRepoMethod, userInfo: UserInfo)(op: (AgoraEntity) => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    DBIO.from(methodRepoDAO.getMethod(method, userInfo)).asTry.flatMap {
      case Success(None) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${method.methodUri} from method repo.")))
      case Success(Some(agoraEntity)) => op(agoraEntity)
      case Failure(throwable) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.", methodRepoDAO.toErrorReport(throwable))))
    }
  }

  def withWdl[T](method: AgoraEntity)(op: (String) => ReadWriteAction[T]): ReadWriteAction[T] = {
    method.payload match {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
      case Some(wdl) => op(wdl)
    }
  }

  def withMethodInputs[T](methodConfig: MethodConfiguration, userInfo: UserInfo)(op: (String, Seq[MethodInput], Seq[MethodInput]) => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    withMethod(methodConfig.methodRepoMethod, userInfo) { method =>
      withWdl(method) { wdl =>
        MethodConfigResolver.gatherInputs(methodConfig, wdl) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodInputs) =>
            //Remove inputs that are both empty and optional
            val (emptyOptionalInputs, inputsToProcess) = methodInputs.partition(input => input.workflowInput.optional && input.expression.isEmpty)
            op(wdl, inputsToProcess, emptyOptionalInputs)
        }
      }
    }
  }

  def withValidatedMCExpressions[T](methodConfiguration: MethodConfiguration, methodInputsToProcess: Seq[MethodConfigResolver.MethodInput], emptyOptionalMethodInputs: Seq[MethodConfigResolver.MethodInput], parser: SlickExpressionParser)
                                   (op: ValidatedMethodConfiguration => ReadWriteAction[T])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    val validated = ExpressionValidator.validateExpressionsForSubmission(methodConfiguration, methodInputsToProcess, emptyOptionalMethodInputs, parser)
    DBIO.from(Future.fromTry(validated)) flatMap op
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

  def withManagedGroupOwnerAccess[T](managedGroupRef: ManagedGroupRef, userRef: RawlsUserRef, dataAccess: DataAccess)(op: ManagedGroup => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.managedGroupQuery.load(managedGroupRef) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${managedGroupRef.membersGroupName.value}] not found")))
      case Some(managedGroup) =>
        dataAccess.rawlsGroupQuery.isGroupMember(managedGroup.adminsGroup, userRef) flatMap {
          case true => op(managedGroup)
          case false =>
            // figure out what error to show - if they are a user show 403 otherwise 404
            dataAccess.rawlsGroupQuery.isGroupMember(managedGroup.membersGroup, userRef) flatMap {
              case true => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "unauthorized")))
              case false => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${managedGroupRef.membersGroupName.value}] not found")))
            }
        }
    }
  }

}

trait SubmissionWiths extends FutureSupport {
  val dataSource: SlickDataSource
  import dataSource.dataAccess.driver.api._
  val costService: SubmissionCostService
  def withSubmissionCost[T](workflowIds: Seq[String], userInfo: UserInfo)(op: Map[String, Float] => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    toFutureTry(costService.getCostWithUser(workflowIds, userInfo)) flatMap {
      case Success(costMap) => op(costMap)
      case Failure(x) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"workflow cost for [$workflowIds] not found")))
    }
  }

}
