package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.expressions.{ExpressionValidator, SlickExpressionParser}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

//Well, this is a joke.
trait MethodWiths {
  val methodRepoDAO: MethodRepoDAO
  val dataSource: SlickDataSource
  val methodConfigResolver: MethodConfigResolver

  import dataSource.dataAccess.driver.api._

  def withMethodConfig(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, dataAccess: DataAccess)(op: (MethodConfiguration) => ReadWriteAction[PerRequestMessage])(implicit executionContext: ExecutionContext): ReadWriteAction[PerRequestMessage] = {
    dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfigurationNamespace, methodConfigurationName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exist in ${workspaceContext}")))
      case Some(methodConfiguration) => op(methodConfiguration)
    }
  }

  def withMethod[T](method: MethodRepoMethod, userInfo: UserInfo)(op: WDL => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    DBIO.from(methodRepoDAO.getMethod(method, userInfo)).asTry.flatMap {
      case Success(None) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${method.methodUri} from method repo.")))
      case Success(Some(agoraEntity)) => op(agoraEntity)
      case Failure(throwable) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.", methodRepoDAO.toErrorReport(throwable))))
    }
  }

  def withMethodInputs[T](methodConfig: MethodConfiguration, userInfo: UserInfo)(op: GatherInputsResult => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    withMethod(methodConfig.methodRepoMethod, userInfo) { wdl =>
      methodConfigResolver.gatherInputs(userInfo, methodConfig, wdl) match {
        case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
        case Success(gatherInputsResult: GatherInputsResult) =>
          op(gatherInputsResult)
      }
    }
  }

  def withValidatedMCExpressions[T](methodConfiguration: MethodConfiguration,
                                    gatherInputsResult: GatherInputsResult,
                                    allowRootEntity: Boolean,
                                    parser: SlickExpressionParser)
                                   (op: ValidatedMethodConfiguration => ReadWriteAction[T])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    val validated = ExpressionValidator.validateExpressionsForSubmission(methodConfiguration, gatherInputsResult, allowRootEntity, parser)
    DBIO.from(Future.fromTry(validated)) flatMap op
  }
}

trait UserWiths {
  val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  def withBillingProject[T](projectName: RawlsBillingProjectName, dataAccess: DataAccess)(op: RawlsBillingProject => ReadWriteAction[T])(implicit executionContext: ExecutionContext): ReadWriteAction[T] = {
    dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"billing project [${projectName.value}] not found")))
      case Some(project) => op(project)
    }
  }
}
