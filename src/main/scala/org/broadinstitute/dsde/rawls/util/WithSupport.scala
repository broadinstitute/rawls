package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{AgoraEntity, UserInfo, ErrorReport, MethodConfiguration}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import spray.http.StatusCodes

import scala.concurrent.ExecutionContext
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
      withWdl(method) { wdl => Try(MethodConfigResolver.gatherInputs(methodConfig,wdl)) match {
        case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(exception,StatusCodes.BadRequest)))
        case Success(methodInputs) => op(wdl,methodInputs)
      }}
    }
  }
}
