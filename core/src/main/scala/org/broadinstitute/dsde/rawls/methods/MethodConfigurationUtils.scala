package org.broadinstitute.dsde.rawls.methods

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.MethodRepoDAO
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{ErrorReport, MethodConfiguration, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.util.FutureSupport.toFutureTry

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object MethodConfigurationUtils {

  def gatherMethodConfigInputs(ctx: RawlsRequestContext,
                               methodRepoDAO: MethodRepoDAO,
                               methodConfig: MethodConfiguration,
                               methodConfigResolver: MethodConfigResolver
  )(implicit executionContext: ExecutionContext): Future[MethodConfigResolver.GatherInputsResult] =
    toFutureTry(methodRepoDAO.getMethod(methodConfig.methodRepoMethod, ctx.userInfo)).map {
      case Success(None) =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.NotFound,
            s"Cannot get ${methodConfig.methodRepoMethod.methodUri} from method repo."
          )
        )
      case Success(Some(wdl)) =>
        methodConfigResolver
          .gatherInputs(ctx.userInfo, methodConfig, wdl)
          .recoverWith { case regrets =>
            Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regrets)))
          }
          .get
      case Failure(throwable) =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.BadGateway,
            s"Unable to query the method repo.",
            methodRepoDAO.toErrorReport(throwable)
          )
        )
    }
}
