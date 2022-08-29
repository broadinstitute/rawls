package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsRequestContext, RawlsUserEmail, UserInfo}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 11/5/15.
 */
trait RoleSupport {
  protected val gcsDAO: GoogleServicesDAO
  protected val ctx: RawlsRequestContext
  implicit protected val executionContext: ExecutionContext

  def tryIsFCAdmin(userEmail: RawlsUserEmail): Future[Boolean] =
    gcsDAO.isAdmin(userEmail.value) recoverWith { case t =>
      throw new RawlsException("Unable to query for admin status.", t)
    }

  def asFCAdmin[T](op: => Future[T]): Future[T] =
    tryIsFCAdmin(ctx.userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op
      else
        Future.failed(
          new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be an admin."))
        )
    }

  def tryIsCurator(userEmail: RawlsUserEmail): Future[Boolean] =
    gcsDAO.isLibraryCurator(userEmail.value) recoverWith { case t =>
      throw new RawlsException("Unable to query for library curator status.", t)
    }

  def asCurator[T](op: => Future[T]): Future[T] =
    tryIsCurator(ctx.userInfo.userEmail) flatMap { isCurator =>
      if (isCurator) op
      else
        Future.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a library curator.")
          )
        )
    }
}
