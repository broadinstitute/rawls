package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.model.{UserInfo, ErrorReport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import spray.http.StatusCodes
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 11/5/15.
 */
trait RoleSupport {
  protected val gcsDAO: GoogleServicesDAO
  protected val userInfo: UserInfo
  implicit protected val executionContext: ExecutionContext

  def tryIsFCAdmin(userEmail: String): Future[Boolean] = {
    gcsDAO.isAdmin(userEmail) recoverWith { case t => throw new RawlsException("Unable to query for admin status.", t) }
  }

  def asFCAdmin(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsFCAdmin(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }
  
  def tryIsCurator(userEmail: String): Future[Boolean] = {
    gcsDAO.isLibraryCurator(userEmail) recoverWith { case t => throw new RawlsException("Unable to query for library curator status.", t) }
  }

  def asCurator(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsCurator(userInfo.userEmail) flatMap { isCurator =>
      if (isCurator) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a library curator.")))
    }
  }
}
