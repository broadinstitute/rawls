package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.model.{UserInfo, ErrorReport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestComplete, PerRequestMessage}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 11/5/15.
 */
trait RoleSupport {
  protected val gcsDAO: GoogleServicesDAO
  protected val userInfo: UserInfo
  implicit protected val executionContext: ExecutionContext

  def tryIsFCAdmin(userId: String): Future[Boolean] = {
    gcsDAO.isAdmin(userId) recoverWith { case t => throw new RawlsException("Unable to query for admin status.", t) }
  }

  def asFCAdmin(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsFCAdmin(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }
  
  def tryIsCurator(userId: String): Future[Boolean] = {
    gcsDAO.isLibraryCurator(userId) recoverWith { case t => throw new RawlsException("Unable to query for library curator status.", t) }
  }

  def asCurator(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsCurator(userInfo.userEmail) flatMap { isCurator =>
      if (isCurator) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a library curator.")))
    }
  }
}
