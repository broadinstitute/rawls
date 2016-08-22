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
trait AdminSupport {
  protected val gcsDAO: GoogleServicesDAO
  protected val userInfo: UserInfo
  implicit protected val executionContext: ExecutionContext

  def tryIsFCAdmin(userId: String): Future[Boolean] = {
    gcsDAO.isAdmin(userId) transform( s => s, t => throw new RawlsException("Unable to query for admin status.", t))
  }

  def asFCAdmin(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsFCAdmin(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }

  //maybe generic-ify this?
  def tryIsFCCurator(userId: String): Future[Boolean] = {
    gcsDAO.isCurator(userId) transform( s => s, t => throw new RawlsException("Unable to query for curator status.", t))
  }

  def asFCCurator(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsFCCurator(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }
}
