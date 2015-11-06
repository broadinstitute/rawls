package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsException
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

  def tryIsAdmin(userId: String): Future[Boolean] = {
    gcsDAO.isAdmin(userId) transform( s => s, t => throw new RawlsException("Unable to query for admin status.", t))
  }

  def asAdmin(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    tryIsAdmin(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }
}
