package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import scala.concurrent.{ExecutionContext, Future}

trait UserUtils {
  implicit protected val executionContext: ExecutionContext
  val samDAO: SamDAO

  def collectMissingUsers(userEmails: Set[String], ctx: RawlsRequestContext): Future[Set[String]] =
    Future
      .traverse(userEmails) { email =>
        samDAO.getUserIdInfo(email, ctx).map {
          case SamDAO.NotFound => Option(email)
          case _ => None
        }
      }
      .map(_.flatten)

}
