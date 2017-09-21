package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{ErrorReportSource, ErrorReportable, UserInfo, UserStatus}

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("sam")
  def registerUser(userInfo: UserInfo): Future[Option[UserStatus]]
}
