package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserSubjectId, SubsystemStatus, UserInfo, UserStatus}

import scala.concurrent.Future

class MockSamDAO(ok: Boolean = true) extends SamDAO {
  val serviceAccount = RawlsUserEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val userAccount = UserStatus(RawlsUser(RawlsUserSubjectId("1234567890"),RawlsUserEmail("valid-user")), enabled = Map[String, Boolean]())

  override def getStatus(): Future[SubsystemStatus] = {
    if (ok) Future.successful(SubsystemStatus(true, None))
    else Future.successful(SubsystemStatus(false, None))
  }

  override def getUserStatus(userInfo: UserInfo):Future[Option[UserStatus]] = {
    if(ok) Future.successful(Some(userAccount))
    else Future.successful(None)
  }


  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = ???
}