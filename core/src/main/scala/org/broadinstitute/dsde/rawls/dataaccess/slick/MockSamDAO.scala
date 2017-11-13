package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserSubjectId, SubsystemStatus, UserInfo, UserStatus}
import scala.concurrent.Future

class MockSamDAO(ok: Boolean = true) extends SamDAO {
  val userAccount = UserStatus(RawlsUser(RawlsUserSubjectId("123456789876543210101"),RawlsUserEmail("project-owner-access")), enabled = Map[String, Boolean]())
  val notPetServiceAccount = UserStatus(RawlsUser(RawlsUserSubjectId("123456789876543210202"),RawlsUserEmail("project-owner-access-sa")), enabled = Map[String, Boolean]())

  override def getStatus(): Future[SubsystemStatus] = {
    if (ok) Future.successful(SubsystemStatus(true, None))
    else Future.successful(SubsystemStatus(false, None))
  }

  override def getUserStatus(userInfo: UserInfo):Future[Option[UserStatus]] = {
    if(ok) {
      if(userInfo.userEmail.value.startsWith("pet-")) {
        Future.successful(Some(userAccount))
      }
      else {
        Future.successful(Some(notPetServiceAccount))
      }
    }
    else Future.successful(None)
  }


  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    if(ok) Future.successful(Some(userAccount))
    else Future.successful(None)
  }
}