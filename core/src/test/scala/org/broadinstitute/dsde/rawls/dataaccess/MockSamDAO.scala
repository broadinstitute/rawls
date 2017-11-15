package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{SubsystemStatus, UserInfo, UserStatus}

import scala.concurrent.Future

/**
  * Created by mbemis on 11/9/17.
  */
class MockSamDAO extends SamDAO {
  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = Future.successful(None)

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = Future.successful(true)

  override def getStatus(): Future[SubsystemStatus] = Future.successful(SubsystemStatus(true, None))
}
