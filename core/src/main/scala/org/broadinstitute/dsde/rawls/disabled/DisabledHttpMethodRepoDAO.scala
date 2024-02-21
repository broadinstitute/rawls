package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.MethodRepoDAO
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.{ExecutionContext, Future}

class DisabledHttpMethodRepoDAO extends MethodRepoDAO {
  override def getMethodConfig(namespace: String,
                               name: String,
                               version: Int,
                               userInfo: UserInfo
  ): Future[Option[AgoraEntity]] =
    throw new NotImplementedError("getMethodConfig is not implemented for Azure.")
  override def getMethod(method: MethodRepoMethod, userInfo: UserInfo): Future[Option[WDL]] =
    throw new NotImplementedError("getMethod is not implemented for Azure.")
  override def postMethodConfig(namespace: String,
                                name: String,
                                methodConfiguration: MethodConfiguration,
                                userInfo: UserInfo
  ): Future[AgoraEntity] =
    throw new NotImplementedError("postMethodConfig is not implemented for Azure.")
  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] =
    throw new NotImplementedError("getStatus is not implemented for Azure.")
}
