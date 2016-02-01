package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, UserInfo, AgoraEntity}
import scala.concurrent.Future

/**
 * @author tsharpe
 */
trait MethodRepoDAO extends ErrorReportable {
  override val errorReportSource = "agora"
  def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]
  def postMethodConfig( namespace: String, name: String, methodConfig: MethodConfiguration, userInfo: UserInfo ): Future[AgoraEntity]
  def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]
}
