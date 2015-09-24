package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, UserInfo, AgoraEntity}
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait MethodRepoDAO {
  def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Option[AgoraEntity]
  def postMethodConfig( namespace: String, name: String, methodConfig: MethodConfiguration, userInfo: UserInfo ): AgoraEntity
  def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Option[AgoraEntity]
}
