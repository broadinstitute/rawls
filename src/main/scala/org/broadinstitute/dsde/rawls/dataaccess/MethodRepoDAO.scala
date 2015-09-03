package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{UserInfo, AgoraEntity}
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait MethodRepoDAO {
  def getMethodConfig( namespace: String, name: String, version: String, userInfo: UserInfo ): Option[AgoraEntity]
  def getMethod( namespace: String, name: String, version: String, userInfo: UserInfo ): Option[AgoraEntity]
}
