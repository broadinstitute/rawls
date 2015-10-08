package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, UserInfo, AgoraEntity}
import spray.http.HttpCookie

import scala.concurrent.Future
import scala.util.Try

/**
 * @author tsharpe
 */
trait MethodRepoDAO {
  def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]
  def postMethodConfig( namespace: String, name: String, methodConfig: MethodConfiguration, userInfo: UserInfo ): Future[AgoraEntity]
  def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]
}
