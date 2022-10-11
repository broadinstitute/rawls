package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author tsharpe
 */
trait MethodRepoDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("agora")
  def getMethodConfig(namespace: String, name: String, version: Int, userInfo: UserInfo): Future[Option[AgoraEntity]]
  def postMethodConfig(namespace: String,
                       name: String,
                       methodConfig: MethodConfiguration,
                       userInfo: UserInfo
  ): Future[AgoraEntity]
  def getMethod(method: MethodRepoMethod, userInfo: UserInfo): Future[Option[WDL]]
  def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus]
}
