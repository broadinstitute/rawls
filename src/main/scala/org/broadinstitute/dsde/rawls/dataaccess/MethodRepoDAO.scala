package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{ErrorReport, MethodConfiguration, UserInfo, AgoraEntity}
import spray.httpx.UnsuccessfulResponseException
import scala.concurrent.Future

/**
 * @author tsharpe
 */
trait MethodRepoDAO {
  def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]
  def postMethodConfig( namespace: String, name: String, methodConfig: MethodConfiguration, userInfo: UserInfo ): Future[AgoraEntity]
  def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]]

  def toErrorReport(throwable: Throwable) = {
    val SOURCE = "agora"
    throwable match {
      case ure: UnsuccessfulResponseException =>
        ErrorReport(SOURCE, ErrorReport.message(ure), Option(ure.response.status), ErrorReport.causes(throwable), Seq.empty)
      case _ =>
        ErrorReport(SOURCE, ErrorReport.message(throwable), None, ErrorReport.causes(throwable), throwable.getStackTrace)
    }
  }
}
