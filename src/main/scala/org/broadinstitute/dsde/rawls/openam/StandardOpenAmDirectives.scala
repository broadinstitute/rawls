package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.ExecutionContext

trait StandardOpenAmDirectives extends OpenAmDirectives {
  val rawlsOpenAmClient: RawlsOpenAmClient

  def usernameFromCookie(magnet: ImplicitMagnet[ExecutionContext]): Directive1[String] = {
    implicit val ec = magnet.value
    tokenFromCookie flatMap usernameFromToken
  }

  private def tokenFromCookie: Directive1[String] = cookie("iPlanetDirectoryPro") map { _.content }

  private def usernameFromToken(token: String)(implicit ec: ExecutionContext): Directive1[String] = {
    val userInfoFuture = for {
      id <- rawlsOpenAmClient.lookupIdFromSession(token)
      userInfo <- rawlsOpenAmClient.lookupUserInfo(token, id.id, id.realm)
    } yield userInfo.mail.head
    onSuccess(userInfoFuture)
  }
}
