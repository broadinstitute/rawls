package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.vault.common.openam.OpenAMResponse.UsernameCNResponse
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.{Future, ExecutionContext}

trait MockOpenAmDirectives extends OpenAmDirectives {

  // TODO make the response match whatever MockGoogleCloudStorageDAO wants

  def usernameFromCookie(magnet: ImplicitMagnet[ExecutionContext]): Directive1[String] = {
    implicit val ec = magnet.value
    val userNameFuture = for {
      usernameCN <- Future(new UsernameCNResponse("rawls-test", Seq("rawls-test")))
    } yield usernameCN.username
    onSuccess(userNameFuture)
  }
}
