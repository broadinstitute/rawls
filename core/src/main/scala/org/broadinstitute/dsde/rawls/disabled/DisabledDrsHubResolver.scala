package org.broadinstitute.dsde.rawls.disabled

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DrsHubMinimalResponse, DrsResolver}
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class DisabledDrsHubResolver(drsHubUrl: String)(implicit
                                        val system: ActorSystem,
                                        val materializer: Materializer,
                                        val executionContext: ExecutionContext
) extends DrsResolver
  with DsdeHttpDAO
  with Retry {

  // the list of fields we want in DrsHub response. More info can be found here: https://github.com/broadinstitute/drsHub#drsHub-v3
  private val DrsHubRequestFieldsKey: Array[String] = Array("googleServiceAccount")

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  private def resolveDrs(drsUrl: String, userInfo: UserInfo): Future[DrsHubMinimalResponse] =
    throw new NotImplementedError("resolveDrs is not implemented for Azure.")

  override def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] =
    throw new NotImplementedError("drsServiceAccountEmail is not implemented for Azure.")
}

