package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class DisabledMarthaResolver(marthaUrl: String)(implicit
                                        val system: ActorSystem,
                                        val materializer: Materializer,
                                        val executionContext: ExecutionContext
) extends DrsResolver
  with DsdeHttpDAO
  with Retry {

  // the list of fields we want in Martha response. More info can be found here: https://github.com/broadinstitute/martha#martha-v3
  private val MarthaRequestFieldsKey: Array[String] = Array("googleServiceAccount")

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] =
    throw new NotImplementedError("resolveDrsThroughMartha is not implemented for Azure.")

  override def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] =
    throw new NotImplementedError("drsServiceAccountEmail is not implemented for Azure.")
}

