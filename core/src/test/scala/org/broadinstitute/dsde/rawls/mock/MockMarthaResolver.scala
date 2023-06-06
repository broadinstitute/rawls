package org.broadinstitute.dsde.rawls.mock

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.drs.{
  MarthaMinimalResponse,
  MarthaResolver,
  ServiceAccountEmail,
  ServiceAccountPayload
}
import org.broadinstitute.dsde.rawls.mock.MockMarthaResolver._
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}

class MockMarthaResolver(marthaUrl: String)(implicit
  system: ActorSystem,
  materializer: Materializer,
  executionContext: ExecutionContext
) extends MarthaResolver(marthaUrl) {

  override def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] =
    Future.successful {
      drsUrl match {
        case u if u.contains("different")     => differentDrsSAMinimalResponse
        case u if u.contains("jade.datarepo") => MarthaMinimalResponse(None)
        case MockMarthaResolver.dgUrl         => mrBeanSAMinimalResponse
        case _                                => drsSAMinimalResponse
      }
    }
}

object MockMarthaResolver {
  val jdrDevUrl = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4"
  val dgUrl = "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0"

  val mrBeanEmail: ServiceAccountEmail = ServiceAccountEmail("mr_bean@gmail.com")
  val mrBeanSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(mrBeanEmail))
  val mrBeanSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(mrBeanSAPayload))

  val drsServiceAccount = "serviceaccount@foo.com"
  val drsSAEmail: ServiceAccountEmail = ServiceAccountEmail(drsServiceAccount)
  val drsSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(drsSAEmail))
  val drsSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(drsSAPayload))

  val differentDrsServiceAccount = "differentserviceaccount@foo.com"
  val differentDrsSAEmail: ServiceAccountEmail = ServiceAccountEmail(differentDrsServiceAccount)
  val differentDrsSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(differentDrsSAEmail))
  val differentDrsSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(differentDrsSAPayload))
}
