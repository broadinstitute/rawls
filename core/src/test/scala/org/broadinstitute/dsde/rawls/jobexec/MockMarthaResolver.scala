package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.martha.{MarthaMinimalResponse, MarthaResolver, ServiceAccountEmail, ServiceAccountPayload}
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}

class MockMarthaResolver(marthaUrl: String)
                        (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaResolver(marthaUrl) {

  val drsServiceAccount = "serviceaccount@foo.com"
  val drsSAEmail: ServiceAccountEmail = ServiceAccountEmail(drsServiceAccount)
  val drsSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(drsSAEmail))
  val drsSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(drsSAPayload))

  val differentDrsServiceAccount = "differentserviceaccount@foo.com"
  val differentDrsSAEmail: ServiceAccountEmail = ServiceAccountEmail(differentDrsServiceAccount)
  val differentDrsSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(differentDrsSAEmail))
  val differentDrsSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(differentDrsSAPayload))

  override def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] = {
    Future.successful{
      drsUrl match {
        case u if u.contains("different") => differentDrsSAMinimalResponse
        case u if u.contains("jade.datarepo") => MarthaMinimalResponse(None)
        case _ => drsSAMinimalResponse
      }
    }
  }
}
