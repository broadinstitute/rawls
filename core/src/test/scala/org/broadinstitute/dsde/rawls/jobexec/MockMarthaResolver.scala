package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.martha.{MarthaMinimalResponse, MarthaResolver, ServiceAccountEmail, ServiceAccountPayload}
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}

class MockMarthaResolver(marthaUrl: String)
                        (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaResolver(marthaUrl) {

  val dosServiceAccount = "serviceaccount@foo.com"
  val dosSAEmail: ServiceAccountEmail = ServiceAccountEmail(dosServiceAccount)
  val dosSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(dosSAEmail))
  val dosSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(dosSAPayload))

  val differentDosServiceAccount = "differentserviceaccount@foo.com"
  val differentDosSAEmail: ServiceAccountEmail = ServiceAccountEmail(differentDosServiceAccount)
  val differentDosSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(differentDosSAEmail))
  val differentDosSAMinimalResponse: MarthaMinimalResponse = MarthaMinimalResponse(Option(differentDosSAPayload))

  override def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] = {
    Future.successful{
      drsUrl match {
        case u if u.contains("different") => differentDosSAMinimalResponse
        case u if u.contains("jade.datarepo") => MarthaMinimalResponse(None)
        case _ => dosSAMinimalResponse
      }
    }
  }
}
