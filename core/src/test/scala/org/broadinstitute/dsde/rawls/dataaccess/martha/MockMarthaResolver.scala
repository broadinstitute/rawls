package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}

class MockMarthaResolver(marthaUrl: String)
                        (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaResolver(marthaUrl) {

  val dosServiceAccount = "serviceaccount@foo.com"
  val differentDosServiceAccount = "differentserviceaccount@foo.com"

  override def dosServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] = {
    Future.successful {
      drsUrl match {
        case u if u.contains("different") => Some(differentDosServiceAccount)
        case u if u.contains("jade.datarepo") => None
        case _ => Some(dosServiceAccount)
      }
    }
  }
}
