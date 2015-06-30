package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.ExecutionServiceStatus
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try
import spray.client.pipelining._
import spray.http.{FormData,HttpCookie}
import spray.http.HttpHeaders.Cookie
import spray.httpx.SprayJsonSupport._

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String )( implicit system: ActorSystem ) extends ExecutionServiceDAO {

  override def submitWorkflow( wdl: String, inputs: String, authCookie: HttpCookie ): ExecutionServiceStatus = {
    val url = executionServiceURL+"/workflows"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    val formData = FormData(Seq("wdl" -> wdl, "inputs" -> inputs))
    Await.result(pipeline(Post(url,formData)),Duration.Inf)
  }
}