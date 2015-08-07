package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.{ExecutionServiceOutputs, ExecutionServiceStatus}
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

  override def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflow/${id}/status"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Get(url)),Duration.Inf)
  }

  override def outputs(id: String, authCookie: HttpCookie): ExecutionServiceOutputs = {
    val url = executionServiceURL + s"/workflow/${id}/outputs"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceOutputs]
    Await.result(pipeline(Get(url)), Duration.Inf)
  }

  override def abort(id: String, authCookie: HttpCookie): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflow/${id}"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Delete(url)),Duration.Inf)
  }
}