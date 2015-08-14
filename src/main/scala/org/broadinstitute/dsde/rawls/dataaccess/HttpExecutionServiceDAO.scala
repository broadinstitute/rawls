package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.{ExecutionServiceLogs, ExecutionServiceOutputs, ExecutionServiceStatus}
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
    // TODO: how to get the version?
    val url = executionServiceURL+"/workflows/v1"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs))
    Await.result(pipeline(Post(url,formData)),Duration.Inf)
  }

  override def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Get(url)),Duration.Inf)
  }

  override def outputs(id: String, authCookie: HttpCookie): ExecutionServiceOutputs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceOutputs]
    Await.result(pipeline(Get(url)), Duration.Inf)
  }

  override def logs(id: String, authCookie: HttpCookie): ExecutionServiceLogs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceLogs]
    Await.result(pipeline(Get(url)), Duration.Inf)
  }

  override def abort(id: String, authCookie: HttpCookie): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Post(url)),Duration.Inf)
  }
}