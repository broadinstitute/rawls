package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import spray.client.pipelining._
import spray.http.FormData
import spray.httpx.SprayJsonSupport._

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String )( implicit system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO with Retry {

  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): ExecutionServiceStatus = {
    // TODO: how to get the version?
    val url = executionServiceURL+"/workflows/v1"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs) ++ options.map("workflowOptions" -> _).toSeq)
    retry(Await.result(pipeline(Post(url,formData)),Duration.Inf),when500)
  }

  override def validateWorkflow(wdl: String, inputs: String, userInfo: UserInfo): ExecutionServiceValidation = {
    val url = executionServiceURL + "/workflows/v1/validate"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceValidation]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs))
    retry(Await.result(pipeline(Post(url,formData)),Duration.Inf),when500)
  }

  override def status(id: String, userInfo: UserInfo): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(Await.result(pipeline(Get(url)),Duration.Inf),when500)
  }

  override def callLevelMetadata(id: String, userInfo: UserInfo): ExecutionMetadata = {
    val url = executionServiceURL + s"/workflows/v1/${id}/metadata"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionMetadata]
    retry(Await.result(pipeline(Get(url)),Duration.Inf),when500)
  }

  override def outputs(id: String, userInfo: UserInfo): ExecutionServiceOutputs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceOutputs]
    retry(Await.result(pipeline(Get(url)), Duration.Inf),when500)
  }

  override def logs(id: String, userInfo: UserInfo): ExecutionServiceLogs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceLogs]
    retry(Await.result(pipeline(Get(url)), Duration.Inf),when500)
  }

  override def abort(id: String, userInfo: UserInfo): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(Await.result(pipeline(Post(url)),Duration.Inf),when500)
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}