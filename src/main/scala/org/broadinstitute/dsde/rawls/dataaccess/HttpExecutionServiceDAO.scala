package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import spray.client.pipelining
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try
import spray.client.pipelining._
import spray.http.{FormData,HttpCookie}
import spray.http.HttpHeaders.{Authorization, Cookie}
import spray.httpx.SprayJsonSupport._

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String )( implicit system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO {

  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): ExecutionServiceStatus = {
    // TODO: how to get the version?
    val url = executionServiceURL+"/workflows/v1"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs) ++ options.map("workflowOptions" -> _).toSeq)
    Await.result(pipeline(Post(url,formData)),Duration.Inf)
  }

  override def validateWorkflow(wdl: String, inputs: String, userInfo: UserInfo): ExecutionServiceValidation = {
    val url = executionServiceURL + "/workflows/v1/validate"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceValidation]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs))
    Await.result(pipeline(Post(url,formData)),Duration.Inf)
  }

  override def status(id: String, userInfo: UserInfo): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Get(url)),Duration.Inf)
  }

  override def outputs(id: String, userInfo: UserInfo): ExecutionServiceOutputs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceOutputs]
    Await.result(pipeline(Get(url)), Duration.Inf)
  }

  override def logs(id: String, userInfo: UserInfo): ExecutionServiceLogs = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceLogs]
    Await.result(pipeline(Get(url)), Duration.Inf)
  }

  override def abort(id: String, userInfo: UserInfo): ExecutionServiceStatus = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    Await.result(pipeline(Post(url)),Duration.Inf)
  }
}