package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.util.FutureSupport
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import spray.client.pipelining._
import spray.http.FormData
import spray.httpx.SprayJsonSupport._

import scala.util.Try

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String, workflowSubmissionTimeout: FiniteDuration )( implicit val system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO with Retry with FutureSupport {

  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    implicit val timeout = Timeout(workflowSubmissionTimeout)
    // TODO: how to get the version?
    val url = executionServiceURL+"/workflows/v1"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    val formData = FormData(Seq("wdlSource" -> wdl, "workflowInputs" -> inputs) ++ options.map("workflowOptions" -> _).toSeq)
    pipeline(Post(url,formData))
  }

  override def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def callLevelMetadata(id: String, userInfo: UserInfo): Future[ExecutionMetadata] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/metadata"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionMetadata]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceOutputs]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceLogs]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(when500) { () => toFutureTry(pipeline(Post(url))) }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}