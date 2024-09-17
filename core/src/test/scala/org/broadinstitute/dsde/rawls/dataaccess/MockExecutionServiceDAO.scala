package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import spray.json.JsObject

import scala.concurrent.Future
import scala.util.Success

class MockExecutionServiceDAO(timeout: Boolean = false, val identifier: String = "", failSubmission: Boolean = false)
    extends ExecutionServiceDAO {
  var submitWdlSource: String = null
  var submitInput: Seq[String] = null
  var submitOptions: Option[String] = None
  var labels: Map[String, String] = Map.empty // could make this more sophisticated: map of workflow to map[s,s]
  var collection: Option[String] = None

  override def submitWorkflows(wdl: WDL,
                               inputs: Seq[String],
                               options: Option[String],
                               labels: Option[Map[String, String]],
                               workflowCollection: Option[String],
                               userInfo: UserInfo
  ) = {
    this.submitInput = inputs
    this.submitWdlSource = wdl.asInstanceOf[WdlSource].source
    this.submitOptions = options
    this.labels = labels.getOrElse(Map.empty)
    this.collection = workflowCollection

    val inputPattern = """\{"three_step.cgrep.pattern":"(sample[0-9])"\}""".r

    val workflowIds = inputs.map {
      case inputPattern(sampleName) => sampleName
      case _                        => "69d1d92f-3895-4a7b-880a-82535e9a096e"
    }
    if (failSubmission) {
      Future.successful(workflowIds.map(_ => Right(ExecutionServiceFailure("Fail", "Fail", Option.empty))))
    } else if (timeout) {
      Future.failed(
        new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.GatewayTimeout, s"Failed to submit"))
      )
    } else {
      Future.successful(workflowIds.map(id => Left(ExecutionServiceStatus(id, "Submitted"))))
    }
  }

  override def logs(id: String, userInfo: UserInfo) = Future.successful(
    ExecutionServiceLogs(
      id,
      Option(
        Map(
          "x" -> Seq(
            ExecutionServiceCallLogs(
              stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
              stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt"
            )
          )
        )
      )
    )
  )

  override def outputs(id: String, userInfo: UserInfo) =
    Future.successful(ExecutionServiceOutputs(id, Map("foo" -> Left(AttributeString("bar")))))

  override def abort(id: String, userInfo: UserInfo) = Future.successful(Success(ExecutionServiceStatus(id, "Aborted")))

  override def status(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceStatus(id, "Submitted"))

  override def callLevelMetadata(id: String, metadataParams: MetadataParams, userInfo: UserInfo): Future[JsObject] =
    Future.successful(null)

  override def getLabels(id: String, userInfo: UserInfo): Future[ExecutionServiceLabelResponse] =
    Future.successful(ExecutionServiceLabelResponse(id, labels))

  override def patchLabels(id: String,
                           userInfo: UserInfo,
                           newLabels: Map[String, String]
  ): Future[ExecutionServiceLabelResponse] = {
    labels ++= newLabels
    Future.successful(ExecutionServiceLabelResponse(id, labels))
  }

  override def getCost(id: String, userInfo: UserInfo): Future[WorkflowCostBreakdown] = ???

  override def version() = Future.successful(ExecutionServiceVersion("25"))

  override def getStatus() = {
    // these differ from Rawls model Subsystems
    val execSubsystems = List("DockerHub", "Engine Database", "PAPI", "GCS")
    val systemsMap: Map[String, SubsystemStatus] = (execSubsystems map { _ -> HealthMonitor.OkStatus }).toMap
    Future.successful(systemsMap)
  }
}
