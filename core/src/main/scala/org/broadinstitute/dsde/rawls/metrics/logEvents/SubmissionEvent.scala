package org.broadinstitute.dsde.rawls.metrics.logEvents

import java.util

case class SubmissionEvent(submissionId: String,
                           workspaceId: String,
                           methodId: String,
                           methodNamespace: String,
                           methodName: String,
                           methodUri: String,
                           methodRepository: String,
                           methodConfigVersion: Int,
                           methodDataReferenceName: Option[String],
                           rawlsWorkflowIds: Seq[Long],
                           externalIds: Seq[String],
                           useCallCaching: Boolean,
                           useReferenceDisks: Boolean,
                           memoryRetryMultiplier: Double,
                           userComment: Option[String]
) extends MetricsLoggable {

  override def event: String = "rawls:submission"

  override protected def toLoggableMap: util.Map[String, Any] =
    this.transformMap(
      Map(
        "submissionId" -> submissionId,
        "workspaceId" -> workspaceId,
        "methodId" -> methodId,
        "methodNamespace" -> methodNamespace,
        "methodName" -> methodName,
        "methodUri" -> methodUri,
        "methodRepository" -> methodRepository,
        "methodConfigVersion" -> methodConfigVersion,
        "methodDataReferenceName" -> methodDataReferenceName,
        "rawlsWorkflowIds" -> rawlsWorkflowIds,
        "externalIds" -> externalIds,
        "useCallCaching" -> useCallCaching,
        "useReferenceDisks" -> useReferenceDisks,
        "memoryRetryMultiplier" -> memoryRetryMultiplier,
        "userComment" -> userComment
      )
    )
}
