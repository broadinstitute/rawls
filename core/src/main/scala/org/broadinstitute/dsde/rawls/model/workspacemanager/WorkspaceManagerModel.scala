package org.broadinstitute.dsde.rawls.model.workspacemanager

import org.broadinstitute.dsde.rawls.model.JsonSupport
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

case class WMCreateWorkspaceRequest(id: String, authToken: String, spendProfile: Option[String], policies: Option[List[String]])
case class WMCreateDataReferenceRequest(name: String, resourceId: Option[String], referenceType: Option[String], reference: Option[JsObject], cloningInstructions: String, credentialId: Option[String])
case class WMCreateWorkspaceResponse(id: String)
case class WMGetWorkspaceResponse(id: String)
case class WMResourceDescription(resource_id: String, workspace_id: String, application_id: String, is_visible: String, owner: String, attributes: String)
case class WMDataReferenceResponse(referenceId: String, name: String, resourceDescription: Option[WMResourceDescription], referenceType: Option[String], reference: Option[JsObject], credentialId: Option[String], cloningInstructions: String)

case class DataRepoSnapshot(name: String, snapshotId: String)

object WorkspaceManagerJsonSupport extends JsonSupport {

  implicit val WMCreateWorkspaceRequestFormat = jsonFormat4(WMCreateWorkspaceRequest)
  implicit val WMCreateWorkspaceResponseFormat = jsonFormat1(WMCreateWorkspaceResponse)

  implicit val WMGetWorkspaceResponseFormat = jsonFormat1(WMGetWorkspaceResponse)

  implicit val WMCreateDataReferenceRequestFormat = jsonFormat6(WMCreateDataReferenceRequest)
  implicit val WMResourceDescriptionFormat = jsonFormat6(WMResourceDescription)
  implicit val WMDataReferenceResponseFormat = jsonFormat7(WMDataReferenceResponse)

  implicit val DataRepoSnapshotFormat = jsonFormat2(DataRepoSnapshot)

}

object DataReferenceType extends Enumeration {
  val DataRepoSnapshot = Value
}

object CloningInstructions extends Enumeration {
  val COPY_NOTHING, COPY_DEFINITION, COPY_RESOURCE, COPY_REFERENCE = Value
}
