package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.SubmissionRetryStatuses.RetryStatus
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowCostTypes.WorkflowCostType
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.workbench.model.ValueObjectFormat
import spray.json.{DefaultJsonProtocol, DeserializationException, JsNumber, JsObject, JsValue, RootJsonFormat}

import scala.util.{Failure, Success, Try}

object GoogleProjectModel {
  def newGoogleProject(googleProjectId: String,
                       billingAccount: Option[String],
                       message: Option[String],
                       billingProjectId: String
  ) = RawlsGoogleProject(googleProjectId, billingAccount, message, billingProjectId)

}

//TODO should billingAccount and project be strings or BillingAccountName etc?
case class RawlsGoogleProject(googleProjectId: String,
                              billingAccount: Option[String],
                              message: Option[String],
                              billingProjectId: String
)

trait GoogleProjectJsonSupport extends DefaultJsonProtocol {
//  import spray.json.DefaultJsonProtocol._

//  implicit override val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  implicit val RawlsGoogleProjectFormat: RootJsonFormat[RawlsGoogleProject] = jsonFormat4(
    RawlsGoogleProject
  )

}

object GoogleProjectJsonSupport extends GoogleProjectJsonSupport
