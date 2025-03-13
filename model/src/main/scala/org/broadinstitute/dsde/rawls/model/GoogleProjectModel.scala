package org.broadinstitute.dsde.rawls.model

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

object GoogleProjectModel {
  def newGoogleProject(googleProjectId: GoogleProjectId,
                       billingAccount: Option[RawlsBillingAccountName],
                       message: Option[String],
                       billingProjectId: String
  ) = RawlsGoogleProject(googleProjectId, billingAccount, message, RawlsBillingProjectName(billingProjectId))

}

case class RawlsGoogleProject(googleProjectId: GoogleProjectId,
                              billingAccount: Option[RawlsBillingAccountName],
                              message: Option[String],
                              billingProjectId: RawlsBillingProjectName
)

trait GoogleProjectJsonSupport extends DefaultJsonProtocol {

  implicit val RawlsGoogleProjectFormat: RootJsonFormat[RawlsGoogleProject] = jsonFormat4(
    RawlsGoogleProject
  )

}

object GoogleProjectJsonSupport extends GoogleProjectJsonSupport
