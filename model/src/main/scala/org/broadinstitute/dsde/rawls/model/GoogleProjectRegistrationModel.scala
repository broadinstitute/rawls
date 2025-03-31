package org.broadinstitute.dsde.rawls.model

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

object GoogleProjectRegistrationModel {
  def newGoogleProjectRegistration(googleProjectId: GoogleProjectId,
                                   billingAccount: Option[RawlsBillingAccountName],
                                   message: Option[String],
                                   billingProjectId: String
  ) = GoogleProjectRegistration(googleProjectId, billingAccount, message, RawlsBillingProjectName(billingProjectId))

}

case class GoogleProjectRegistration(googleProjectId: GoogleProjectId,
                                     billingAccount: Option[RawlsBillingAccountName],
                                     message: Option[String],
                                     billingProjectId: RawlsBillingProjectName
)

trait GoogleProjectRegistrationJsonSupport$ extends DefaultJsonProtocol {

  implicit val GoogleProjectRegistrationFormat: RootJsonFormat[GoogleProjectRegistration] = jsonFormat4(
    GoogleProjectRegistration
  )

}

object GoogleProjectRegistrationJsonSupport$ extends GoogleProjectRegistrationJsonSupport$
