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

object UnRegisteredGoogleProjectRegistration {
  def apply(googleProjectId: GoogleProjectId): GoogleProjectRegistration =
    GoogleProjectRegistration(
      googleProjectId = googleProjectId,
      billingAccount = None,
      message = None,
      billingProjectId = RawlsBillingProjectName("UNREGISTERED")
    )

  def unapply(registration: GoogleProjectRegistration): Option[GoogleProjectId] =
    if (
      registration.billingAccount.isEmpty && registration.message.isEmpty && registration.billingProjectId.value == "UNREGISTERED"
    )
      Some(registration.googleProjectId)
    else
      None
}

trait GoogleProjectRegistrationJsonSupport$ extends DefaultJsonProtocol {

  implicit val GoogleProjectRegistrationFormat: RootJsonFormat[GoogleProjectRegistration] = jsonFormat4(
    GoogleProjectRegistration
  )

}

object GoogleProjectRegistrationJsonSupport$ extends GoogleProjectRegistrationJsonSupport$
