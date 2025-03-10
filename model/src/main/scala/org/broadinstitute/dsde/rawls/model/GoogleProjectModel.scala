package org.broadinstitute.dsde.rawls.model

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
