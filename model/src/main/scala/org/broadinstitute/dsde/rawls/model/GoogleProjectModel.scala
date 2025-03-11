package org.broadinstitute.dsde.rawls.model

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._

object GoogleProjectModel {
  def newGoogleProject(googleProjectId: String,
                       billingAccount: Option[String],
                       message: Option[String],
                       billingProjectId: String
  ) = RawlsGoogleProject(googleProjectId, billingAccount, message, RawlsBillingProjectName(billingProjectId))

}

//TODO should any other fields be objects instead of strings?
case class RawlsGoogleProject(googleProjectId: String,
                              billingAccount: Option[String],
                              message: Option[String],
                              billingProjectId: RawlsBillingProjectName
)

trait GoogleProjectJsonSupport extends DefaultJsonProtocol {

  implicit val RawlsGoogleProjectFormat: RootJsonFormat[RawlsGoogleProject] = jsonFormat4(
    RawlsGoogleProject
  )

}

object GoogleProjectJsonSupport extends GoogleProjectJsonSupport
