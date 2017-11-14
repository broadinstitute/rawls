package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{ErrorReportSource, ErrorReportable, JsonSupport, SubsystemStatus, UserInfo, UserStatus}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("sam")
  def registerUser(userInfo: UserInfo): Future[Option[UserStatus]]
  def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean]
  def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Boolean]

  def getStatus(): Future[SubsystemStatus]
}

object SamResourceActions {
  case class SamResourceAction(value: String)

  val createWorkspace = SamResourceAction("create_workspace")
}

object SamResourceTypeNames {
  case class SamResourceTypeName(value: String)

  val billingProject = SamResourceTypeName("billing-project")
}

case class SamPolicy(memberEmails: Seq[String], actions: Seq[String], roles: Seq[String])

class SamModelJsonSupport extends JsonSupport {
  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
}
