package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem
import spray.json.RootJsonFormat

/**
  * Created by rtitle on 5/24/17.
  */
case class SubsystemStatus(
  ok: Boolean,
  messages: Option[List[String]]
)

case class StatusCheckResponse(
  ok: Boolean,
  systems: Map[Subsystem, SubsystemStatus]
)

object Subsystems {
  val AllSubsystems =
    Set(Agora,
        Cromwell,
        Database,
        GoogleBilling,
        GoogleBuckets,
        GoogleGenomics,
        GoogleGroups,
        GooglePubSub,
        Sam,
        BillingProfileManager,
        WorkspaceManager
    )
  // CriticalSubsystems are those that will trigger rawls to report down
  val CriticalSubsystems = Set(Database, GoogleGroups, Sam)
  val GoogleSubsystems = Set(GoogleBilling, GoogleBuckets, GoogleGenomics, GoogleGroups, GooglePubSub)

  sealed trait Subsystem extends RawlsEnumeration[Subsystem] {
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = Subsystems.withName(name)
    def isGoogle = GoogleSubsystems.contains(this)
  }

  def withName(name: String): Subsystem =
    name match {
      case "Agora"                 => Agora
      case "Cromwell"              => Cromwell
      case "Database"              => Database
      case "GoogleBilling"         => GoogleBilling
      case "GoogleBuckets"         => GoogleBuckets
      case "GoogleGenomics"        => GoogleGenomics
      case "GoogleGroups"          => GoogleGroups
      case "GooglePubSub"          => GooglePubSub
      case "Mongo"                 => Mongo
      case "Sam"                   => Sam
      case "BillingProfileManager" => BillingProfileManager
      case "WorkspaceManager"      => WorkspaceManager
      case _                       => throw new RawlsException(s"invalid Subsystem [$name]")
    }

  case object Agora extends Subsystem
  case object Cromwell extends Subsystem
  case object Database extends Subsystem
  case object GoogleBilling extends Subsystem
  case object GoogleBuckets extends Subsystem
  case object GoogleGenomics extends Subsystem
  case object GoogleGroups extends Subsystem
  case object GooglePubSub extends Subsystem
  case object Mongo extends Subsystem
  case object Sam extends Subsystem
  case object BillingProfileManager extends Subsystem
  case object WorkspaceManager extends Subsystem
}

object StatusJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val SubsystemFormat: RootJsonFormat[Subsystem] = rawlsEnumerationFormat(Subsystems.withName)

  implicit val SubsystemStatusFormat: RootJsonFormat[SubsystemStatus] = jsonFormat2(SubsystemStatus)

  implicit val StatusCheckResponseFormat: RootJsonFormat[StatusCheckResponse] = jsonFormat2(StatusCheckResponse)
}
