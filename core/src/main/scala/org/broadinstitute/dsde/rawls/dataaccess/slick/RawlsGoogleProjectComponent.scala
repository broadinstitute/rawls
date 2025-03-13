package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsBillingAccountName,
  RawlsBillingProjectName,
  RawlsGoogleProject
}

final case class RawlsGoogleProjectRecord(googleProjectId: String,
                                          billingAccount: Option[String],
                                          message: Option[String],
                                          billingProjectId: String
)

object RawlsGoogleProjectRecord {
  def fromGoogleProject(googleProject: RawlsGoogleProject): RawlsGoogleProjectRecord =
    RawlsGoogleProjectRecord(
      googleProject.googleProjectId.value,
      googleProject.billingAccount.map(_.value),
      googleProject.message,
      googleProject.billingProjectId.value
    )

  def toGoogleProject(projectRecord: RawlsGoogleProjectRecord): RawlsGoogleProject =
    RawlsGoogleProject(
      GoogleProjectId(projectRecord.googleProjectId),
      projectRecord.billingAccount.map(RawlsBillingAccountName(_)),
      projectRecord.message,
      RawlsBillingProjectName(projectRecord.billingProjectId)
    )

}

trait RawlsGoogleProjectComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsGoogleProjectTable(tag: Tag) extends Table[RawlsGoogleProjectRecord](tag, "GOOGLE_PROJECT") {
    def googleProjectId = column[String]("GOOGLE_PROJECT_ID", O.PrimaryKey, O.Length(254))

    def billingAccount = column[Option[String]]("BILLING_ACCOUNT")

    def message = column[Option[String]]("MESSAGE")

    def billingProject = column[String]("BILLING_PROJECT")

    def * = (googleProjectId, billingAccount, message, billingProject) <> ((RawlsGoogleProjectRecord.apply _).tupled,
                                                                           RawlsGoogleProjectRecord.unapply
    )
  }

  type RawlsGoogleProjectQuery = Query[RawlsGoogleProjectTable, RawlsGoogleProjectRecord, Seq]

  object rawlsGoogleProjectQuery extends TableQuery(new RawlsGoogleProjectTable(_)) {

    def create(googleProject: RawlsGoogleProject): ReadWriteAction[RawlsGoogleProject] =
      rawlsGoogleProjectQuery.result
        .flatMap {
          case Seq() => rawlsGoogleProjectQuery += RawlsGoogleProjectRecord.fromGoogleProject(googleProject)
          case _ =>
            throw new RawlsException(
              s"Cannot create billing project [${googleProject.googleProjectId.value}] in database because it already exists."
            )
        }
        .map(_ => googleProject)

  }
}
