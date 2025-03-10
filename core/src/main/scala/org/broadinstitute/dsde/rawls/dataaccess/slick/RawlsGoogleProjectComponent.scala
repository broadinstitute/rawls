package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.RawlsGoogleProject

final case class RawlsGoogleProjectRecord(googleProjectId: String,
                                          billingAccount: Option[String],
                                          message: Option[String],
                                          billingProjectId: String
)

object RawlsGoogleProjectRecord {
  def fromGoogleProject(googleProject: RawlsGoogleProject): RawlsGoogleProjectRecord =
    RawlsGoogleProjectRecord(
      googleProject.googleProjectId,
      googleProject.billingAccount,
      googleProject.message,
      googleProject.billingProjectId
    )

  def toGoogleProject(projectRecord: RawlsGoogleProjectRecord): RawlsGoogleProject =
    RawlsGoogleProject(
      projectRecord.googleProjectId,
      projectRecord.billingAccount,
      projectRecord.message,
      projectRecord.billingProjectId
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
      // TODO validation
//      validateUserDefinedString(billingProject.projectName.value)
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
