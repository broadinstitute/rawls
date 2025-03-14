package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  GoogleProjectRegistration,
  RawlsBillingAccountName,
  RawlsBillingProjectName
}

final case class GoogleProjectRegistrationRecord(googleProjectId: String,
                                                 billingAccount: Option[String],
                                                 message: Option[String],
                                                 billingProjectId: String
)

object GoogleProjectRegistrationRecord {
  def fromGoogleProjectRegistration(googleProjectReg: GoogleProjectRegistration): GoogleProjectRegistrationRecord =
    GoogleProjectRegistrationRecord(
      googleProjectReg.googleProjectId.value,
      googleProjectReg.billingAccount.map(_.value),
      googleProjectReg.message,
      googleProjectReg.billingProjectId.value
    )

  def toGoogleProjectRegistration(googleProjectRegRec: GoogleProjectRegistrationRecord): GoogleProjectRegistration =
    GoogleProjectRegistration(
      GoogleProjectId(googleProjectRegRec.googleProjectId),
      googleProjectRegRec.billingAccount.map(RawlsBillingAccountName(_)),
      googleProjectRegRec.message,
      RawlsBillingProjectName(googleProjectRegRec.billingProjectId)
    )

}

trait GoogleProjectRegistrationComponent {
  this: DriverComponent =>

  import driver.api._

  class GoogleProjectRegistrationTable(tag: Tag) extends Table[GoogleProjectRegistrationRecord](tag, "GOOGLE_PROJECT") {
    def googleProjectId = column[String]("GOOGLE_PROJECT_ID", O.PrimaryKey, O.Length(254))

    def billingAccount = column[Option[String]]("BILLING_ACCOUNT")

    def message = column[Option[String]]("MESSAGE")

    def billingProject = column[String]("BILLING_PROJECT")

    def * = (googleProjectId, billingAccount, message, billingProject) <> (
      (GoogleProjectRegistrationRecord.apply _).tupled,
      GoogleProjectRegistrationRecord.unapply
    )
  }

  type GoogleProjectRegistrationQuery = Query[GoogleProjectRegistrationTable, GoogleProjectRegistrationRecord, Seq]

  object googleProjectRegistrationQuery extends TableQuery(new GoogleProjectRegistrationTable(_)) {

    def create(googleProjectReg: GoogleProjectRegistration): ReadWriteAction[GoogleProjectRegistration] =
      googleProjectRegistrationQuery
        .withId(googleProjectReg.googleProjectId.value)
        .result
        .flatMap {
          case Seq() =>
            googleProjectRegistrationQuery += GoogleProjectRegistrationRecord.fromGoogleProjectRegistration(
              googleProjectReg
            )
          case _ =>
            updateBillingProjectIdAndAccount(googleProjectReg)
        }
        .map(_ => googleProjectReg)

    def updateBillingProjectIdAndAccount(googleProjectReg: GoogleProjectRegistration): WriteAction[Int] =
      googleProjectRegistrationQuery
        .withId(googleProjectReg.googleProjectId.value)
        .map(reg => (reg.billingProject, reg.billingAccount))
        .update((googleProjectReg.billingProjectId.value, googleProjectReg.billingAccount.map(_.value)))

    def withId(projectId: String): GoogleProjectRegistrationQuery =
      filter(_.googleProjectId === projectId)

    def findById(id: GoogleProjectId): ReadAction[Option[GoogleProjectRegistration]] =
      uniqueResult[GoogleProjectRegistrationRecord](withId(id.value)) flatMap {
        case None => DBIO.successful(None)
        case Some(googleProjectRegRec) =>
          DBIO.successful(Option(GoogleProjectRegistrationRecord.toGoogleProjectRegistration(googleProjectRegRec)))
      }

  }
}
