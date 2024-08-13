package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.GoogleApiTypes.GoogleApiType
import org.broadinstitute.dsde.rawls.dataaccess.GoogleOperationNames.GoogleOperationName
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleApiTypes, GoogleOperationNames}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.InsertExtensionMethod
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import slick.ast.BaseTypedType
import slick.jdbc.JdbcType

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

final case class RawlsBillingProjectRecord(projectName: String,
                                           creationStatus: String,
                                           billingAccount: Option[String],
                                           message: Option[String],
                                           cromwellBackend: Option[String],
                                           servicePerimeter: Option[String],
                                           googleProjectNumber: Option[String],
                                           invalidBillingAccount: Boolean,
                                           spendReportDataset: Option[String],
                                           spendReportTable: Option[String],
                                           spendReportDatasetGoogleProject: Option[String],
                                           billingProfileId: Option[UUID],
                                           landingZoneId: Option[UUID]
)

object RawlsBillingProjectRecord {
  def fromBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord =
    RawlsBillingProjectRecord(
      billingProject.projectName.value,
      billingProject.status.toString,
      billingProject.billingAccount.map(_.value),
      billingProject.message,
      billingProject.cromwellBackend.map(_.value),
      billingProject.servicePerimeter.map(_.value),
      billingProject.googleProjectNumber.map(_.value),
      billingProject.invalidBillingAccount,
      billingProject.spendReportDataset.map(_.value),
      billingProject.spendReportTable.map(_.value),
      billingProject.spendReportDatasetGoogleProject.map(_.value),
      billingProject.billingProfileId.map(UUID.fromString),
      billingProject.landingZoneId.map(UUID.fromString)
    )

  def toBillingProject(projectRecord: RawlsBillingProjectRecord): RawlsBillingProject =
    RawlsBillingProject(
      RawlsBillingProjectName(projectRecord.projectName),
      CreationStatuses.withName(projectRecord.creationStatus),
      projectRecord.billingAccount.map(RawlsBillingAccountName),
      projectRecord.message,
      projectRecord.cromwellBackend.map(CromwellBackend),
      projectRecord.servicePerimeter.map(ServicePerimeterName),
      projectRecord.googleProjectNumber.map(GoogleProjectNumber),
      projectRecord.invalidBillingAccount,
      projectRecord.spendReportDataset.map(BigQueryDatasetName),
      projectRecord.spendReportTable.map(BigQueryTableName),
      projectRecord.spendReportDatasetGoogleProject.map(GoogleProject),
      billingProfileId = projectRecord.billingProfileId.map(_.toString),
      landingZoneId = projectRecord.landingZoneId.map(_.toString)
    )

  def toBillingProjectSpendExport(projectRecord: RawlsBillingProjectRecord): BillingProjectSpendExport = {
    val table = for {
      googleProjectId <- projectRecord.spendReportDatasetGoogleProject
      datasetName <- projectRecord.spendReportDataset
      tableName <- projectRecord.spendReportTable
    } yield s"$googleProjectId.$datasetName.$tableName"

    val billingAccount = RawlsBillingAccountName(
      projectRecord.billingAccount.getOrElse(
        throw new RawlsException(s"billing account not set on project ${projectRecord.projectName}")
      )
    )

    BillingProjectSpendExport(RawlsBillingProjectName(projectRecord.projectName), billingAccount, table)
  }

}

case class RawlsBillingProjectOperationRecord(projectName: String,
                                              operationName: GoogleOperationName,
                                              operationId: String,
                                              done: Boolean,
                                              errorMessage: Option[String],
                                              api: GoogleApiType
)

final case class BillingAccountChange(id: Long,
                                      billingProjectName: RawlsBillingProjectName,
                                      userId: RawlsUserSubjectId,
                                      previousBillingAccount: Option[RawlsBillingAccountName],
                                      newBillingAccount: Option[RawlsBillingAccountName],
                                      created: Instant,
                                      googleSyncTime: Option[Instant],
                                      outcome: Option[Outcome]
)

trait RawlsBillingProjectComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("NAME", O.PrimaryKey, O.Length(254))

    def creationStatus = column[String]("CREATION_STATUS", O.Length(20))

    def billingAccount = column[Option[String]]("BILLING_ACCOUNT", O.Length(100))

    def message = column[Option[String]]("MESSAGE")

    def cromwellBackend = column[Option[String]]("CROMWELL_BACKEND")

    def servicePerimeter = column[Option[String]]("SERVICE_PERIMETER")

    def googleProjectNumber = column[Option[String]]("GOOGLE_PROJECT_NUMBER")

    def invalidBillingAccount = column[Boolean]("INVALID_BILLING_ACCT")

    def spendReportDataset = column[Option[String]]("SPEND_REPORT_DATASET", O.Length(1024))

    def spendReportTable = column[Option[String]]("SPEND_REPORT_TABLE", O.Length(1024))

    def spendReportDatasetGoogleProject = column[Option[String]]("SPEND_REPORT_DATASET_GOOGLE_PROJECT", O.Length(1024))

    def billingProfileId = column[Option[UUID]]("BILLING_PROFILE_ID")

    def landingZoneId = column[Option[UUID]]("LANDING_ZONE_ID")

    def * = (projectName,
             creationStatus,
             billingAccount,
             message,
             cromwellBackend,
             servicePerimeter,
             googleProjectNumber,
             invalidBillingAccount,
             spendReportDataset,
             spendReportTable,
             spendReportDatasetGoogleProject,
             billingProfileId,
             landingZoneId
    ) <> ((RawlsBillingProjectRecord.apply _).tupled, RawlsBillingProjectRecord.unapply)
  }

  final class BillingAccountChanges(tag: Tag) extends Table[BillingAccountChange](tag, "BILLING_ACCOUNT_CHANGES") {

    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)

    def billingProjectName = column[String]("BILLING_PROJECT_NAME")

    def userId = column[String]("USER_ID")

    def previousBillingAccount = column[Option[String]]("PREVIOUS_BILLING_ACCOUNT")

    def newBillingAccount = column[Option[String]]("NEW_BILLING_ACCOUNT")

    def created = column[Timestamp]("CREATED")

    def googleSyncTime = column[Option[Timestamp]]("GOOGLE_SYNC_TIME")

    def outcome = column[Option[String]]("OUTCOME")

    def message = column[Option[String]]("MESSAGE")

    override def * =
      (
        id,
        billingProjectName,
        userId,
        previousBillingAccount,
        newBillingAccount,
        created,
        googleSyncTime,
        outcome,
        message
      ) <> (
        r => MigrationUtils.unsafeFromEither(BillingAccountChanges.fromRecord(r)),
        BillingAccountChanges.toRecord(_: BillingAccountChange).some
      )
  }

  // these 2 implicits are lazy because there is a timing problem initializing MappedColumnType, if they are not lazy
  // we get null pointer exceptions
  implicit lazy val googleApiTypeColumnType: JdbcType[GoogleApiType] with BaseTypedType[GoogleApiType] =
    MappedColumnType.base[GoogleApiType, String](
      apiType => apiType.toString,
      stringValue => GoogleApiTypes.withName(stringValue)
    )

  implicit lazy val googleOperationNameColumnType
    : JdbcType[GoogleOperationName] with BaseTypedType[GoogleOperationName] =
    MappedColumnType.base[GoogleOperationName, String](
      operationName => operationName.toString,
      stringValue => GoogleOperationNames.withName(stringValue)
    )

  class RawlsBillingProjectOperationTable(tag: Tag)
      extends Table[RawlsBillingProjectOperationRecord](tag, "BILLING_PROJECT_OPERATION") {
    def projectName = column[String]("PROJECT_NAME", O.Length(254))

    def operationName = column[GoogleOperationName]("OPERATION_NAME", O.Length(254))

    def operationId = column[String]("OPERATION_ID", O.Length(254))

    def done = column[Boolean]("DONE")

    def errorMessage = column[Option[String]]("ERROR_MESSAGE")

    def api = column[GoogleApiType]("API")

    def pk = primaryKey("PK_BILLING_PROJECT_OPERATION", (projectName, operationName))

    def * = (projectName, operationName, operationId, done, errorMessage, api) <> (
      RawlsBillingProjectOperationRecord.tupled,
      RawlsBillingProjectOperationRecord.unapply
    )
  }

  protected val rawlsBillingProjectOperationQuery = TableQuery[RawlsBillingProjectOperationTable]

  type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    def create(billingProject: RawlsBillingProject): ReadWriteAction[RawlsBillingProject] = {
      validateUserDefinedString(billingProject.projectName.value)
      rawlsBillingProjectQuery
        .withProjectName(billingProject.projectName)
        .result
        .flatMap {
          case Seq() => rawlsBillingProjectQuery += RawlsBillingProjectRecord.fromBillingProject(billingProject)
          case _ =>
            throw new RawlsException(
              s"Cannot create billing project [${billingProject.projectName.value}] in database because it already exists. If you're testing, this is likely because you previously registered this project, but failed to correctly unregister it. See https://broad.io/44jud7"
            )
        }
        .map(_ => billingProject)
    }

    def updateBillingProfileId(projectName: RawlsBillingProjectName, billingProfileId: Option[UUID]): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .setBillingProfileId(billingProfileId)

    def updateBillingAccountValidity(billingAccount: RawlsBillingAccountName, isInvalid: Boolean): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withBillingAccount(billingAccount.some)
        .setInvalidBillingAccount(isInvalid)

    def updateBillingAccount(projectName: RawlsBillingProjectName,
                             billingAccount: Option[RawlsBillingAccountName],
                             userSubjectId: RawlsUserSubjectId
    ): ReadWriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .setBillingAccount(billingAccount, userSubjectId)

    def updateServicePerimeter(projectName: RawlsBillingProjectName,
                               servicePerimeter: Option[ServicePerimeterName]
    ): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .setServicePerimeter(servicePerimeter)

    def updateGoogleProjectNumber(projectName: RawlsBillingProjectName,
                                  googleProjectNumber: Option[GoogleProjectNumber]
    ): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .setGoogleProjectNumber(googleProjectNumber)

    def updateCreationStatus(projectName: RawlsBillingProjectName,
                             status: CreationStatus,
                             message: Option[String] = None
    ): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .setCreationStatus(status, message)

    def updateLandingZone(projectName: RawlsBillingProjectName, landingZoneId: Option[UUID]): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .map(_.landingZoneId)
        .update(landingZoneId)

    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadAction[Seq[RawlsBillingProject]] =
      rawlsBillingProjectQuery
        .withCreationStatus(status)
        .read

    def listProjectsWithServicePerimeterAndStatus(servicePerimeter: ServicePerimeterName,
                                                  statuses: CreationStatus*
    ): ReadAction[Seq[RawlsBillingProject]] =
      rawlsBillingProjectQuery
        .withServicePerimeter(servicePerimeter.some)
        .withCreationStatuses(statuses)
        .read

    def load(projectName: RawlsBillingProjectName): ReadWriteAction[Option[RawlsBillingProject]] =
      rawlsBillingProjectQuery
        .withProjectName(projectName)
        .read
        .map(_.headOption)

    def delete(billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] =
      rawlsBillingProjectQuery.withProjectName(billingProjectName).delete.map(_ > 0)

    def getBillingProjects(projectNames: Set[RawlsBillingProjectName]): ReadAction[Seq[RawlsBillingProject]] =
      rawlsBillingProjectQuery.withProjectNames(projectNames).read

    def getBillingProjectsWithProfile(billingProfileId: Option[UUID]): ReadAction[Seq[RawlsBillingProject]] =
      rawlsBillingProjectQuery.withBillingProfileId(billingProfileId).read

    def getBillingProjectDetails(
      projectNames: Seq[RawlsBillingProjectName]
    ): ReadAction[Map[String, (CreationStatuses.CreationStatus, Option[String])]] =
      for {
        projects <- rawlsBillingProjectQuery.withProjectNames(projectNames).result
      } yield Map.from(projects.map { p =>
        p.projectName -> (CreationStatuses.withName(p.creationStatus), p.message)
      })

    def setBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName,
                                            datasetName: Option[BigQueryDatasetName],
                                            tableName: Option[BigQueryTableName],
                                            datasetGoogleProject: Option[GoogleProject]
    ): WriteAction[Int] =
      rawlsBillingProjectQuery
        .withProjectName(billingProjectName)
        .setBillingProjectSpendConfiguration(datasetName, tableName, datasetGoogleProject)

    def clearBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName): WriteAction[Int] =
      setBillingProjectSpendConfiguration(billingProjectName, None, None, None)

    def getBillingProjectSpendConfiguration(
      billingProjectName: RawlsBillingProjectName
    ): ReadAction[Option[BillingProjectSpendExport]] =
      rawlsBillingProjectQuery
        .withProjectName(billingProjectName)
        .result
        .map(_.headOption.map(RawlsBillingProjectRecord.toBillingProjectSpendExport))

    def insertOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Unit] =
      (rawlsBillingProjectOperationQuery ++= operations).map(_ => ())

    def updateOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Seq[Int]] =
      DBIO.sequence(
        operations.map(rec =>
          rawlsBillingProjectOperationQuery
            .filter(x => x.projectName === rec.projectName && x.operationName === rec.operationName)
            .update(rec)
        )
      )

    def loadOperationsForProjects(projectNames: Seq[RawlsBillingProjectName],
                                  operationName: GoogleOperationName
    ): ReadAction[Seq[RawlsBillingProjectOperationRecord]] =
      rawlsBillingProjectOperationQuery
        .filter(x => x.projectName.inSetBind(projectNames.map(_.value)) && x.operationName === operationName)
        .result
  }

  implicit class RawlsBillingProjectExtensions(query: RawlsBillingProjectQuery) {

    def read: ReadAction[Seq[RawlsBillingProject]] =
      for {
        projectRecords <- query.result
      } yield projectRecords.map(RawlsBillingProjectRecord.toBillingProject)

    // filters
    def withProjectName(projectName: RawlsBillingProjectName): RawlsBillingProjectQuery =
      query.filter(_.projectName === projectName.value)

    def withProjectNames(projectNames: Iterable[RawlsBillingProjectName]): RawlsBillingProjectQuery =
      query.filter(_.projectName.inSetBind(projectNames.map(_.value)))

    def withBillingProfileId(billingProfileId: Option[UUID]): RawlsBillingProjectQuery =
      query.filter(_.billingProfileId === billingProfileId.map(_.value))

    def withBillingAccount(billingAccount: Option[RawlsBillingAccountName]): RawlsBillingProjectQuery =
      query.filter(_.billingAccount === billingAccount.map(_.value))

    def withCreationStatus(status: CreationStatus): RawlsBillingProjectExtensions =
      query.filter(_.creationStatus === status.toString)

    def withCreationStatuses(statuses: Iterable[CreationStatus]): RawlsBillingProjectQuery =
      query.filter(_.creationStatus.inSetBind(statuses.map(_.toString)))

    def withServicePerimeter(perimeter: Option[ServicePerimeterName]): RawlsBillingProjectQuery =
      query.filter(_.servicePerimeter === perimeter.map(_.value))

    // setters
    def setBillingProfileId(billingProfileId: Option[UUID]): WriteAction[Int] =
      query
        .map(_.billingProfileId)
        .update(billingProfileId.map(_.value))

    def setServicePerimeter(servicePerimeter: Option[ServicePerimeterName]): WriteAction[Int] =
      query
        .map(_.servicePerimeter)
        .update(servicePerimeter.map(_.value))

    def setGoogleProjectNumber(googleProjectNumber: Option[GoogleProjectNumber]): WriteAction[Int] =
      query
        .map(_.googleProjectNumber)
        .update(googleProjectNumber.map(_.value))

    def setCreationStatus(status: CreationStatus, message: Option[String] = None): WriteAction[Int] =
      query
        .map(p => (p.creationStatus, p.message))
        .update((status.toString, message))

    def setMessage(message: Option[String]): WriteAction[Int] =
      query.map(_.message).update(message)

    def setBillingProjectSpendConfiguration(datasetName: Option[BigQueryDatasetName],
                                            tableName: Option[BigQueryTableName],
                                            datasetGoogleProject: Option[GoogleProject]
    ): WriteAction[Int] =
      query
        .map(bp => (bp.spendReportDataset, bp.spendReportTable, bp.spendReportDatasetGoogleProject))
        .update(datasetName.map(_.value), tableName.map(_.value), datasetGoogleProject.map(_.value))

    def setInvalidBillingAccount(isInvalid: Boolean): WriteAction[Int] =
      query.map(_.invalidBillingAccount).update(isInvalid)

    def setBillingAccount(billingAccount: Option[RawlsBillingAccountName],
                          userSubjectId: RawlsUserSubjectId
    ): ReadWriteAction[Int] =
      for {
        billingProjects <- query.read
        count <-
          query
            .map(p => (p.billingAccount, p.invalidBillingAccount))
            .update((billingAccount.map(_.value), false))
        // Record each billing account change
        // - so the `WorkspaceBillingAccountActor` can synchronise the changes with google
        // - to keep an audit log of billing account changes
        _ <- DBIO.sequence(billingProjects.map { project =>
          BillingAccountChanges.create(
            project.projectName,
            project.billingAccount,
            billingAccount,
            userSubjectId
          )
        })
      } yield count
  }

  type BillingAccountChangeQuery = Query[BillingAccountChanges, BillingAccountChange, Seq]

  object BillingAccountChanges extends TableQuery(new BillingAccountChanges(_)) {

    type RecordType = (
      Long, // id
      String, // billing project name
      String, // User Id
      Option[String], // Original billing account
      Option[String], // New billing account
      Timestamp, // Created
      Option[Timestamp], // Google sync time
      Option[String], // Outcome
      Option[String] // Message
    )

    def fromRecord(record: RecordType): Either[String, BillingAccountChange] = record match {
      case (id,
            billingProjectName,
            userId,
            previousBillingAccount,
            newBillingAccount,
            created,
            googleSyncTime,
            outcome,
            message
          ) =>
        Outcome.fromFields(outcome, message).map { outcome =>
          BillingAccountChange(
            id,
            RawlsBillingProjectName(billingProjectName),
            RawlsUserSubjectId(userId),
            previousBillingAccount.map(RawlsBillingAccountName),
            newBillingAccount.map(RawlsBillingAccountName),
            created.toInstant,
            googleSyncTime.map(_.toInstant),
            outcome
          )
        }
    }

    def toRecord(billingAccountChange: BillingAccountChange): RecordType = {
      val (outcome, message) = Outcome.toFields(billingAccountChange.outcome)
      (
        billingAccountChange.id,
        billingAccountChange.billingProjectName.value,
        billingAccountChange.userId.value,
        billingAccountChange.previousBillingAccount.map(_.value),
        billingAccountChange.newBillingAccount.map(_.value),
        Timestamp.from(billingAccountChange.created),
        billingAccountChange.googleSyncTime.map(Timestamp.from),
        outcome,
        message
      )
    }

    def getLastChange(billingProject: RawlsBillingProjectName): ReadAction[Option[BillingAccountChange]] =
      BillingAccountChanges
        .withProjectName(billingProject)
        .sortBy(_.id.desc)
        .result
        .map(_.headOption)
  }

  implicit class BillingAccountChangeExtensions(query: BillingAccountChangeQuery) {

    def create(billingProjectName: RawlsBillingProjectName,
               previousBillingAccount: Option[RawlsBillingAccountName],
               newBillingAccount: Option[RawlsBillingAccountName],
               userSubjectId: RawlsUserSubjectId
    ): ReadWriteAction[Int] =
      query
        .map(change =>
          (change.billingProjectName, change.previousBillingAccount, change.newBillingAccount, change.userId)
        )
        .insert(
          (billingProjectName.value,
           previousBillingAccount.map(_.value),
           newBillingAccount.map(_.value),
           userSubjectId.value
          )
        )

    // filters
    def withId(id: Long): BillingAccountChangeQuery =
      query.filter(_.id === id)

    def withProjectName(billingProjectName: RawlsBillingProjectName): BillingAccountChangeQuery =
      query.filter(_.billingProjectName === billingProjectName.value)

    /* SELECT *
     * FROM BILLING_ACCOUNT_CHANGES BAC,
     * (  SELECT BILLING_PROJECT_NAME, MAX(ID) AS MAXID
     *    FROM BILLING_ACCOUNT_CHANGES
     *    GROUP BY BILLING_PROJECT_NAME
     * ) AS SUBTABLE
     * WHERE SUBTABLE.MAXID = BAC.ID
     */
    /**
      * Selects the latest changes for all billing projects in query.
      */
    def latestChanges: BillingAccountChangeQuery = {
      val latestChangeIds = query
        .groupBy(_.billingProjectName)
        .map { case (_, group) => group.map(_.id).max }

      query
        .filter(_.id.in(latestChangeIds))
        .sortBy(_.id.asc)
    }

    def unsynced: BillingAccountChangeQuery =
      query.filter(_.googleSyncTime.isEmpty)

    // setters
    def setOutcome(outcome: Option[Outcome]): WriteAction[Int] =
      query.map(c => (c.outcome, c.message)).update(Outcome.toFields(outcome))

    def setGoogleSyncTime(syncTime: Option[Instant]): WriteAction[Int] =
      query.map(_.googleSyncTime).update(syncTime.map(Timestamp.from))
  }
}
