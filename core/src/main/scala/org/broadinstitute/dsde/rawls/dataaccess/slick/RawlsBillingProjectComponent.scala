package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.GoogleApiTypes.GoogleApiType
import org.broadinstitute.dsde.rawls.dataaccess.GoogleOperationNames.GoogleOperationName
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleApiTypes, GoogleOperationNames}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.{IgnoreResultExtensionMethod, InsertExtensionMethod}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.ExecutionContext

case class RawlsBillingProjectRecord(projectName: String, creationStatus: String, billingAccount: Option[String], message: Option[String], cromwellBackend: Option[String], servicePerimeter: Option[String], googleProjectNumber: Option[String], invalidBillingAccount: Boolean, spendReportDataset: Option[String], spendReportTable: Option[String], spendReportDatasetGoogleProject: Option[String])

case class RawlsBillingProjectOperationRecord(projectName: String, operationName: GoogleOperationName, operationId: String, done: Boolean, errorMessage: Option[String], api: GoogleApiType)

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

    def * = (projectName, creationStatus, billingAccount, message, cromwellBackend, servicePerimeter, googleProjectNumber, invalidBillingAccount, spendReportDataset, spendReportTable, spendReportDatasetGoogleProject) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  final class BillingAccountChanges(tag: Tag)
    extends Table[BillingAccountChange](tag, "BILLING_ACCOUNT_CHANGES") {

    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)

    def billingProjectName = column[String]("BILLING_PROJECT_NAME")

    def userId = column[String]("USER_ID")

    def originalBillingAccount = column[Option[String]]("ORIGINAL_BILLING_ACCOUNT")

    def newBillingAccount = column[Option[String]]("NEW_BILLING_ACCOUNT")

    def created = column[Timestamp]("CREATED")

    def googleSyncTime = column[Option[Timestamp]]("GOOGLE_SYNC_TIME")

    def outcome = column[Option[String]]("OUTCOME")

    def message = column[Option[String]]("MESSAGE")

    override def * =
      (
        id, billingProjectName, userId, originalBillingAccount, newBillingAccount, created,
        googleSyncTime, outcome, message
      ) <> (
        r => MigrationUtils.unsafeFromEither(BillingAccountChanges.fromRecord(r)),
        BillingAccountChanges.toRecord(_: BillingAccountChange).some
      )
  }

  // these 2 implicits are lazy because there is a timing problem initializing MappedColumnType, if they are not lazy
  // we get null pointer exceptions
  implicit lazy val googleApiTypeColumnType = MappedColumnType.base[GoogleApiType, String](
    { apiType => apiType.toString },
    { stringValue => GoogleApiTypes.withName(stringValue) }
  )

  implicit lazy val googleOperationNameColumnType = MappedColumnType.base[GoogleOperationName, String](
    { operationName => operationName.toString },
    { stringValue => GoogleOperationNames.withName(stringValue) }
  )

  class RawlsBillingProjectOperationTable(tag: Tag) extends Table[RawlsBillingProjectOperationRecord](tag, "BILLING_PROJECT_OPERATION") {
    def projectName = column[String]("PROJECT_NAME", O.Length(254))

    def operationName = column[GoogleOperationName]("OPERATION_NAME", O.Length(254))

    def operationId = column[String]("OPERATION_ID", O.Length(254))

    def done = column[Boolean]("DONE")

    def errorMessage = column[Option[String]]("ERROR_MESSAGE")

    def api = column[GoogleApiType]("API")

    def pk = primaryKey("PK_BILLING_PROJECT_OPERATION", (projectName, operationName))

    def * = (projectName, operationName, operationId, done, errorMessage, api) <> (RawlsBillingProjectOperationRecord.tupled, RawlsBillingProjectOperationRecord.unapply)
  }

  protected val rawlsBillingProjectOperationQuery = TableQuery[RawlsBillingProjectOperationTable]

  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    def create(billingProject: RawlsBillingProject): ReadWriteAction[RawlsBillingProject] = {
      validateUserDefinedString(billingProject.projectName.value)
      uniqueResult(findBillingProjectByName(billingProject.projectName).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProject.projectName.value}] in database because it already exists. If you're testing, this is likely because you previously registered this project, but failed to correctly unregister it. See https://broad.io/44jud7")
        case None => (rawlsBillingProjectQuery += marshalBillingProject(billingProject)).map { _ => billingProject }
      }
    }

    def updateBillingAccountValidity(billingAccount: RawlsBillingAccountName, isInvalid: Boolean): WriteAction[Int] = {
      findBillingProjectsByBillingAccount(billingAccount).map(_.invalidBillingAccount).update(isInvalid)
    }

    def updateBillingAccount(projectName: RawlsBillingProjectName, billingAccount: Option[RawlsBillingAccountName], userSubjectId: RawlsUserSubjectId): ReadWriteAction[Unit] =
      for {
        billingProject <- load(projectName).map {
          _.getOrElse(throw new RawlsException(s"No such billing project '$projectName'"))
        }

        _ <- findBillingProjectByName(projectName)
          .map(row => (row.billingAccount, row.invalidBillingAccount))
          .update(billingAccount.map(_.value), false)

        _ <- billingAccountChangeQuery.create(
          projectName,
          billingProject.billingAccount,
          billingAccount,
          userSubjectId
        )
      } yield ()

    def updateServicePerimeter(projectName: RawlsBillingProjectName,
                               servicePerimeter: Option[ServicePerimeterName]): WriteAction[Unit] =
      findBillingProjectByName(projectName)
        .map(_.servicePerimeter)
        .update(servicePerimeter.map(_.value))
        .ignore

    def updateGoogleProjectNumber(projectName: RawlsBillingProjectName,
                                  googleProjectNumber: Option[GoogleProjectNumber]): WriteAction[Unit] =
      findBillingProjectByName(projectName)
        .map(_.googleProjectNumber)
        .update(googleProjectNumber.map(_.value))
        .ignore

    def updateCreationStatus(projectName: RawlsBillingProjectName,
                             status: CreationStatus,
                             message: Option[String] = None): WriteAction[Unit] =
      findBillingProjectByName(projectName)
        .map(p => (p.creationStatus, p.message))
        .update((status.toString, message))
        .ignore

    def listAll(): ReadWriteAction[Seq[RawlsBillingProject]] = {
      for {
        projectRecords <- this.result
      } yield {
        projectRecords.map(unmarshalBillingProject)
      }
    }

    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadWriteAction[Seq[RawlsBillingProject]] = {
      for {
        projectRecords <- filter(_.creationStatus === status.toString).result
      } yield {
        projectRecords.map(unmarshalBillingProject)
      }
    }

    def listProjectsWithServicePerimeterAndStatus(servicePerimeter: ServicePerimeterName, statuses: CreationStatus*): ReadAction[Seq[RawlsBillingProject]] = {
      for {
        projectRecords <- getProjectsWithPerimeterAndStatusQuery(servicePerimeter, statuses).result
      } yield {
        projectRecords.map(unmarshalBillingProject)
      }
    }

    def getProjectsWithPerimeterAndStatusQuery(servicePerimeter: ServicePerimeterName, statuses: Seq[CreationStatus]): RawlsBillingProjectQuery = {
      filter(rec => rec.servicePerimeter === servicePerimeter.value && rec.creationStatus.inSetBind(statuses.map(_.toString)))
    }

    def load(projectName: RawlsBillingProjectName): ReadWriteAction[Option[RawlsBillingProject]] = {
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(projectName)).map(_.map(unmarshalBillingProject))
    }

    def delete(billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
      rawlsBillingProjectQuery.filter(_.projectName === billingProjectName.value).delete map { count => count > 0 }
    }

    def getBillingProjects(projectNames: Set[RawlsBillingProjectName]): ReadAction[Seq[RawlsBillingProject]] = {
      val query = for {
        project <- rawlsBillingProjectQuery if (project.projectName.inSetBind(projectNames.map(_.value)))
      } yield project

      query.result.map(_.map(unmarshalBillingProject))
    }

    def getBillingProjectDetails(projectNames: Set[RawlsBillingProjectName]): ReadAction[Map[String, (CreationStatuses.CreationStatus, Option[String])]] = {
      val query = for {
        project <- rawlsBillingProjectQuery if (project.projectName.inSetBind(projectNames.map(_.value)))
      } yield project

      query.result.map(_.map { project =>
        project.projectName -> (CreationStatuses.withName(project.creationStatus), project.message)
      }.toMap)
    }

    def setBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName, datasetName: Option[BigQueryDatasetName], tableName: Option[BigQueryTableName], datasetGoogleProject: Option[GoogleProject]): WriteAction[Int] = {
      rawlsBillingProjectQuery
        .filter(_.projectName === billingProjectName.value)
        .map(bp => (bp.spendReportDataset, bp.spendReportTable, bp.spendReportDatasetGoogleProject))
        .update(datasetName.map(_.value), tableName.map(_.value), datasetGoogleProject.map(_.value))
    }

    def clearBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName): WriteAction[Int] = {
      setBillingProjectSpendConfiguration(billingProjectName, None, None, None)
    }

    def getBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName): ReadAction[Option[BillingProjectSpendExport]] = {
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(billingProjectName)).map(_.map(unmarshalBillingProjectSpendExport))
    }

    def insertOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Unit] = {
      (rawlsBillingProjectOperationQuery ++= operations).map(_ => ())
    }

    def updateOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Seq[Int]] = {
      DBIO.sequence(operations.map(rec => rawlsBillingProjectOperationQuery.filter(x => x.projectName === rec.projectName && x.operationName === rec.operationName).update(rec)))
    }

    def loadOperationsForProjects(projectNames: Seq[RawlsBillingProjectName], operationName: GoogleOperationName): ReadAction[Seq[RawlsBillingProjectOperationRecord]] = {
      rawlsBillingProjectOperationQuery.filter(x => x.projectName.inSetBind(projectNames.map(_.value)) && x.operationName === operationName).result
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.status.toString, billingProject.billingAccount.map(_.value), billingProject.message, billingProject.cromwellBackend.map(_.value), billingProject.servicePerimeter.map(_.value), billingProject.googleProjectNumber.map(_.value), billingProject.invalidBillingAccount, billingProject.spendReportDataset.map(_.value), billingProject.spendReportTable.map(_.value), billingProject.spendReportDatasetGoogleProject.map(_.value))
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), CreationStatuses.withName(projectRecord.creationStatus), projectRecord.billingAccount.map(RawlsBillingAccountName), projectRecord.message, projectRecord.cromwellBackend.map(CromwellBackend), projectRecord.servicePerimeter.map(ServicePerimeterName), projectRecord.googleProjectNumber.map(GoogleProjectNumber), projectRecord.invalidBillingAccount, projectRecord.spendReportDataset.map(BigQueryDatasetName), projectRecord.spendReportTable.map(BigQueryTableName), projectRecord.spendReportDatasetGoogleProject.map(GoogleProject))
    }

    private def findBillingProjectByName(name: RawlsBillingProjectName): RawlsBillingProjectQuery = {
      filter(_.projectName === name.value)
    }

    private def findBillingProjectsByBillingAccount(billingAccount: RawlsBillingAccountName): RawlsBillingProjectQuery = {
      filter(_.billingAccount === billingAccount.value)
    }

    private def unmarshalBillingProjectSpendExport(projectRecord: RawlsBillingProjectRecord): BillingProjectSpendExport = {
      val table = (projectRecord.spendReportDatasetGoogleProject, projectRecord.spendReportDataset, projectRecord.spendReportTable) match {
        case (Some(googleProjectId), Some(datasetName), Some(tableName)) => Option(s"$googleProjectId.$datasetName.$tableName")
        case _ => None
      }
      val billingAccount = RawlsBillingAccountName(projectRecord.billingAccount.getOrElse(throw new RawlsException(s"billing account not set on project ${projectRecord.projectName}")))

      BillingProjectSpendExport(RawlsBillingProjectName(projectRecord.projectName), billingAccount, table)
    }
  }

  object BillingAccountChanges {

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
      case (id, billingProjectName, userId, originalBillingAccount, newBillingAccount, created, googleSyncTime, outcome, message) =>
        Outcome.fromFields(outcome, message).map { outcome =>
          BillingAccountChange(
            id,
            RawlsBillingProjectName(billingProjectName),
            RawlsUserSubjectId(userId),
            originalBillingAccount.map(RawlsBillingAccountName),
            newBillingAccount.map(RawlsBillingAccountName),
            created.toInstant,
            googleSyncTime.map(_.toInstant),
            outcome
          )
        }
    }

    def toRecord(billingAccountChange: BillingAccountChange): RecordType = {
      val (outcome, message) = billingAccountChange.outcome.map(Outcome.toTuple).getOrElse((None, None))
      (
        billingAccountChange.id,
        billingAccountChange.billingProjectName.value,
        billingAccountChange.userId.value,
        billingAccountChange.originalBillingAccount.map(_.value),
        billingAccountChange.newBillingAccount.map(_.value),
        Timestamp.from(billingAccountChange.created),
        billingAccountChange.googleSyncTime.map(Timestamp.from),
        outcome,
        message
      )
    }
  }

  object billingAccountChangeQuery extends TableQuery(new BillingAccountChanges(_)) {
    def create(billingProjectName: RawlsBillingProjectName,
               oldBillingAccount: Option[RawlsBillingAccountName],
               newBillingAccount: Option[RawlsBillingAccountName],
               userSubjectId: RawlsUserSubjectId): ReadWriteAction[Unit] =
      billingAccountChangeQuery
        .map(change => (change.billingProjectName, change.originalBillingAccount, change.newBillingAccount, change.userId))
        .insert((billingProjectName.value, oldBillingAccount.map(_.value), newBillingAccount.map(_.value), userSubjectId.value))
        .ignore

    def lastChange(billingProjectName: RawlsBillingProjectName)
                  (implicit executionContext: ExecutionContext): ReadWriteAction[Option[BillingAccountChange]] =
      billingAccountChangeQuery
        .filter(_.billingProjectName === billingProjectName.value)
        .sortBy(_.id.desc)
        .take(1)
        .result
        .map(_.headOption)
  }
}

final case class BillingAccountChange(id: Long,
                                      billingProjectName: RawlsBillingProjectName,
                                      userId: RawlsUserSubjectId,
                                      originalBillingAccount: Option[RawlsBillingAccountName],
                                      newBillingAccount: Option[RawlsBillingAccountName],
                                      created: Instant,
                                      googleSyncTime: Option[Instant],
                                      outcome: Option[Outcome]
                                     )

