package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleApiTypes, GoogleOperationNames}
import org.broadinstitute.dsde.rawls.dataaccess.GoogleApiTypes.GoogleApiType
import org.broadinstitute.dsde.rawls.dataaccess.GoogleOperationNames.GoogleOperationName
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}

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

  // these 2 implicits are lazy because there is a timing problem initializing MappedColumnType, if they are not lazy
  // we get null pointer exceptions
  implicit lazy val googleApiTypeColumnType = MappedColumnType.base[GoogleApiType, String](
    { apiType => apiType.toString },
    { stringValue => GoogleApiTypes.withName(stringValue) }
  )

  implicit lazy val googleOperationNameColumnType = MappedColumnType.base[GoogleOperationName, String](
    { operationName => operationName.toString },
    { stringValue => GoogleOperationNames.withName(stringValue)}
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

    def updateBillingProjects(projects: Traversable[RawlsBillingProject]): WriteAction[Seq[Int]] = {
      DBIO.sequence(projects.map(project => rawlsBillingProjectQuery.filter(_.projectName === project.projectName.value).update(marshalBillingProject(project))).toSeq)
    }

    def updateBillingAccountValidity(billingAccount: RawlsBillingAccountName, isInvalid: Boolean): WriteAction[Int] = {
      findBillingProjectsByBillingAccount(billingAccount).map(_.invalidBillingAccount).update(isInvalid)
    }

    def updateBillingAccount(projectName: RawlsBillingProjectName, billingAccount: Option[RawlsBillingAccountName]): WriteAction[Int] = {
      findBillingProjectByName(projectName).map(_.billingAccount).update(billingAccount.map(_.value))
    }


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
  }
}

