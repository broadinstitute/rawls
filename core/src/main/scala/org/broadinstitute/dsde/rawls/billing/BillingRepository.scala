package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobType,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsBillingProject, RawlsBillingProjectName}

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.concurrent.{ExecutionContext, Future}

/**
 * Data access for rawls billing projects
 */
class BillingRepository(dataSource: SlickDataSource) {

  def setBillingProfileId(projectName: RawlsBillingProjectName, billingProfileId: UUID): Future[Int] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.updateBillingProfileId(projectName, Some(billingProfileId))
    }

  def createBillingProject(billingProject: RawlsBillingProject): Future[RawlsBillingProject] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.create(billingProject)
    }

  def getBillingProject(projectName: RawlsBillingProjectName): Future[Option[RawlsBillingProject]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.load(projectName)
    }

  def getBillingProjects(projectNames: Set[RawlsBillingProjectName]): Future[Seq[RawlsBillingProject]] =
    dataSource.inTransaction(_.rawlsBillingProjectQuery.getBillingProjects(projectNames))

  def getBillingProfileId(
    projectName: RawlsBillingProjectName
  )(implicit executionContext: ExecutionContext): Future[Option[String]] =
    getBillingProject(projectName) map { billingProjectOpt =>
      billingProjectOpt
        .getOrElse(
          throw new RawlsException(s"Billing Project ${projectName.value} does not exist in Rawls database")
        )
        .billingProfileId
    }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[Boolean] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.delete(projectName)
    }

  def updateCreationStatus(projectName: RawlsBillingProjectName,
                           status: CreationStatus,
                           message: Option[String]
  ): Future[Int] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.updateCreationStatus(
        projectName,
        status,
        message
      )
    }

  def storeLandingZoneCreationRecord(jobRecordId: UUID, billingProjectName: String): Future[Unit] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.WorkspaceManagerResourceMonitorRecordQuery.create(
        WorkspaceManagerResourceMonitorRecord(jobRecordId,
                                              WorkspaceManagerResourceJobType.AzureLandingZoneResult.toString,
                                              None,
                                              Option(billingProjectName),
                                              new Timestamp(new Date().getTime)
        )
      )
    }
  def getWorkspaceManagerResourceMonitorRecords(): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.WorkspaceManagerResourceMonitorRecordQuery.getRecords()
    }

  def failUnlessHasNoWorkspaces(projectName: RawlsBillingProjectName)(implicit ec: ExecutionContext): Future[Unit] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.countByNamespace(projectName) map { count =>
        if (count == 0) ()
        else
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Project cannot be deleted because it contains workspaces."
            )
          )
      }
    }
}
