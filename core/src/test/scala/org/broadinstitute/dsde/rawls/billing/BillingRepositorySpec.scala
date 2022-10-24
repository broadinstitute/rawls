package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceManagerResourceMonitorRecord}

import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  CromwellBackend,
  GoogleProjectNumber,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  ServicePerimeterName
}
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BillingRepositorySpec extends AnyFlatSpec with TestDriverComponent {

  behavior of "createBillingProject"

  def makeBillingProject() = RawlsBillingProject(
    RawlsBillingProjectName(UUID.randomUUID().toString),
    CreationStatuses.Ready,
    Some(RawlsBillingAccountName("fake_account")),
    Some("fake message"),
    Some(CromwellBackend("fake_cromwell_backend")),
    Some(ServicePerimeterName("fake_sp_name")),
    Some(GoogleProjectNumber("fake_google_project_number")),
    false,
    Some(BigQueryDatasetName("dataset_name")),
    Some(BigQueryTableName("bq_table_name")),
    Some(GoogleProject("google_project")),
    None, // azure managed app coordinates are paged in from BPM
    Some(UUID.randomUUID().toString)
  )

  it should "create a billing project record" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()

    val result = Await.result(repo.createBillingProject(billingProject), Duration.Inf)

    assertResult(billingProject) {
      result
    }
  }

  behavior of "getBillingProject"

  it should "retrieve a previously created record" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()

    Await.result(repo.createBillingProject(billingProject), Duration.Inf)
    val result = Await.result(repo.getBillingProject(billingProject.projectName), Duration.Inf)

    assertResult(billingProject) {
      result.get
    }
  }

  it should "give back none for a missing project" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)

    val result = Await.result(
      repo.getBillingProject(RawlsBillingProjectName(UUID.randomUUID().toString)),
      Duration.Inf
    )

    assertResult(result) {
      None
    }
  }

  behavior of "setBillingProfileId"

  it should "set a billing profile ID" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()
    val billingProfileId = UUID.randomUUID()
    Await.result(repo.createBillingProject(billingProject), Duration.Inf)

    Await.result(repo.setBillingProfileId(billingProject.projectName, billingProfileId), Duration.Inf)
    val updated = Await.result(repo.getBillingProject(billingProject.projectName), Duration.Inf)

    assertResult(billingProfileId.toString)(updated.get.billingProfileId.get)
  }

  behavior of "deleteBillingProject"

  it should "delete a billing project record" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()
    Await.result(repo.createBillingProject(billingProject), Duration.Inf)

    val deleted = Await.result(repo.deleteBillingProject(billingProject.projectName), Duration.Inf)

    assertResult(deleted)(true)
  }

  behavior of "updateCreationStatus"

  it should "update creation status in a project record" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()
    Await.result(repo.createBillingProject(billingProject), Duration.Inf)
    Await.result(repo.updateCreationStatus(billingProject.projectName, CreationStatuses.Error, Some("errored project")),
                 Duration.Inf
    )
    val result = Await.result(
      repo.getBillingProject(billingProject.projectName),
      Duration.Inf
    )
    assertResult(CreationStatuses.Error)(result.get.status)
    assertResult(Some("errored project"))(result.get.message)
  }

  behavior of "storeLandingZoneCreationRecord"

  it should "store record for landing zone creation job" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val wsmRecordDao = new WorkspaceManagerResourceMonitorRecordDao(slickDataSource)
    val jobId = UUID.randomUUID()
    val billingProject = makeBillingProject()
    Await.result(repo.createBillingProject(billingProject), Duration.Inf)
    Await.result(wsmRecordDao.create(jobId, billingProject.projectName.value), Duration.Inf)

    val records = Await.result(wsmRecordDao.selectAll(), Duration.Inf)

    assertResult(1)(records.length)
    assertResult(WorkspaceManagerResourceMonitorRecord.JobType.AzureLandingZoneResult)(records.head.jobType)

    assertResult(None)(records.head.workspaceId)
    assertResult(Some(billingProject.projectName.value))(records.head.billingProjectId)
    assertResult(jobId)(records.head.jobControlId)
  }

}
