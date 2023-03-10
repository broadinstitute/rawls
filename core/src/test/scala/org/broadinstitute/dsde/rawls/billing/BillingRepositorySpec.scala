package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  CromwellBackend,
  GoogleProjectNumber,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsUserEmail,
  ServicePerimeterName
}
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

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
    val userEmail = RawlsUserEmail("user@email.com")
    Await.result(repo.createBillingProject(billingProject), Duration.Inf)
    Await.result(
      wsmRecordDao.create(
        WorkspaceManagerResourceMonitorRecord.forAzureLandingZone(
          jobId,
          billingProject.projectName,
          userEmail
        )
      ),
      Duration.Inf
    )

    val records = Await.result(wsmRecordDao.selectAll(), Duration.Inf)

    assertResult(1)(records.length)
    assertResult(JobType.AzureLandingZoneResult)(records.head.jobType)

    assertResult(None)(records.head.workspaceId)
    assertResult(Some(billingProject.projectName.value))(records.head.billingProjectId)
    assertResult(Some(userEmail.value))(records.head.userEmail)
    assertResult(jobId)(records.head.jobControlId)
  }

  behavior of "getCreationStatus"

  it should "return the status of a billing project" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()
    val status = billingProject.status

    Await.result(repo.createBillingProject(billingProject), Duration.Inf)

    assertResult(status) {
      Await.result(repo.getCreationStatus(billingProject.projectName), Duration.Inf)
    }
  }

  behavior of "getLandingZoneId"

  it should "return the landing zone ID" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val billingProject = makeBillingProject()
    val landingZoneId = billingProject.landingZoneId

    Await.result(repo.createBillingProject(billingProject), Duration.Inf)

    assertResult(landingZoneId) {
      Await.result(repo.getLandingZoneId(billingProject.projectName), Duration.Inf)
    }
  }

  behavior of "getBillingProjectsWithProfile"

  it should "return billing projects with the specified profile ID" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val firstProject = makeBillingProject()
    val secondProject = makeBillingProject()
    val projectWithDifferentBP = makeBillingProject()
    val billingProfileId = UUID.fromString(firstProject.billingProfileId.get)

    Await.result(repo.createBillingProject(firstProject), Duration.Inf)
    Await.result(repo.createBillingProject(secondProject), Duration.Inf)
    Await.result(repo.setBillingProfileId(secondProject.projectName, billingProfileId), Duration.Inf)
    Await.result(repo.createBillingProject(projectWithDifferentBP), Duration.Inf)

    val projectNames = Await.result(repo.getBillingProjectsWithProfile(Some(billingProfileId)), Duration.Inf).map {
      _.projectName
    }
    assertResult(2)(projectNames.length)
    projectNames should contain theSameElementsAs Seq(firstProject.projectName, secondProject.projectName)
  }

  it should "return return an empty Seq if no billing profile ID specified" in withDefaultTestDatabase {
    val repo = new BillingRepository(slickDataSource)
    val projects = Await.result(repo.getBillingProjectsWithProfile(None), Duration.Inf)
    assertResult(0)(projects.length)
  }
}
