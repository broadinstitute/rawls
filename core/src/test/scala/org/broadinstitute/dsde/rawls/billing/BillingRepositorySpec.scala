package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreationStatuses, CromwellBackend, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, ServicePerimeterName}
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
    None,  // azure managed app coordinates are paged in from BPM
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
      repo.getBillingProject(RawlsBillingProjectName(UUID.randomUUID().toString)), Duration.Inf
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

    assertResult(billingProfileId.toString){ updated.get.billingProfileId.get }
  }
}
