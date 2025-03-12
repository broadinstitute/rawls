package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProject, RawlsGoogleProject}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class GoogleProjectRepositorySpec extends AnyFlatSpec with TestDriverComponent {

  behavior of "createGoogleProject"

  def makeGoogleProject(billingProject: RawlsBillingProject) = RawlsGoogleProject(
    UUID.randomUUID().toString,
    billingProject.billingAccount.map(_.toString),
    Some("fake message"),
    billingProject.projectName
  )

  it should "create a google project record" in withDefaultTestDatabase {
    val repo = new GoogleProjectRepository(slickDataSource)
    val googleProject = makeGoogleProject(testData.testProject1)

    val result = Await.result(repo.createGoogleProject(googleProject), Duration.Inf)

    assertResult(googleProject) {
      result
    }
  }

}
