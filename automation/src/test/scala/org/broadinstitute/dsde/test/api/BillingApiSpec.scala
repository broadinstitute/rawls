package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.Rawls
import org.scalatest.CancelAfterFailure
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

//noinspection NoTailRecursionAnnotation,RedundantBlock,ScalaUnusedSymbol
@BillingsTest
class BillingApiSpec extends AnyFreeSpec with MethodFixtures with Matchers with TestReporterFixture with LazyLogging with CancelAfterFailure {

  "A user with a billing account" - {
    "can create a new billing project with v2 api" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)
      val billingProjectName = "rawls-billingapispecV2-" + makeRandomId()
      Rawls.billingV2.createBillingProject(billingProjectName, ServiceTestConfig.Projects.billingAccountId)
      val result = Rawls.billingV2.getBillingProject(billingProjectName).toList
      val expected = List(
        "projectName" -> billingProjectName,
        "billingAccount" -> ServiceTestConfig.Projects.billingAccountId,
        "invalidBillingAccount" -> false,
        "roles" -> List("Owner")
      )
      result should contain allElementsOf expected
      Rawls.billingV2.deleteBillingProject(billingProjectName)
    }
  }
}
