package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, SamResourceAction, SamResourceTypeNames, WorkspaceAzureCloudContext}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class BillingProfileManagerDAOSpec extends AnyFlatSpec with TestDriverComponent with MockitoSugar {
  val azConfig: AzureConfig = AzureConfig(
    "fake-sp-id",
    "fake-tenant-id",
    "fake-sub-id",
    "fake-mrg-id",
    "fake-bp-name",
    "fake-alpha-feature-group"
  )

  behavior of "listBillingProfiles"

  it should "return no billing profiles if the feature flag is off" in {
    val samDAO: SamDAO = mock[SamDAO]
    val config = new MultiCloudWorkspaceConfig(false, None, None)
    val billingProfileManagerDAO = new BillingProfileManagerDAO(samDAO, config)

    val result = Await.result(billingProfileManagerDAO.listBillingProfiles(userInfo), Duration.Inf)

    result.isEmpty shouldBe true
  }

  it should "return billing profiles to which the user has access" in {
    val config = new MultiCloudWorkspaceConfig(true, None,
      Some(azConfig)
    )
    val samDAO: SamDAO = mock[SamDAO]
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, azConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    )).thenReturn(Future.successful(true))
    val billingProfileManagerDAO = new BillingProfileManagerDAO(samDAO, config)

    val result = Await.result(
      billingProfileManagerDAO.listBillingProfiles(userInfo), Duration.Inf
    )

    val expected = Seq(
      RawlsBillingProject(
        RawlsBillingProjectName(azConfig.billingProjectName),
        CreationStatuses.Ready,
        None,
        None,
        azureManagedAppCoordinates = Some(
          WorkspaceAzureCloudContext(
            azConfig.azureTenantId,
            azConfig.azureSubscriptionId,
            azConfig.azureResourceGroupId
          )
        )
      )
    )

    result should contain theSameElementsAs expected
  }

  it should "return no profiles if the user lacks permissions" in {
    val config = new MultiCloudWorkspaceConfig(true, None,
      Some(azConfig)
    )
    val samDAO: SamDAO = mock[SamDAO]
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, azConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    )).thenReturn(Future.successful(false))
    val billingProfileManagerDAO = new BillingProfileManagerDAO(samDAO, config)

    val result = Await.result(billingProfileManagerDAO.listBillingProfiles(userInfo), Duration.Inf)

    result.isEmpty shouldBe true
  }
}