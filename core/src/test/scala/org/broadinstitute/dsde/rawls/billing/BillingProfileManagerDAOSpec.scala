package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreationStatuses, RawlsBillingProject, RawlsBillingProjectName, RawlsUserEmail, RawlsUserSubjectId, SamBillingProjectActions, SamBillingProjectRoles, SamResourceAction, SamResourceTypeNames, SamRolesAndActions, SamUserResource, UserInfo}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProfileManagerDAOSpec extends AnyFlatSpec with MockitoSugar {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val azConfig: AzureConfig = AzureConfig(
    "fake-sp-id",
    UUID.randomUUID().toString,
    UUID.randomUUID().toString,
    "fake-mrg-id",
    "fake-bp-name",
    "fake-alpha-feature-group",
    "eastus"
  )
  val userInfo: UserInfo = UserInfo(
    RawlsUserEmail("fake@example.com"),
    OAuth2BearerToken("fake_token"),
    0,
    RawlsUserSubjectId("sub"),
    None
  )

  behavior of "listBillingProfiles"

  it should "return billing profiles to which the user has access" in {
    val samDAO: SamDAO = mock[SamDAO]
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, azConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    )).thenReturn(Future.successful(true))
    val bpSamResource = SamUserResource(azConfig.billingProjectName,
      SamRolesAndActions(
        Set(SamBillingProjectRoles.owner),
        Set(SamBillingProjectActions.createWorkspace)
      ),
      SamRolesAndActions(Set.empty, Set.empty),
      SamRolesAndActions(Set.empty, Set.empty),
      Set.empty,
      Set.empty
    )
    val gcpSamResource = SamUserResource(
      UUID.randomUUID().toString,
      SamRolesAndActions(
        Set(SamBillingProjectRoles.owner),
        Set(SamBillingProjectActions.createWorkspace)
      ),
      SamRolesAndActions(Set.empty, Set.empty),
      SamRolesAndActions(Set.empty, Set.empty),
      Set.empty,
      Set.empty
    )

    val samUserResources = Seq(bpSamResource, gcpSamResource)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      samDAO,
      mock[BillingProfileManagerClientProvider],
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )

    val result = Await.result(
      billingProfileManagerDAO.listBillingProfiles(samUserResources, userInfo), Duration.Inf
    )

    val expected = Seq(
      RawlsBillingProject(
        RawlsBillingProjectName(azConfig.billingProjectName),
        CreationStatuses.Ready,
        None,
        None,
        azureManagedAppCoordinates = Some(
          AzureManagedAppCoordinates(
            UUID.fromString(azConfig.azureTenantId),
            UUID.fromString(azConfig.azureSubscriptionId),
            azConfig.azureResourceGroupId
          )
        )
      )
    )

    result should contain theSameElementsAs expected
  }

  it should "return no profiles if the user lacks permissions" in {
    val samDAO: SamDAO = mock[SamDAO]
    when(samDAO.userHasAction(SamResourceTypeNames.managedGroup, azConfig.alphaFeatureGroup,
      SamResourceAction("use"),
      userInfo
    )).thenReturn(Future.successful(false))
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      samDAO,
      mock[BillingProfileManagerClientProvider],
      new MultiCloudWorkspaceConfig(true, None,
        Some(azConfig)
      )
    )

    val result = Await.result(billingProfileManagerDAO.listBillingProfiles(Seq.empty, userInfo), Duration.Inf)

    result.isEmpty shouldBe true
  }

  it should "return no billing profiles if the feature flag is off" in {
    val samDAO: SamDAO = mock[SamDAO]
    val config = new MultiCloudWorkspaceConfig(false, None, None)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      samDAO,
      mock[BillingProfileManagerClientProvider],
      config
    )

    val result = Await.result(billingProfileManagerDAO.listBillingProfiles(Seq.empty, userInfo), Duration.Inf)

    result.isEmpty shouldBe true
  }

  it should "return no billing profiles if azure config is not set" in {
    val samDAO: SamDAO = mock[SamDAO]
    val config = new MultiCloudWorkspaceConfig(true, None, None)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      samDAO,
      mock[BillingProfileManagerClientProvider],
      config
    )

    val result = Await.result(billingProfileManagerDAO.listBillingProfiles(Seq.empty, userInfo), Duration.Inf)

    result.isEmpty shouldBe true
  }

}
