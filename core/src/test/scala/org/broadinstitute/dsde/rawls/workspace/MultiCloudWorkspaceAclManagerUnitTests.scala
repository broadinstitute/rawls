package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamWorkspacePolicyNames,
  UserInfo,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class MultiCloudWorkspaceAclManagerUnitTests extends AnyFlatSpec with MockitoTestUtils {

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val defaultWorkspaceName: WorkspaceName = WorkspaceName("fake_namespace", "fake_name")

  def multiCloudWorkspaceAclManagerConstructor(
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
    billingProfileManagerDAO: BillingProfileManagerDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS),
    dataSource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
  ): WorkspaceAclManager =
    new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)(
      ExecutionContext.global
    )

  "maybeShareWorkspaceNamespaceCompute" should "add new writers to the pet-creator billing profile policy" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.writer, "writer1@example.com"),
      (SamWorkspacePolicyNames.writer, "writer2@example.com"),
      (SamWorkspacePolicyNames.owner, "owner@example.com"),
      (SamWorkspacePolicyNames.reader, "reader@example.com")
    )
    val billingProfileId = UUID.randomUUID()

    val mockDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(mockDataSource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            RawlsBillingProjectName(defaultWorkspaceName.namespace),
            CreationStatuses.Ready,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            Option(billingProfileId.toString),
            None
          )
        )
      )
    )

    val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    doNothing()
      .when(mockBpmDAO)
      .addProfilePolicyMember(ArgumentMatchers.eq(billingProfileId),
                              ArgumentMatchers.eq(ProfilePolicy.PetCreator),
                              any(),
                              any()
      )

    val multiCloudWorkspaceAclManager =
      multiCloudWorkspaceAclManagerConstructor(billingProfileManagerDAO = mockBpmDAO, dataSource = mockDataSource)

    Await.result(
      multiCloudWorkspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                                        defaultWorkspaceName,
                                                                        defaultRequestContext
      ),
      5 seconds
    )

    policyAdditions.foreach {
      case (SamWorkspacePolicyNames.writer, email) =>
        verify(mockBpmDAO).addProfilePolicyMember(ArgumentMatchers.eq(billingProfileId),
                                                  ArgumentMatchers.eq(ProfilePolicy.PetCreator),
                                                  ArgumentMatchers.eq(email),
                                                  any()
        )
      case (_, email) =>
        verify(mockBpmDAO, times(0)).addProfilePolicyMember(any(), any(), ArgumentMatchers.eq(email), any())
    }
  }

  it should "throw if the workspace's billing project doesn't have a billing profile id" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.writer, "writer@example.com")
    )

    val mockDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(mockDataSource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            RawlsBillingProjectName(defaultWorkspaceName.namespace),
            CreationStatuses.Ready,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            None, /* billing_profile_id */
            None
          )
        )
      )
    )

    val mockBpmDao = mock[BillingProfileManagerDAO]

    val multiCloudWorkspaceAclManager =
      multiCloudWorkspaceAclManagerConstructor(billingProfileManagerDAO = mockBpmDao, dataSource = mockDataSource)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        multiCloudWorkspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                                          defaultWorkspaceName,
                                                                          defaultRequestContext
        ),
        5 seconds
      )
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
    verifyNoInteractions(mockBpmDao)
  }

  it should "throw if the workspace's billing project is missing" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.writer, "writer@example.com")
    )

    val mockDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(mockDataSource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        None
      )
    )

    val mockBpmDao = mock[BillingProfileManagerDAO]

    val multiCloudWorkspaceAclManager =
      multiCloudWorkspaceAclManagerConstructor(billingProfileManagerDAO = mockBpmDao, dataSource = mockDataSource)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        multiCloudWorkspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                                          defaultWorkspaceName,
                                                                          defaultRequestContext
        ),
        5 seconds
      )
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.InternalServerError)
    verifyNoInteractions(mockBpmDao)
  }
}
