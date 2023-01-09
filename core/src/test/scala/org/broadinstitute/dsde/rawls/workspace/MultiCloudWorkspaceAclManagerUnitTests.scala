package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
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

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class MultiCloudWorkspaceAclManagerUnitTests extends AnyFlatSpec with MockitoTestUtils {

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

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
    val spendProfileId = UUID.randomUUID()

    val mockDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(mockDataSource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            RawlsBillingProjectName("fake-project"),
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
            Option(spendProfileId.toString),
            None
          )
        )
      )
    )

    val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    doNothing()
      .when(mockBpmDAO)
      .addProfilePolicyMember(ArgumentMatchers.eq(spendProfileId),
                              ArgumentMatchers.eq(ProfilePolicy.PetCreator),
                              any(),
                              any()
      )

    val multiCloudWorkspaceAclManager =
      multiCloudWorkspaceAclManagerConstructor(billingProfileManagerDAO = mockBpmDAO, dataSource = mockDataSource)

    Await.result(
      multiCloudWorkspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                                        WorkspaceName("doesntmatter", "dontcare"),
                                                                        defaultRequestContext
      ),
      5 seconds
    )

    policyAdditions.foreach {
      case (SamWorkspacePolicyNames.writer, email) =>
        verify(mockBpmDAO).addProfilePolicyMember(ArgumentMatchers.eq(spendProfileId),
                                                  ArgumentMatchers.eq(ProfilePolicy.PetCreator),
                                                  ArgumentMatchers.eq(email),
                                                  any()
        )
      case (_, email) =>
        verify(mockBpmDAO, times(0)).addProfilePolicyMember(any(), any(), ArgumentMatchers.eq(email), any())
    }
  }
}
