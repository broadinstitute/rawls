package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectPolicyNames,
  SamPolicy,
  SamPolicyWithNameAndEmail,
  SamResourcePolicyName,
  SamResourceTypeNames,
  SamWorkspacePolicyNames,
  UserInfo,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class RawlsWorkspaceAclManagerUnitTests extends AnyFlatSpec with MockitoTestUtils {
  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )
  val defaultWorkspaceName: WorkspaceName = WorkspaceName("fake_namespace", "fake_name")

  def rawlsWorkspaceAclManagerConstructor(samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)): WorkspaceAclManager =
    new RawlsWorkspaceAclManager(
      samDAO
    )(ExecutionContext.global)

  def verifyCorrectSamInteractions(policyAdditions: Set[(SamResourcePolicyName, String)], samDAO: SamDAO): Unit =
    policyAdditions.foreach {
      case (SamWorkspacePolicyNames.canCompute, email) =>
        verify(samDAO).addUserToPolicy(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(defaultWorkspaceName.namespace),
          ArgumentMatchers.eq(SamBillingProjectPolicyNames.canComputeUser),
          ArgumentMatchers.eq(email),
          any()
        )
      case (_, email) =>
        verify(samDAO, times(0)).addUserToPolicy(any(), any(), any(), ArgumentMatchers.eq(email), any())
    }

  "maybeShareWorkspaceNamespaceCompute" should "add new can-compute workspace users to the can-compute billing project policy" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.canCompute, "computer1@example.com"),
      (SamWorkspacePolicyNames.canCompute, "computer2@example.com"),
      (SamWorkspacePolicyNames.writer, "writer@example.com"),
      (SamWorkspacePolicyNames.owner, "owner@example.com"),
      (SamWorkspacePolicyNames.reader, "reader@example.com")
    )

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceAclManager = rawlsWorkspaceAclManagerConstructor(samDAO)
    Await.result(
      workspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                              defaultWorkspaceName,
                                                              defaultRequestContext
      ),
      5 seconds
    )

    verifyCorrectSamInteractions(policyAdditions, samDAO)
  }

  it should "tolerate the can-compute policy not existing on the billing project" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.canCompute, "computer@example.com")
    )

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any()))
      .thenReturn(Future.failed(new Exception("can-compute policy not found")))

    val workspaceAclManager = rawlsWorkspaceAclManagerConstructor(samDAO)
    Await.result(
      workspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions,
                                                              defaultWorkspaceName,
                                                              defaultRequestContext
      ),
      5 seconds
    )

    verifyCorrectSamInteractions(policyAdditions, samDAO)
  }

  "getWorkspacePolicies" should "return the members of all workspace policies except the can-catalog policy" in {
    val workspaceId = UUID.randomUUID
    val user = WorkbenchEmail("user@example.com")
    val catalogUser = WorkbenchEmail("catalogUser@example.com")
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val policies = Set(
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.owner,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.reader,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareReader,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareWriter,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCompute,
                                SamPolicy(Set(user), Set.empty, Set.empty),
                                WorkbenchEmail("")
      ),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCatalog,
                                SamPolicy(Set(catalogUser), Set.empty, Set.empty),
                                WorkbenchEmail("")
      )
    )
    when(samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceId.toString, defaultRequestContext))
      .thenReturn(Future.successful(policies))

    val aclManager = rawlsWorkspaceAclManagerConstructor(samDAO)
    val result = Await.result(aclManager.getWorkspacePolicies(workspaceId, defaultRequestContext), 5 seconds)

    val expected = Set(
      user -> SamWorkspacePolicyNames.projectOwner,
      user -> SamWorkspacePolicyNames.owner,
      user -> SamWorkspacePolicyNames.writer,
      user -> SamWorkspacePolicyNames.reader,
      user -> SamWorkspacePolicyNames.shareReader,
      user -> SamWorkspacePolicyNames.shareWriter,
      user -> SamWorkspacePolicyNames.canCompute
    )
    assertResult(expected)(result)
  }
}
