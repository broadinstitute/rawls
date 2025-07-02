package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.headers.OAuth2BearerToken
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
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
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
    dataSource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
  ): WorkspaceAclManager =
    new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, dataSource)(
      ExecutionContext.global
    )

  "maybeShareWorkspaceNamespaceCompute" should "add new writers and owners to the pet-creator billing profile policy" in {
    val policyAdditions = Set(
      (SamWorkspacePolicyNames.writer, "writer1@example.com"),
      (SamWorkspacePolicyNames.writer, "writer2@example.com"),
      (SamWorkspacePolicyNames.owner, "owner@example.com"),
      (SamWorkspacePolicyNames.reader, "reader@example.com")
    )
    val billingProfileId = UUID.randomUUID()

    val mockDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(mockDataSource.inTransaction[Option[RawlsBillingProject]](any(), any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            UUID.randomUUID(),
            RawlsBillingProjectName(defaultWorkspaceName.namespace),
            CreationStatuses.Ready,
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
  }
}
