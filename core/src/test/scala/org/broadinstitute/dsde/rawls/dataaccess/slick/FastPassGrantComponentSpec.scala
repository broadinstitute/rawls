package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeNumber,
  AttributeString,
  FastPassGrant,
  GoogleProjectId,
  GoogleProjectNumber,
  RawlsBillingAccountName,
  Workspace,
  WorkspaceName,
  WorkspaceState,
  WorkspaceType,
  WorkspaceVersions
}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.{IamMemberTypes, IamResourceTypes}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.UUID

class FastPassGrantComponentSpec
    extends AnyFreeSpec
    with TestDriverComponent
    with Matchers
    with FastPassGrantComponent
    with RawlsTestUtils {

  val id = 0L
  val workspaceId = UUID.randomUUID()
  val expiration = OffsetDateTime.now(ZoneOffset.UTC).plusHours(2)
  val created = OffsetDateTime.now(ZoneOffset.UTC)

  val requesterPaysRole = "organizations/400176686919/roles/RequesterPays"
  val terraBucketReaderRole = "organizations/400176686919/roles/terraBucketReader"
  val terraBucketWriterRole = "organizations/400176686919/roles/terraBucketWriter"
  val terraBillingProjectOwnerRole = "organizations/400176686919/roles/terra_billing_project_owner"
  val terraWorkspaceCanComputeRole = "organizations/400176686919/roles/terra_workspace_can_compute"
  val terraWorkspaceNextflowRole = "organizations/400176686919/roles/terra_workspace_nextflow_role"

  val model = FastPassGrant(
    id,
    workspaceId.toString,
    WorkbenchUserId("12345678"),
    WorkbenchEmail("foo@bar.com"),
    IamMemberTypes.User,
    IamResourceTypes.Bucket,
    "my-bucket",
    terraBucketReaderRole,
    expiration.truncatedTo(ChronoUnit.MILLIS),
    created.truncatedTo(ChronoUnit.MILLIS)
  )

  val record = FastPassGrantRecord(
    id,
    workspaceId,
    "12345678",
    "foo@bar.com",
    "user",
    "bucket",
    "my-bucket",
    terraBucketReaderRole,
    new Timestamp(expiration.toInstant.toEpochMilli),
    new Timestamp(created.toInstant.toEpochMilli)
  )

  val googleProjectId: GoogleProjectId = GoogleProjectId("test_google_project")
  val googleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("123456789")
  val workspaceVersion: WorkspaceVersions.V2.type = WorkspaceVersions.V2
  val workspaceBillingAccount: RawlsBillingAccountName = RawlsBillingAccountName("billing_account_name")

  val workspace: Workspace = Workspace(
    "test_namespace",
    "test_name",
    workspaceId.toString,
    "bucketname",
    Some("workflow-collection"),
    currentTime(),
    currentTime(),
    "me",
    Map(
      AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
      AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
      AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)
    ),
    false,
    workspaceVersion,
    googleProjectId,
    Option(googleProjectNumber),
    Option(workspaceBillingAccount),
    None,
    Option(currentTime()),
    WorkspaceType.RawlsWorkspace,
    WorkspaceState.Ready
  )

  "FastPassGrantRecord" - {
    "Translates between FastPassGrant and FastPassGrantRecord" in {
      Seq(IamResourceTypes.Bucket, IamResourceTypes.Project).foreach { gcpResourceType =>
        val resourceTypeModel = model.copy(resourceType = gcpResourceType)
        val resourceTypeRecord = record.copy(resourceType = gcpResourceType.value)
        Seq(
          requesterPaysRole,
          terraBucketReaderRole,
          terraBucketWriterRole,
          terraBillingProjectOwnerRole,
          terraWorkspaceCanComputeRole,
          terraWorkspaceNextflowRole
        ).foreach { iamRole =>
          val testModel = resourceTypeModel.copy(organizationRole = iamRole)
          val testRecord = resourceTypeRecord.copy(roleName = iamRole)
          FastPassGrantRecord.fromFastPassGrant(testModel) shouldBe testRecord
          FastPassGrantRecord.toFastPassGrant(testRecord) shouldBe testModel

        }
      }
    }
    "CRUD Operations" - {
      "Does not find a non-existent FastPassGrant by ID" in {
        assertResult(None) {
          runAndWait(fastPassGrantQuery.findById(-1L))
        }
      }
      "Does not find a FastPassGrant for a non-existent user" in {
        assertResult(Seq.empty) {
          runAndWait(fastPassGrantQuery.findFastPassGrantsForUser(WorkbenchUserId("404")))
        }
      }
      "Does not find a FastPassGrant for a non-existent workspace" in {
        assertResult(Seq.empty) {
          runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(UUID.randomUUID()))
        }
      }
      "Does not find a FastPassGrant for a non-existent workspace and user" in {
        assertResult(Seq.empty) {
          runAndWait(
            fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(UUID.randomUUID(), WorkbenchUserId("404"))
          )
        }
      }

      "Inserts a FastPassGrant with a linked workspace" in {
        runAndWait(workspaceQuery.delete(WorkspaceName(workspace.namespace, workspace.name)))
        runAndWait(workspaceQuery.createOrUpdate(workspace))
        val id = runAndWait(fastPassGrantQuery.insert(model))
        assert(id > 0L)
        runAndWait(fastPassGrantQuery.findById(id)) shouldBe Some(model.copy(id = id))
      }

      "Deletes a FastPassGrant with a linked workspace" in {
        runAndWait(workspaceQuery.delete(WorkspaceName(workspace.namespace, workspace.name)))
        runAndWait(workspaceQuery.createOrUpdate(workspace))
        val id = runAndWait(fastPassGrantQuery.insert(model))
        runAndWait(fastPassGrantQuery.delete(id))
        runAndWait(fastPassGrantQuery.findById(id)) shouldBe None
        runAndWait(
          workspaceQuery.getV2WorkspaceId(WorkspaceName(workspace.namespace, workspace.name))
        ) should not be None
      }
    }
    "Expiration times in FastPassGrants" - {
      "Allow them to be found in the DB" in {
        runAndWait(workspaceQuery.delete(WorkspaceName(workspace.namespace, workspace.name)))
        runAndWait(workspaceQuery.createOrUpdate(workspace))

        val expiredGrant1 =
          model.copy(expiration = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(1).truncatedTo(ChronoUnit.MILLIS))
        val expiredGrant2 =
          model.copy(expiration = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(30).truncatedTo(ChronoUnit.MILLIS),
                     userSubjectId = WorkbenchUserId("a different user")
          )

        val expiredId1 = runAndWait(fastPassGrantQuery.insert(expiredGrant1))
        val expiredId2 = runAndWait(fastPassGrantQuery.insert(expiredGrant2))
        val nonExpiredId = runAndWait(fastPassGrantQuery.insert(model))

        val expiredGrants = runAndWait(fastPassGrantQuery.findExpiredFastPassGrants())
        expiredGrants should contain(expiredGrant1.copy(id = expiredId1))
        expiredGrants should contain(expiredGrant2.copy(id = expiredId2))
        expiredGrants should not contain model.copy(id = nonExpiredId)
      }
    }
  }

}
