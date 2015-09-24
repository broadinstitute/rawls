package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID
import scala.util.Try
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleCloudStorageDAO
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.http.OAuth2BearerToken

class HttpGoogleCloudStorageDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with BeforeAndAfterAll {
  val gcsDAO = new HttpGoogleCloudStorageDAO(
    true, // use service account to manage buckets
    gcsConfig.getString("secrets"),
    gcsConfig.getString("pathToP12"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds")
  )

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testBucket = gcsDAO.getBucketName(testWorkspaceId)
  val testWorkspace = WorkspaceName(testProject, "someName")

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("sub_email").toString, OAuth2BearerToken("testtoken"), 123)
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123)

  override def afterAll() = {
    Try(gcsDAO.deleteBucket(testCreator,testWorkspaceId)) // one last-gasp attempt at cleaning up
  }

  "HttpGoogleCloudStorageDAO" should "do all of the things" in {
    gcsDAO.createBucket(testCreator, testProject, testWorkspaceId, testWorkspace)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = storage.buckets.get(testBucket).execute()

    val readerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Read)
    val writerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Write)
    val ownerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Owner)

    // check that the access level for each group is what we expect
    val readerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(readerGroup)).execute()
    val writerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(writerGroup)).execute()
    val ownerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(ownerGroup)).execute()

    readerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Read))
    writerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Write))
    ownerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Owner))

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    val readerResource = directory.groups.get(readerGroup).execute()
    val writerResource = directory.groups.get(writerGroup).execute()
    val ownerResource = directory.groups.get(ownerGroup).execute()

    // check that the creator is an owner, and that getACL is consistent
    gcsDAO.getMaximumAccessLevel(testCreator.userEmail, testWorkspaceId) should be (WorkspaceAccessLevel.Owner)
    gcsDAO.getACL(testWorkspaceId).acl should be (Map(testCreator -> WorkspaceAccessLevel.Owner))

    // try adding a user, changing their access, then revoking it
    gcsDAO.updateACL(testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Read)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId) should be (WorkspaceAccessLevel.Read)
    gcsDAO.updateACL(testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Write)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId) should be (WorkspaceAccessLevel.Write)
    gcsDAO.updateACL(testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.NoAccess)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId) should be (WorkspaceAccessLevel.NoAccess)

    // check that we can properly deconstruct group names
    val groupName = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Owner)
    gcsDAO.fromGroupId(groupName) should be (Some(WorkspacePermissionsPair(testWorkspaceId, WorkspaceAccessLevel.Owner)))

    // delete the bucket. confirm that the corresponding groups are deleted
    gcsDAO.deleteBucket(testCreator, testWorkspaceId)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup).execute() }
  }
}