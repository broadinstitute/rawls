package org.broadinstitute.dsde.rawls.integrationtest

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleCloudStorageDAO
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceACLUpdate, WorkspaceAccessLevel, WorkspaceName}
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
  val testBucket = "rawls-gcs-test-" + System.currentTimeMillis().toString()
  val testWorkspace = WorkspaceName(testProject, testBucket)

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("sub_email").toString, OAuth2BearerToken("testtoken"), 123)
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123)

  override def beforeAll: Unit = {
    gcsDAO.createBucket(testCreator, testProject, testBucket)
    gcsDAO.setupACL(testCreator, testBucket, testWorkspace)
  }

  override def afterAll: Unit = {
    val storageCred = gcsDAO.getBucketServiceAccountCredential
    val storage = gcsDAO.getStorage(storageCred)

    // make sure groups are destroyed
    // (note that this is done already as part of the test, but just in case it fails prematurely...)
    try { gcsDAO.teardownACL(testBucket, testWorkspace) }
    catch { case e: GoogleJsonResponseException => /* do nothing */ }

    // delete bucket (can be done directly, since there's nothing in it)
    gcsDAO.deleteBucket(testCreator, testProject, testBucket)
  }

  "HttpGoogleCloudStorageDAO" should "do all of the things" in {
    val storageCred = gcsDAO.getBucketServiceAccountCredential
    val storage = gcsDAO.getStorage(storageCred)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = storage.buckets.get(testBucket).execute()

    val readerGroup = gcsDAO.makeGroupId(testWorkspace, WorkspaceAccessLevel.Read)
    val writerGroup = gcsDAO.makeGroupId(testWorkspace, WorkspaceAccessLevel.Write)
    val ownerGroup = gcsDAO.makeGroupId(testWorkspace, WorkspaceAccessLevel.Owner)

    // check that the access level for each group is what we expect
    val readerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(readerGroup)).execute()
    val writerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(writerGroup)).execute()
    val ownerBAC = storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(ownerGroup)).execute()

    readerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Read))
    writerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Write))
    ownerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Owner))

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val groupCred = gcsDAO.getGroupServiceAccountCredential
    val directory = gcsDAO.getGroupDirectory(groupCred)
    val readerResource = directory.groups.get(readerGroup).execute()
    val writerResource = directory.groups.get(writerGroup).execute()
    val ownerResource = directory.groups.get(ownerGroup).execute()

    // check that the creator is an owner, and that getACL is consistent
    gcsDAO.getMaximumAccessLevel(testCreator.userEmail, testWorkspace) should be (WorkspaceAccessLevel.Owner)
    gcsDAO.getACL(testBucket, testWorkspace).acl should be (Map(testCreator -> WorkspaceAccessLevel.Owner))

    // try adding a user, changing their access, then revoking it
    gcsDAO.updateACL(testBucket, testWorkspace, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Read)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspace) should be (WorkspaceAccessLevel.Read)
    gcsDAO.updateACL(testBucket, testWorkspace, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Write)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspace) should be (WorkspaceAccessLevel.Write)
    gcsDAO.updateACL(testBucket, testWorkspace, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.NoAccess)))
    gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspace) should be (WorkspaceAccessLevel.NoAccess)

    // check that we can properly deconstruct group names
    val groupName = gcsDAO.makeGroupId(testWorkspace, WorkspaceAccessLevel.Owner)
    gcsDAO.deconstructGroupId(groupName) should be (testWorkspace, WorkspaceAccessLevel.Owner)

    // tear down the ACL. confirm that the corresponding groups are deleted
    gcsDAO.teardownACL(testBucket, testWorkspace)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup).execute() }
  }
}