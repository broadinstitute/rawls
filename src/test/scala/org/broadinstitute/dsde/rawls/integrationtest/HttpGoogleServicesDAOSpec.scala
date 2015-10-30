package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID
import akka.actor.ActorSystem
import org.joda.time.DateTime

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.util.Try
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.dataaccess.{Retry, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.http.OAuth2BearerToken

import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext

class HttpGoogleServicesDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with BeforeAndAfterAll with Retry {
  implicit val system = ActorSystem("HttpGoogleCloudStorageDAOSpec")
  val gcsDAO = new HttpGoogleServicesDAO(
    true, // use service account to manage buckets
    gcsConfig.getString("secrets"),
    gcsConfig.getString("pathToP12"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds"),
    "broad-dsde-dev"
  )

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testBucket = gcsDAO.getBucketName(testWorkspaceId)
  val testWorkspace = WorkspaceName(testProject, "someName")

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("client_email").toString, OAuth2BearerToken("testtoken"), 123, "123456789876543212345")
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123, "123456789876543212345")

  override def afterAll() = {
    Try(gcsDAO.deleteBucket(testCreator,testWorkspaceId)) // one last-gasp attempt at cleaning up
  }

  "HttpGoogleServicesDAO" should "do all of the things" in {
    Await.result(gcsDAO.createBucket(testCreator, testProject, testWorkspaceId, testWorkspace), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(() => Future { storage.buckets.get(testBucket).execute() }), Duration.Inf)

    val readerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Read)
    val writerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Write)
    val ownerGroup = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Owner)

    // check that the access level for each group is what we expect
    val readerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(readerGroup)).execute() }), Duration.Inf)
    val writerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(writerGroup)).execute() }), Duration.Inf)
    val ownerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(testBucket, gcsDAO.makeGroupEntityString(ownerGroup)).execute() }), Duration.Inf)

    readerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Read))
    writerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Write))
    ownerBAC.getRole should be (WorkspaceAccessLevel.toCanonicalString(WorkspaceAccessLevel.Owner))

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    val readerResource = Await.result(retry(when500)(() => Future { directory.groups.get(readerGroup).execute() }), Duration.Inf)
    val writerResource = Await.result(retry(when500)(() => Future { directory.groups.get(writerGroup).execute() }), Duration.Inf)
    val ownerResource = Await.result(retry(when500)(() => Future { directory.groups.get(ownerGroup).execute() }), Duration.Inf)

    // check that the creator is an owner, and that getACL is consistent
    Await.result(gcsDAO.getMaximumAccessLevel(testCreator.userEmail, testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevel.Owner)
    Await.result(gcsDAO.getACL(testWorkspaceId), Duration.Inf).acl should be (Map(testCreator.userEmail -> WorkspaceAccessLevel.Owner))

    // try adding a user, changing their access, then revoking it
    Await.result(gcsDAO.updateACL(testCreator.userEmail, testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Read))), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevel.Read)
    Await.result(gcsDAO.updateACL(testCreator.userEmail, testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.Write))), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevel.Write)
    Await.result(gcsDAO.updateACL(testCreator.userEmail, testWorkspaceId, Seq(WorkspaceACLUpdate(testCollaborator.userEmail, WorkspaceAccessLevel.NoAccess))), Duration.Inf)
    Await.result(gcsDAO.getMaximumAccessLevel(testCollaborator.userEmail, testWorkspaceId), Duration.Inf) should be (WorkspaceAccessLevel.NoAccess)

    // check that we can properly deconstruct group names
    val groupName = gcsDAO.toGroupId(testBucket, WorkspaceAccessLevel.Owner)
    gcsDAO.fromGroupId(groupName) should be (Some(WorkspacePermissionsPair(testWorkspaceId, WorkspaceAccessLevel.Owner)))

    // delete the bucket. confirm that the corresponding groups are deleted
    Await.result(gcsDAO.deleteBucket(testCreator, testWorkspaceId), Duration.Inf)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup).execute() }
  }

  it should "crud tokens" in {
    val userInfo = UserInfo(null, null, 0, UUID.randomUUID().toString)
    assertResult(None) { Await.result(gcsDAO.getToken(userInfo), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
    Await.result(gcsDAO.storeToken(userInfo, "testtoken"), Duration.Inf)
    assertResult(Some("testtoken")) { Await.result(gcsDAO.getToken(userInfo), Duration.Inf) }
    val storeTime = Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get

    Thread.sleep(100)

    Await.result(gcsDAO.storeToken(userInfo, "testtoken2"), Duration.Inf)
    assertResult(Some("testtoken2")) { Await.result(gcsDAO.getToken(userInfo), Duration.Inf) }
    assert(Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get.isAfter(storeTime))

    Await.result(gcsDAO.deleteToken(userInfo), Duration.Inf)
    assertResult(None) { Await.result(gcsDAO.getToken(userInfo), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case gjre: GoogleJsonResponseException => gjre.getStatusCode/100 == 5
      case _ => false
    }
  }
}