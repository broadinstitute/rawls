package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{WorkspaceName, GCSAccessLevel, BucketAccessControlJsonSupport}
import org.broadinstitute.dsde.rawls.mock.MockACLs
import BucketAccessControlJsonSupport._
import spray.json._

object MockGoogleCloudStorageDAO extends GoogleCloudStorageDAO {
  override def getOurRedirectURI(callbackPath: String): String = "http://localhost.broadinstitute.org:8080/"+callbackPath

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = "https://google.com/other_stuff"

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {}

  override def createBucket(userId: String, projectId: String, bucketName: String): Unit = {}

  val acls = Map(
    // the default test user should have access to everything
    "test@broadinstitute.org" -> MockACLs.bacsForLevel(GCSAccessLevel.Owner),
    "test_token" -> MockACLs.bacsForLevel(GCSAccessLevel.Owner),
    "owner-access" -> MockACLs.bacsForLevel(GCSAccessLevel.Owner),
    "write-access" -> MockACLs.bacsForLevel(GCSAccessLevel.Write),
    "read-access" -> MockACLs.bacsForLevel(GCSAccessLevel.Read),
    "no-access" -> MockACLs.bacsForLevel(GCSAccessLevel.NoAccess)
 )

  override def getACL(userId: String, bucketName: String): String = {
    val acl = acls get userId orElse {
      val errorMsg = "Need to add %s to MockGoogleCloudStorageDAO.acls map".format(userId)
      println(errorMsg)
      throw new RuntimeException(errorMsg)
    }

    acl.toJson.toString
  }

  override def putACL(userId: String, bucketName: String, acl: String): Unit = {}

  override def createGoogleGroup(userId: String, accessLevel: String, workspaceName: WorkspaceName): String = {
    return ""
  }

  override def setGroupACL(userId: String, accessLevel: String, createRequest: String): Unit = {}

}
