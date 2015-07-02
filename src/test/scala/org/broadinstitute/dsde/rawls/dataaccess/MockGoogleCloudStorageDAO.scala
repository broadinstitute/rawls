package org.broadinstitute.dsde.rawls.dataaccess

object MockGoogleCloudStorageDAO extends GoogleCloudStorageDAO {
  override def getOurRedirectURI(callbackPath: String): String = "http://localhost.broadinstitute.org:8080/"+callbackPath

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = "https://google.com/other_stuff"

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {}

  override def createBucket(userId: String, projectId: String, bucketName: String): Unit = {}

  override def getACL(userId: String, bucketName: String): String = "{\"acl\": []}"

  override def putACL(userId: String, bucketName: String, acl: String): Unit = {}
}
