package org.broadinstitute.dsde.rawls.dataaccess

trait GoogleCloudStorageDAO {
  def getOurRedirectURI(callbackPath: String): String

  def getGoogleRedirectURI(userId: String, callbackPath: String): String

  def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit

  def createBucket(userId: String, projectId: String, bucketName: String): Unit

  def deleteBucket(userId: String, projectId: String, bucketName: String): Unit

  def getACL(userId: String, bucketName: String): String

  def putACL(userId: String, bucketName: String, acl: String): Unit
}
