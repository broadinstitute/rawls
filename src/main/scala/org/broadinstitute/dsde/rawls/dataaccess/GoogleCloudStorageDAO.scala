package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceName

trait GoogleCloudStorageDAO {
  def getOurRedirectURI(callbackPath: String): String

  def getGoogleRedirectURI(userId: String, callbackPath: String): String

  def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit

  def createBucket(userId: String, projectId: String, bucketName: String): Unit

  def getACL(userId: String, bucketName: String): String

  def putACL(userId: String, bucketName: String, acl: String): Unit

  def createGoogleGroup(userId: String, accessLevel: String, workspaceName: WorkspaceName): Unit

  def setGroupACL(userId: String, accessLevel: String): Unit

  }
