package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.acl.AccessLevel
import org.broadinstitute.dsde.rawls.model.{GoogleBucketACLJsonSupport, BucketAccessControls, BucketAccessControl}
import GoogleBucketACLJsonSupport._
import spray.json._

object MockGoogleCloudStorageDAO extends GoogleCloudStorageDAO {
  override def getOurRedirectURI(callbackPath: String): String = "http://localhost.broadinstitute.org:8080/"+callbackPath

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = "https://google.com/other_stuff"

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {}

  override def createBucket(userId: String, projectId: String, bucketName: String): Unit = {}

  override def getACL(userId: String, bucketName: String): String = {
    val writerBAC = BucketAccessControl(
      kind = "dummy value",
      id = "dummy value",
      selfLink = "dummy value",
      bucket = "dummy value",
      entity = "dummy value",
      role = AccessLevel.Write,
      projectTeam = Map(),
      etag = "dummy value"
    )
    val readerBAC = BucketAccessControl(
      kind = "dummy value",
      id = "dummy value",
      selfLink = "dummy value",
      bucket = "dummy value",
      entity = "dummy value",
      role = AccessLevel.Read,
      projectTeam = Map(),
      etag = "dummy value"
    )
    val acl = BucketAccessControls(kind = "dummy value", items = Seq(writerBAC, readerBAC))

    acl.toJson.toString
  }

  override def putACL(userId: String, bucketName: String, acl: String): Unit = {}
}
