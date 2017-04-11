package org.broadinstitute.dsde.rawls.util

import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.MockGoogleServicesDAO
import org.broadinstitute.dsde.rawls.model.{AttributeName, UserInfo, Workspace, WorkspaceAccessLevels}
import org.scalatest.FreeSpecLike
import slick.dbio.{DBIO, SuccessAction}
import org.joda.time.DateTime
import spray.http.OAuth2BearerToken

import scala.concurrent.ExecutionContext

/**
  * Created by ahaessly on 3/31/17.
  */
class LibraryPermissionsSpec extends FreeSpecLike with LibraryPermissionsSupport {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  val userInfo: UserInfo = UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345")

  val discoverabilityOnly = Seq(discoverableWSAttribute)
  val publishName = Seq(publishedFlag)
  val publishNames = Seq(publishedFlag, AttributeName("library", "anothertest"))
  val modifyNames = Seq(AttributeName("library", "test2"), AttributeName("library", "anothertest"))
  val modifyName = Seq(AttributeName("library", "test2"))

  val testWorkspace = new Workspace(workspaceId=UUID.randomUUID().toString,
    namespace="testWorkspaceNamespace",
    name="testWorkspaceName",
    realm=None,
    isLocked=false,
    createdBy="createdBy",
    createdDate=DateTime.now(),
    lastModified=DateTime.now(),
    attributes=Map.empty,
    bucketName="bucketName",
    accessLevels=Map.empty,
    realmACLs=Map())


  "LibraryPermissionsSupport" - {
      "with catalog and read permissions" - {
        "should be allowed to modify workspace attributes" in {
          getPermissionChecker(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read) match {
            case fn: ChangeMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify workspace, one attribute" in {
          getPermissionChecker(modifyName, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read) match {
            case fn: ChangeMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify discoverability" in {
          getPermissionChecker(discoverabilityOnly ++ modifyName, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read) match {
            case fn: ChangeDiscoverabilityAndMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify publish as curator" in {
          getPermissionChecker(publishName, isCurator=true, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)match {
            case fn: ChangePublishedChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish with other attributes" in {
          intercept[RawlsException](getPermissionChecker(publishNames, isCurator=true, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)match {
            case _ => fail
          })
        }
      }
      "with write permissions" - {
        "should be allowed to modify workspace" in {
          getPermissionChecker(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)match {
            case fn: ChangeMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify discoverability" in {
          getPermissionChecker(discoverabilityOnly ++ modifyName, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)match {
            case fn: ChangeDiscoverabilityAndMetadataChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish as curator" in {
          getPermissionChecker(publishName, isCurator=true, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
      }
      "with owner permissions" - {
        "should be allowed to modify workspace" in {
          getPermissionChecker(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)match {
            case fn: ChangeMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify discoverability" in {
          getPermissionChecker(discoverabilityOnly ++ modifyName, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)match {
            case fn: ChangeDiscoverabilityAndMetadataChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=true, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)match {
            case fn: ChangePublishedChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
      }
      "with catalog only permissions" - {
        "should not be allowed to modify workspace" in {
          getPermissionChecker(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)match {
            case fn: ChangeMetadataChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify discoverability" in {
          getPermissionChecker(discoverabilityOnly, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)match {
            case fn: ChangeDiscoverabilityChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=true, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
      }
      "with read and share permissions" - {
        "should not be allowed to modify workspace" in {
          getPermissionChecker(modifyNames, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)match {
            case fn: ChangeMetadataChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should be allowed to modify discoverability" in {
          getPermissionChecker(discoverabilityOnly, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)match {
            case fn: ChangeDiscoverabilityChecker =>
              assertResult(SuccessAction(testWorkspace))(fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
        "should not be allowed to modify publish" in {
          getPermissionChecker(publishName, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)match {
            case fn: ChangePublishedChecker =>
              intercept[RawlsExceptionWithErrorReport](fn.withPermissions({DBIO.successful(testWorkspace)}))
            case _ => fail
          }
        }
      }
    }

}
