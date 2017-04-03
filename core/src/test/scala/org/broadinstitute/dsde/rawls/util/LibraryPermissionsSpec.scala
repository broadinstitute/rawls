package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.MockGoogleServicesDAO
import org.broadinstitute.dsde.rawls.model.{AttributeName, UserInfo, WorkspaceAccessLevels}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.scalatest.FreeSpecLike
import spray.http.{OAuth2BearerToken, StatusCodes}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by ahaessly on 3/31/17.
  */
class LibraryPermissionsSpec extends FreeSpecLike with LibraryPermissionsSupport {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  val userInfo: UserInfo = UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345")

  val discoverabilityOnly = Seq(discoverableWSAttribute)
  val discoverabilityAndModNames = Seq(AttributeName("library","test1"), discoverableWSAttribute)
  val publishNames = Seq(publishedFlag)
  val modifyNames = Seq(AttributeName("library", "test2"))

    "LibraryPermissionsSupport" - {
      "with catalog and read permissions" - {
        "should be allowed to modify workspace" in {
          val mod = getPermissionFunction(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$3")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should be allowed to modify discoverability" in {
          val mod = getPermissionFunction(discoverabilityAndModNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$4")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should be allowed to modify publish as curator" in {
          val mod = getPermissionFunction(publishNames, isCurator=true, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
      }
      "with write permissions" - {
        "should be allowed to modify workspace" in {
          val mod = getPermissionFunction(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$3")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify discoverability" in {
          val mod = getPermissionFunction(discoverabilityAndModNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$4")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish as curator" in {
          val mod = getPermissionFunction(publishNames, isCurator=true, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Write)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
      }
      "with owner permissions" - {
        "should be allowed to modify workspace" in {
          val mod = getPermissionFunction(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$3")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should be allowed to modify discoverability" in {
          val mod = getPermissionFunction(discoverabilityAndModNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$4")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=true, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=false, canShare=false, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Owner)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
      }
      "with catalog only permissions" - {
        "should not be allowed to modify workspace" in {
          val mod = getPermissionFunction(modifyNames, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$3")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify discoverability" in {
          val mod = getPermissionFunction(discoverabilityOnly, isCurator=false, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$2")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=true, canShare=false, hasCatalogOnly=true, userLevel=WorkspaceAccessLevels.NoAccess)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
      }
      "with read and share permissions" - {
        "should not be allowed to modify workspace" in {
          val mod = getPermissionFunction(modifyNames, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$3")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should be allowed to modify discoverability" in {
          val mod = getPermissionFunction(discoverabilityOnly, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$2")(mod.getClass.getName)
          assertResult(RequestComplete(StatusCodes.OK))(Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
        "should not be allowed to modify publish" in {
          val mod = getPermissionFunction(publishNames, isCurator=false, canShare=true, hasCatalogOnly=false, userLevel=WorkspaceAccessLevels.Read)
          assertResult("org.broadinstitute.dsde.rawls.util.LibraryPermissionsSupport$$anonfun$getPermissionFunction$1")(mod.getClass.getName)
          intercept[RawlsExceptionWithErrorReport](Await.result(mod({Future.successful(RequestComplete(StatusCodes.OK))}), Duration.Inf))
        }
      }
    }

}
