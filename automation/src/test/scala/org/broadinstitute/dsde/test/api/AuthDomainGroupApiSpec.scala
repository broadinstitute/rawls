package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceResponse
import org.broadinstitute.dsde.test.util.AuthDomainMatcher
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.serviceAccountScopes
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.Waiters.scaled
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json._

import scala.util.Try

class AuthDomainGroupApiSpec extends AnyFreeSpec with Matchers with WorkspaceFixtures with BillingFixtures with GroupFixtures with Eventually {

  /*
  * Unless otherwise declared, this auth token will be used for API calls.
  * We are using a curator to prevent collisions with users in tests (who are Students and AuthDomainUsers), not
  *  because we specifically need a curator.
  */

  val defaultUser: Credentials = UserPool.chooseCurator
  val authTokenDefault: AuthToken = defaultUser.makeAuthToken()
  
  "A workspace" - {
    "with one group in its auth domain" - {

      "can be created" in {
        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("authDomain", List(user.email)) { authDomainName =>
          withCleanBillingProject(user) { projectName =>
            withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>

              // user is one of the authdomain group members
              groupNameToMembersEmails(authDomainName) should contain (user.email)
              // user can access workspace and see the authdomain group
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(authDomainName))(user.makeAuthToken())

            }(user.makeAuthToken())
          }
        }
      }

      "can be cloned and retain the auth domain" in {
        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("authDomain", List(user.email)) { authDomainName =>
          withCleanBillingProject(defaultUser, userEmails = List(user.email)) { projectName =>
            withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName),
              List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

              val workspaceCloneName = workspaceName + "_clone"
              register cleanUp Try(Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())).recover({
                case _: Exception =>
              })
              // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
              Orchestration.workspaces.clone(projectName, workspaceName, projectName, workspaceCloneName, Set(authDomainName))(user.makeAuthToken())

              // the authdomain group should be found in cloned workspace
              val groups = Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(user.makeAuthToken())
              groups should contain theSameElementsAs List(authDomainName)

              Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
            }
          }
        }
      }


      "when the user is not inside of the group" - {

        "when the workspace is shared with them" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("authDomain") { authDomainName =>
              withCleanBillingProject(defaultUser) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName),
                  List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

                  // user can see workspace but cannot access workspace
                  AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }
            }
          }
        }

        "when the workspace is not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("authDomain") { authDomainName =>
              withCleanBillingProject(defaultUser) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>

                  // user cannot see workspace and user cannot access workspace
                  AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }
            }
          }
        }
      }

      "when the user is inside of the group" - {

        "when the workspace is shared with them" - {
          "can be seen and is accessible" in {

            val user = UserPool.chooseAuthDomainUser
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("authDomain", List(user.email)) { authDomainName =>
              withCleanBillingProject(defaultUser) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName),
                  List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

                  // user can see workspace and user can access workspace
                  AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(authDomainName))(user.makeAuthToken())
                }
              }
            }
          }
        }

        "when the workspace is not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseAuthDomainUser
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomain", List(user.email)) { authDomainName =>
              withCleanBillingProject(defaultUser) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>

                  // user cannot see workspace and user cannot access workspace
                  AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }
            }
          }
        }
      }

    }

    "bucket should not be accessible to project owners via projectViewer Google role" in {

      //It can take some time to propagate the permissions through Google's systems, so reconfigure the patience
      implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Minutes)), interval = scaled(Span(10, Seconds)))

      val userA = UserPool.chooseProjectOwner //The project owner who can't see the workspace
      val userB = UserPool.chooseAuthDomainUser //The user who owns the workspace

      val userAToken: AuthToken = userA.makeAuthToken(serviceAccountScopes)
      val userBToken: AuthToken = userB.makeAuthToken(serviceAccountScopes)

      withGroup("AuthDomain") { authDomainName =>
        withCleanBillingProject(userA, List(userA.email), List(userB.email)) { projectName =>
          withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>

            val bucketName = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(userBToken).parseJson.convertTo[WorkspaceResponse].workspace.bucketName

            eventually {
              //assert that userB receives 200 when trying to access bucket (to verify that bucket is set up correctly)
              Google.storage.getBucket(bucketName)(userBToken).status.intValue() should be(200)
            }

            eventually {
              //assert that userA receives 403 when trying to access bucket
              Google.storage.getBucket(bucketName)(userAToken).status.intValue() should be(403)
            }

          }(userBToken)
        }
      }(userBToken)
    }

  }

}
