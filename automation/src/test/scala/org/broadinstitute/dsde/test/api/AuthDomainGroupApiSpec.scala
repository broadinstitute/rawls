package org.broadinstitute.dsde.test.api

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceResponse
import org.broadinstitute.dsde.test.util.AuthDomainMatcher
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.{billingScopes, serviceAccountScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture.{GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service._
import org.scalatest.CancelAfterFailure
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json._

@AuthDomainsTest
class AuthDomainGroupApiSpec
    extends AnyFreeSpec
    with Matchers
    with WorkspaceFixtures
    with GroupFixtures
    with Eventually
    with CancelAfterFailure {

  /*
   * Unless otherwise declared, this auth token will be used for API calls.
   */

  val defaultUser: Credentials = UserPool.chooseProjectOwner
  val defaultUserAuthToken: AuthToken = defaultUser.makeAuthToken()
  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  "A workspace" - {
    "with one group in its auth domain" - {

      "can be created" in {
        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = defaultUserAuthToken

        withGroup("authDomain", List(user.email)) { authDomainName =>
          withTemporaryBillingProject(billingAccountId, users = List(user.email).some) { projectName =>
            withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>
              // user is one of the authdomain group members
              groupNameToMembersEmails(authDomainName) should contain(user.email)
              // user can access workspace and see the authdomain group
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(authDomainName))(
                user.makeAuthToken()
              )

            }(user.makeAuthToken())
          }(defaultUser.makeAuthToken(billingScopes))
        }
      }

      "can be cloned and retain the auth domain" in {
        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = defaultUserAuthToken

        withGroup("authDomain", List(user.email)) { authDomainName =>
          withTemporaryBillingProject(billingAccountId, users = List(user.email).some) { projectName =>
            withWorkspace(projectName,
                          "AuthDomainGroupApiSpec_workspace",
                          Set(authDomainName),
                          List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
            ) { workspaceName =>
              val workspaceCloneName = workspaceName + "_clone"
              // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
              Orchestration.workspaces.clone(projectName,
                                             workspaceName,
                                             projectName,
                                             workspaceCloneName,
                                             Set(authDomainName)
              )(user.makeAuthToken())
              try {
                // the authdomain group should be found in cloned workspace
                val groups =
                  Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(user.makeAuthToken())
                groups should contain theSameElementsAs List(authDomainName)
              } finally
                Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
            }
          }(defaultUser.makeAuthToken(billingScopes))
        }
      }

      "when the user is not inside of the group" - {

        "when the workspace is shared with them" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = defaultUserAuthToken

            withGroup("authDomain") { authDomainName =>
              withTemporaryBillingProject(billingAccountId) { projectName =>
                withWorkspace(projectName,
                              "AuthDomainGroupApiSpec_workspace",
                              Set(authDomainName),
                              List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                ) { workspaceName =>
                  // user can see workspace but cannot access workspace
                  AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }(defaultUser.makeAuthToken(billingScopes))
            }
          }
        }

        "when the workspace is not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = defaultUserAuthToken

            withGroup("authDomain") { authDomainName =>
              withTemporaryBillingProject(billingAccountId) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>
                  // user cannot see workspace and user cannot access workspace
                  AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }(defaultUser.makeAuthToken(billingScopes))
            }
          }
        }
      }

      "when the user is inside of the group" - {

        "when the workspace is shared with them" - {
          "can be seen and is accessible" in {

            val user = UserPool.chooseAuthDomainUser
            implicit val authToken: AuthToken = defaultUserAuthToken

            withGroup("authDomain", List(user.email)) { authDomainName =>
              withTemporaryBillingProject(billingAccountId) { projectName =>
                withWorkspace(projectName,
                              "AuthDomainGroupApiSpec_workspace",
                              Set(authDomainName),
                              List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                ) { workspaceName =>
                  // user can see workspace and user can access workspace
                  AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(authDomainName))(
                    user.makeAuthToken()
                  )
                }
              }(defaultUser.makeAuthToken(billingScopes))
            }
          }
        }

        "when the workspace is not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseAuthDomainUser
            implicit val authToken: AuthToken = defaultUserAuthToken

            withGroup("AuthDomain", List(user.email)) { authDomainName =>
              withTemporaryBillingProject(billingAccountId) { projectName =>
                withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>
                  // user cannot see workspace and user cannot access workspace
                  AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                }
              }(defaultUser.makeAuthToken(billingScopes))
            }
          }
        }
      }

    }

    "bucket should not be accessible to project owners via projectViewer Google role" in {

      // It can take some time to propagate the permissions through Google's systems, so reconfigure the patience
      implicit val patienceConfig =
        PatienceConfig(timeout = scaled(Span(10, Minutes)), interval = scaled(Span(10, Seconds)))

      val userA = UserPool.chooseProjectOwner // The project owner who can't see the workspace
      val userB = UserPool.chooseAuthDomainUser // The user who owns the workspace

      val userAToken: AuthToken = userA.makeAuthToken(serviceAccountScopes)
      val userBToken: AuthToken = userB.makeAuthToken(serviceAccountScopes)

      withGroup("AuthDomain") { authDomainName =>
        withTemporaryBillingProject(billingAccountId, users = List(userB.email).some) { projectName =>
          withWorkspace(projectName, "AuthDomainGroupApiSpec_workspace", Set(authDomainName)) { workspaceName =>
            val bucketName = Rawls.workspaces
              .getWorkspaceDetails(projectName, workspaceName)(userBToken)
              .parseJson
              .convertTo[WorkspaceResponse]
              .workspace
              .bucketName

            eventually {
              // assert that userB receives 200 when trying to access bucket (to verify that bucket is set up correctly)
              Google.storage.getBucket(bucketName)(userBToken).status.intValue() should be(200)
            }

            eventually {
              // assert that userA receives 403 when trying to access bucket
              Google.storage.getBucket(bucketName)(userAToken).status.intValue() should be(403)
            }

          }(userBToken)
        }(userA.makeAuthToken(billingScopes))
      }(userBToken)
    }

  }

}
