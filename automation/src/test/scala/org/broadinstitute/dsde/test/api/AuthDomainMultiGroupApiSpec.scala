package org.broadinstitute.dsde.test.api

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.test.util.AuthDomainMatcher
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture.{GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.{AclEntry, Orchestration, Rawls, WorkspaceAccessLevel}
import org.scalatest.CancelAfterFailure
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import spray.json.{JsValue, JsonParser}

@AuthDomainsTest
class AuthDomainMultiGroupApiSpec
    extends AnyFreeSpec
    with Matchers
    with WorkspaceFixtures
    with GroupFixtures
    with Eventually
    with CancelAfterFailure {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(150, Seconds)), interval = scaled(Span(10, Seconds)))

  /*
   * Unless otherwise declared, this auth token will be used for API calls.
   * We are using a curator to prevent collisions with users in tests (who are Students and AuthDomainUsers), not
   *  because we specifically need a curator.
   */

  val defaultUser: Credentials = UserPool.chooseCurator
  val authTokenDefault: AuthToken = defaultUser.makeAuthToken()
  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  "A workspace" - {
    "with multiple groups in its auth domain" - {

      "can be created" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
            withTemporaryBillingProject(billingAccountId, users = List(user.email).some) { projectName =>
              withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                // user is in members emails in two authdomain groups
                groupNameToMembersEmails(groupOne) should contain(user.email)
                groupNameToMembersEmails(groupTwo) should contain(user.email)

                // user can access workspace and see two authdomain groups
                AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(
                  user.makeAuthToken()
                )

              }(user.makeAuthToken())
            }(UserPool.chooseProjectOwner.makeAuthToken(billingScopes))
          }
        }
      }

      "can be cloned and retain the auth domain" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
            withTemporaryBillingProject(billingAccountId, users = List(user, defaultUser).map(_.email).some) {
              projectName =>
                val authDomains = Set(groupOne, groupTwo)
                val acl = List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                withWorkspace(projectName, "GroupsApiSpec_workspace", authDomains, acl) { workspaceName =>
                  val workspaceCloneName = workspaceName + "_clone"
                  // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
                  Orchestration.workspaces.clone(projectName,
                                                 workspaceName,
                                                 projectName,
                                                 workspaceCloneName,
                                                 authDomains
                  )(user.makeAuthToken())
                  try {
                    // two authdomain groups should be in cloned workspace
                    val groups =
                      Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(user.makeAuthToken())
                    groups should contain theSameElementsAs List(groupOne, groupTwo)
                  } finally
                    Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
                }
            }(UserPool.chooseProjectOwner.makeAuthToken(billingScopes))
          }
        }
      }

      "can be cloned and have a group added to the auth domain" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
            withGroup("AuthDomainThree", List(user.email)) { groupThree =>
              withTemporaryBillingProject(billingAccountId, users = List(user, defaultUser).map(_.email).some) {
                projectName =>
                  val authDomain = Set(groupOne, groupTwo)
                  val acl = List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                  withWorkspace(projectName, "GroupsApiSpec_workspace", authDomain, acl) { workspaceName =>
                    val workspaceCloneName = workspaceName + "_clone"
                    val newAuthDomain = authDomain + groupThree
                    // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
                    Orchestration.workspaces.clone(projectName,
                                                   workspaceName,
                                                   projectName,
                                                   workspaceCloneName,
                                                   newAuthDomain
                    )(user.makeAuthToken())
                    try {
                      // verify three authdomain groups are found in cloned workspace
                      val groups = Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(
                        user.makeAuthToken()
                      )
                      groups should contain theSameElementsAs List(groupOne, groupTwo, groupThree)
                    } finally
                      Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
                  }
              }(UserPool.chooseProjectOwner.makeAuthToken(billingScopes))
            }
          }
        }
      }

      // no groups
      "when the user is in none of the groups" - {
        "when shared with them" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>
                withTemporaryBillingProject(billingAccountId, users = List(defaultUser.email).some) { projectName =>
                  val authDomain = Set(groupOne, groupTwo)
                  withWorkspace(projectName,
                                "GroupsApiSpec_workspace",
                                authDomain,
                                List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                  ) { workspaceName =>
                    // user can see workspace but user cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }(UserPool.chooseProjectOwner.makeAuthToken(billingScopes))
              }
            }
          }
        }

        "when the user is a billing project owner" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseProjectOwner
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>
                withTemporaryBillingProject(billingAccountId, users = List(defaultUser.email).some) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                    // user can see workspace but user cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }(user.makeAuthToken(billingScopes))
              }
            }
          }
        }

        "when not shared with them" - {
          "cannot be seen and is not accessible" in {
            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>
                withTemporaryBillingProject(billingAccountId, users = List(defaultUser.email).some) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                    // user cannot see workspace and user cannot access workspace
                    AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }(UserPool.chooseProjectOwner.makeAuthToken(billingScopes))
              }
            }
          }
        }
      }

      // one group
      "when the user is in one of the groups" - {
        "when shared with them" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName,
                                "GroupsApiSpec_workspace",
                                Set(groupOne, groupTwo),
                                List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                  ) { workspaceName =>
                    // user can see workspace but user cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }
              }
            }
          }

          "when the user is a billing project owner" - {
            "can be seen but is not accessible" in {

              val user = UserPool.chooseProjectOwner

              withGroup("AuthDomainOne") { groupOne =>
                withTemporaryBillingProject(billingAccountId, owners = List(defaultUser.email).some) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne)) { workspaceName =>
                    // default user can see workspace but cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(defaultUser.makeAuthToken())
                  }(user.makeAuthToken())
                }(user.makeAuthToken(billingScopes))
              }(user.makeAuthToken())
            }
          }
        }

        "when not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                    // user cannot see workspace and user cannot access workspace
                    AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }
              }
            }
          }
        }
      }

      // in all groups
      "when the user is in all of the groups" - {
        "when shared with them" - {
          "can be seen and is accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName,
                                "GroupsApiSpec_workspace",
                                Set(groupOne, groupTwo),
                                List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                  ) { workspaceName =>
                    // user can see workspace and user can access workspace
                    AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(
                      user.makeAuthToken()
                    )
                  }
                }
              }
            }
          }

          "and given writer access" - {
            "the user has correct permissions" in {

              val user = UserPool.chooseStudent
              implicit val authToken: AuthToken =
                UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

              withGroup("AuthDomainOne", List(user.email)) { groupOne =>
                withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                  withTemporaryBillingProject(billingAccountId) { projectName =>
                    withWorkspace(projectName,
                                  "GroupsApiSpec_workspace",
                                  Set(groupOne, groupTwo),
                                  List(AclEntry(user.email, WorkspaceAccessLevel.Writer))
                    ) { workspaceName =>
                      eventually {
                        val level = getWorkspaceAccessLevel(projectName, workspaceName)(user.makeAuthToken())
                        level should (be("WRITER"))
                      }
                      AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(
                        user.makeAuthToken()
                      )
                    }
                  }
                }
              }
            }
          }

          "when the user is a billing project owner" - {
            "can be seen and is accessible" in {

              val user = UserPool.chooseProjectOwner
              implicit val authToken: AuthToken = user.makeAuthToken()

              withGroup("AuthDomainOne", List(user.email)) { groupOne =>
                withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                  withTemporaryBillingProject(billingAccountId) { projectName =>
                    withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                      // user can see workspace and user can access workspace
                      AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(
                        user.makeAuthToken()
                      )
                    }
                  }(user.makeAuthToken(billingScopes))
                }
              }
            }
          }
        }

        "when shared with one of the groups in the auth domain" - {
          "can be seen and is accessible by group member who is a member of both auth domain groups" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName,
                                "GroupsApiSpec_workspace",
                                Set(groupOne, groupTwo),
                                List(AclEntry(groupNameToEmail(groupOne), WorkspaceAccessLevel.Reader))
                  ) { workspaceName =>
                    // user can see workspace and user can access workspace
                    AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(
                      user.makeAuthToken()
                    )
                  }
                }
              }
            }
          }

          "can be seen but is not accessible by group member who is a member of only one auth domain group" in {
            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName,
                                "GroupsApiSpec_workspace",
                                Set(groupOne, groupTwo),
                                List(AclEntry(groupNameToEmail(groupOne), WorkspaceAccessLevel.Reader))
                  ) { workspaceName =>
                    // user can see workspace but user cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }
              }
            }
          }
        }

        "when not shared with them" - {
          "cannot be seen and is not accessible" in {
            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken =
              UserPool.chooseProjectOwner.makeAuthToken(billingScopes)

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                withTemporaryBillingProject(billingAccountId) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>
                    // user cannot see workspace and user cannot access workspace
                    AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }
              }
            }
          }
        }

      } // End of in all groups
    }

  }

  private def getWorkspaceAccessLevel(projectName: String, workspaceName: String)(implicit token: AuthToken): String = {
    import spray.json.DefaultJsonProtocol._
    val response = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)
    val json: JsValue = JsonParser(response)
    val field: JsValue = json.asJsObject.fields("accessLevel")
    field.convertTo[String]
  }

}
