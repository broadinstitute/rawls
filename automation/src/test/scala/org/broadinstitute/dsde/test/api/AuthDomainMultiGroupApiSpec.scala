package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.{AclEntry, Orchestration, Rawls, WorkspaceAccessLevel}
import org.broadinstitute.dsde.workbench.service.test.AuthDomainMatcher
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import spray.json.{JsValue, JsonParser}

import scala.util.Try


class AuthDomainMultiGroupApiSpec extends FreeSpec with Matchers with WorkspaceFixtures with BillingFixtures
  with GroupFixtures with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(150, Seconds)), interval = scaled(Span(10, Seconds)))

  /*
   * Unless otherwise declared, this auth token will be used for API calls.
   * We are using a curator to prevent collisions with users in tests (who are Students and AuthDomainUsers), not
   *  because we specifically need a curator.
   */

  val defaultUser: Credentials = UserPool.chooseCurator
  val authTokenDefault: AuthToken = defaultUser.makeAuthToken()


  "A workspace" - {
    "with multiple groups in its auth domain" - {

      "can be created" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

            withCleanBillingProject(user) { projectName =>
              withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>

                // user is in members emails in two authdomain groups
                groupNameToMembersEmails(groupOne) should contain (user.email)
                groupNameToMembersEmails(groupTwo) should contain (user.email)

                // user can access workspace and see two authdomain groups
                AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(user.makeAuthToken())

              }(user.makeAuthToken())
            }
          }
        }
      }

      "can be cloned and retain the auth domain" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

            withCleanBillingProject(defaultUser) { projectName =>
              Rawls.billing.addUserToBillingProject(projectName, user.email, BillingProjectRole.User)

              val authDomains = Set(groupOne, groupTwo)
              val acl = List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
              withWorkspace(projectName, "GroupsApiSpec_workspace", authDomains, acl) { workspaceName =>

                val workspaceCloneName = workspaceName + "_clone"
                register cleanUp Try(Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())).recover({
                  case _: Exception =>
                })
                // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
                Orchestration.workspaces.clone(projectName, workspaceName, projectName, workspaceCloneName, authDomains)(user.makeAuthToken())

                // two authdomain groups should be in cloned workspace
                val groups = Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(user.makeAuthToken())
                groups should contain theSameElementsAs List(groupOne, groupTwo)

                Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
              }
            }
          }
        }
      }


      "can be cloned and have a group added to the auth domain" in {

        val user = UserPool.chooseAuthDomainUser
        implicit val authToken: AuthToken = authTokenDefault

        withGroup("AuthDomainOne", List(user.email)) { groupOne =>
          withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
            withGroup("AuthDomainThree", List(user.email)) { groupThree =>

              withCleanBillingProject(defaultUser) { projectName =>
                Rawls.billing.addUserToBillingProject(projectName, user.email, BillingProjectRole.User)

                val authDomain = Set(groupOne, groupTwo)
                val acl = List(AclEntry(user.email, WorkspaceAccessLevel.Reader))
                withWorkspace(projectName, "GroupsApiSpec_workspace", authDomain, acl) { workspaceName =>

                  val workspaceCloneName = workspaceName + "_clone"
                  register cleanUp Try(Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())).recover({
                    case _: Exception =>
                  })

                  val newAuthDomain = authDomain + groupThree
                  // Note: this is not a passthrough to Rawls is because library needs to overwrite any publish and discoverableByGroups values
                  Orchestration.workspaces.clone(projectName, workspaceName, projectName, workspaceCloneName, newAuthDomain)(user.makeAuthToken())
                  // verify three authdomain groups are found in cloned workspace
                  val groups = Rawls.workspaces.getAuthDomainsInWorkspace(projectName, workspaceCloneName)(user.makeAuthToken())
                  groups should contain theSameElementsAs List(groupOne, groupTwo, groupThree)

                  Rawls.workspaces.delete(projectName, workspaceCloneName)(user.makeAuthToken())
                }
              }
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

                withCleanBillingProject(defaultUser) { projectName =>

                  val authDomain = Set(groupOne, groupTwo)
                  withWorkspace(projectName, "GroupsApiSpec_workspace", authDomain, List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

                    // user can see workspace but user cannot access workspace
                    AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(user.makeAuthToken())
                  }
                }
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

                withCleanBillingProject(user, userEmails = List(defaultUser.email)) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>

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
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>
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

      // one group
      "when the user is in one of the groups" - {
        "when shared with them" - {
          "can be seen but is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>

                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo), List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

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
              implicit val authToken: AuthToken = authTokenDefault

              withGroup("AuthDomainOne") { groupOne =>
                withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>
                  withCleanBillingProject(defaultUser, List(user.email)) { projectName =>
                    withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne)) { workspaceName =>

                      // user can see workspace but user cannot access workspace
                      AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(defaultUser.makeAuthToken())

                    }(user.makeAuthToken())
                  }
                }(user.makeAuthToken())
              }(user.makeAuthToken())
            }
          }
        }

        "when not shared with them" - {
          "cannot be seen and is not accessible" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne") { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>
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
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo), List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

                    // user can see workspace and user can access workspace
                    AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(user.makeAuthToken())
                  }
                }
              }
            }
          }

          "and given writer access" - {
            "the user has correct permissions" in {

              val user = UserPool.chooseStudent
              implicit val authToken: AuthToken = authTokenDefault

              withGroup("AuthDomainOne", List(user.email)) { groupOne =>
                withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                  withCleanBillingProject(defaultUser) { projectName =>
                    withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo), List(AclEntry(user.email, WorkspaceAccessLevel.Writer))) { workspaceName =>
                      eventually {
                        val level = getWorkspaceAccessLevel(projectName, workspaceName)(user.makeAuthToken())
                        level should (be("WRITER"))
                      }
                      AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(user.makeAuthToken())
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

                  withCleanBillingProject(user) { projectName =>
                    withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo)) { workspaceName =>

                      // user can see workspace and user can access workspace
                      AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(user.makeAuthToken())
                    }
                  }
                }
              }
            }
          }
        }

        "when shared with one of the groups in the auth domain" - {
          "can be seen and is accessible by group member who is a member of both auth domain groups" in {

            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo), List(AclEntry(groupNameToEmail(groupOne), WorkspaceAccessLevel.Reader))) { workspaceName =>

                    // user can see workspace and user can access workspace
                    AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupOne, groupTwo))(user.makeAuthToken())
                  }
                }
              }
            }
          }

          "can be seen but is not accessible by group member who is a member of only one auth domain group" in {
            val user = UserPool.chooseStudent
            implicit val authToken: AuthToken = authTokenDefault
            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo") { groupTwo =>
                withCleanBillingProject(defaultUser) { projectName =>
                  withWorkspace(projectName, "GroupsApiSpec_workspace", Set(groupOne, groupTwo), List(AclEntry(groupNameToEmail(groupOne), WorkspaceAccessLevel.Reader))) { workspaceName =>

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
            implicit val authToken: AuthToken = authTokenDefault

            withGroup("AuthDomainOne", List(user.email)) { groupOne =>
              withGroup("AuthDomainTwo", List(user.email)) { groupTwo =>

                withCleanBillingProject(defaultUser) { projectName =>
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
