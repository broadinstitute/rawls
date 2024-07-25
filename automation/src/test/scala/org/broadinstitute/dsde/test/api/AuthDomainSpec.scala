package org.broadinstitute.dsde.test.api

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceResponse
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.{billingScopes, serviceAccountScopes}
import org.broadinstitute.dsde.workbench.config.{ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture.{GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.Orchestration.groups.GroupRole
import org.broadinstitute.dsde.workbench.service.{AclEntry, Google, Orchestration, Rawls, RestException, WorkspaceAccessLevel}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json._


@AuthDomainsTest
class AuthDomainSpec extends AnyFlatSpec with Matchers with WorkspaceFixtures with GroupFixtures with Eventually {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(150, Seconds)), interval = scaled(Span(2, Seconds)))

  lazy val projectOwner = UserPool.chooseProjectOwner
  lazy val (projectUser, groupOwner) = {
    val users = UserPool.chooseStudents(2)
    (users(0), users(1))
  }

  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  "AuthDomains" should "create and access a workspace with an auth domain" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email, projectOwner.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email, projectOwner.email)) { realmGroup2 =>
        withGroup("ad3", List(projectUser.email, projectOwner.email)) { realmGroup3 =>
          withTemporaryBillingProject(billingAccountId) { projectName =>
            withWorkspace(projectName,
                          "AuthDomains",
                          Set(realmGroup, realmGroup2, realmGroup3),
                          List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))
            ) { workspace =>
              Orchestration.workspaces.setAttributes(projectName, workspace, Map("foo" -> "bar"))(
                projectUser.makeAuthToken()
              )
            }(projectOwner.makeAuthToken())
          }(projectOwner.makeAuthToken(billingScopes))
        }(groupOwnerToken)
      }(groupOwnerToken)
    }(groupOwnerToken)
  }

  it should "not create a workspace with a multi-group auth domain if you're not in all groups" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    intercept[RestException] {
      withGroup("ad", List(projectOwner.email)) { realmGroup =>
        withGroup("ad2", List(projectOwner.email)) { realmGroup2 =>
          withGroup("ad3") { realmGroup3 =>
            withTemporaryBillingProject(billingAccountId) { projectName =>
              withWorkspace(projectName, "AuthDomains", Set(realmGroup, realmGroup2, realmGroup3)) { _ =>
                fail("should not have created workspace")
              }(projectOwner.makeAuthToken())
            }(projectOwner.makeAuthToken(billingScopes))
          }(groupOwnerToken)
        }(groupOwnerToken)
      }(groupOwnerToken)
    }
  }

  it should "do the right security when access group membership changes and there is an access" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email, projectOwner.email)) { realmGroup =>
      withGroup("ng", List(projectUser.email)) { nestedGroup =>
        val nestedGroupFull = Orchestration.groups.getGroup(nestedGroup)(groupOwnerToken)
        withGroup("ag", List(nestedGroupFull.groupEmail)) { accessGroup =>
          val accessGroupFull = Orchestration.groups.getGroup(accessGroup)(groupOwnerToken)
          val workspaceOwnerToken = projectOwner.makeAuthToken()

          // we need a test specific project here because we add one of the groups just created as a writer to the workspace
          // which adds the group to the can-compute policy on the project. Deleting the workspace does not remove the group
          // from the policy so the group remains in use and cannot be deleted so cleanup will fail. Using a new project which
          // gets released removes the offending policy and allows the group to be cleaned up.
          withTemporaryBillingProject(billingAccountId) { localProject =>
            withWorkspace(localProject,
                          "AuthDomains",
                          Set(realmGroup),
                          List(AclEntry(accessGroupFull.groupEmail, WorkspaceAccessLevel.Writer))
            ) { workspace =>
              val user = projectUser
              val userToken = user.makeAuthToken()

              // user is in all the right groups, this should work
              Orchestration.workspaces.setAttributes(localProject, workspace, Map("foo" -> "bar"))(userToken)

              // remove user from nestedGroup and they should lose access
              Orchestration.groups.removeUserFromGroup(nestedGroup, user.email, GroupRole.Member)(groupOwnerToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(localProject, workspace, Map("foo" -> "bar"))(userToken)
                }
              }
              // add user back to nestedGroup and they should have access
              Orchestration.groups.addUserToGroup(nestedGroup, user.email, GroupRole.Member)(groupOwnerToken)
              Orchestration.workspaces.setAttributes(localProject, workspace, Map("foo" -> "bar"))(userToken)

              // remove accessGroup from acl and user should lose access
              Orchestration.workspaces.updateAcl(localProject,
                                                 workspace,
                                                 accessGroupFull.groupEmail,
                                                 WorkspaceAccessLevel.NoAccess,
                                                 None,
                                                 None
              )(workspaceOwnerToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(localProject, workspace, Map("foo" -> "bar"))(userToken)
                }
              }

            }(workspaceOwnerToken)
          }(projectOwner.makeAuthToken(billingScopes))
        }(groupOwnerToken)
      }(groupOwnerToken)
    }(groupOwnerToken)
  }

  it should "clone a workspace if the source has a multi-group auth domain and user is in all groups" in {
    val authToken = projectOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email)) { realmGroup2 =>
        withGroup("ad3", List(projectUser.email)) { realmGroup3 =>
          val authDomain = Set(realmGroup, realmGroup2, realmGroup3)
          withTemporaryBillingProject(billingAccountId, users = List(projectUser.email).some) { projectName =>
            withWorkspace(projectName,
                          "AuthDomains",
                          authDomain,
                          List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))
            ) { workspace =>
              val clone = "AuthDomainsClone_" + makeRandomId()
              Orchestration.workspaces.clone(projectName, workspace, projectName, clone, authDomain)(
                projectUser.makeAuthToken()
              )
              try {
                Orchestration.workspaces.setAttributes(projectName, clone, Map("foo" -> "bar"))(
                  projectUser.makeAuthToken()
                )

                Orchestration.groups.removeUserFromGroup(realmGroup2, projectUser.email, GroupRole.Member)(authToken)
                eventually {
                  intercept[RestException] {
                    Orchestration.workspaces.setAttributes(projectName, clone, Map("foo" -> "bar"))(
                      projectUser.makeAuthToken()
                    )
                  }
                }
                // add users back so the cleanup part of withGroup doesn't have a fit
                Orchestration.groups.addUserToGroup(realmGroup2, projectUser.email, GroupRole.Member)(authToken)
              } finally
                Orchestration.workspaces.delete(projectName, clone)(projectUser.makeAuthToken())
            }(authToken)
          }(projectOwner.makeAuthToken(billingScopes))
        }(authToken)
      }(authToken)
    }(authToken)
  }

  it should "clone a workspace if the user added a group to the source authorization domain" in {
    val authToken = projectOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email)) { realmGroup2 =>
        withGroup("ad3", List(projectUser.email)) { realmGroup3 =>
          val authDomain = Set(realmGroup, realmGroup2)
          withTemporaryBillingProject(billingAccountId, users = List(projectUser.email).some) { projectName =>
            withWorkspace(projectName,
                          "AuthDomains",
                          authDomain,
                          List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))
            ) { workspace =>
              val clone = "AuthDomainsClone_" + makeRandomId()
              Orchestration.workspaces.clone(projectName, workspace, projectName, clone, authDomain + realmGroup3)(
                projectUser.makeAuthToken()
              )
              try
                Orchestration.workspaces.setAttributes(projectName, clone, Map("foo" -> "bar"))(
                  projectUser.makeAuthToken()
                )
              finally
                Orchestration.workspaces.delete(projectName, clone)(projectUser.makeAuthToken())

            }(authToken)
          }(projectOwner.makeAuthToken(billingScopes))
        }(authToken)
      }(authToken)
    }(authToken)
  }

  it should "not allow changing a workspace's Realm if it exists" in {
    val authToken = projectOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email)) { realmGroup2 =>
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName,
                        "AuthDomains",
                        Set(realmGroup),
                        List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))
          ) { workspace =>
            intercept[RestException] {
              val clone = "AuthDomainsClone_" + makeRandomId()
              Orchestration.workspaces.clone(projectName, workspace, projectName, clone, Set(realmGroup2))(authToken)
              // This should be unreachable assuming the above throws
              Orchestration.workspaces.delete(projectName, workspace)(authToken)
            }
          }(authToken)
        }(projectOwner.makeAuthToken(billingScopes))
      }(authToken)
    }(authToken)
  }

  it should "bucket should not be accessible to project owners via projectViewer Google role" in {

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
