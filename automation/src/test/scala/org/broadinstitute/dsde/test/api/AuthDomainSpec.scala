package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.Orchestration.groups.GroupRole
import org.broadinstitute.dsde.workbench.service.{AclEntry, Orchestration, RestException, WorkspaceAccessLevel}
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuthDomainSpec extends AnyFlatSpec with Matchers with CleanUp with BillingFixtures with WorkspaceFixtures with GroupFixtures with RandomUtil with Eventually with BeforeAndAfterAll {
  
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(150, Seconds)), interval = scaled(Span(2, Seconds)))

  lazy val projectOwner = UserPool.chooseProjectOwner
  lazy val (projectUser, groupOwner) = {
    val users = UserPool.chooseStudents(2)
    (users(0), users(1))
  }
  lazy val project = {
    val p = claimGPAllocProject(projectOwner)
    Orchestration.billing.addUserToBillingProject(p.projectName, projectUser.email, BillingProjectRole.User)(projectOwner.makeAuthToken())
    p
  }

  override protected def afterAll(): Unit = {
    project.cleanup(projectOwner)
  }

  "AuthDomains" should "create and access a workspace with an auth domain" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email, projectOwner.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email, projectOwner.email)) { realmGroup2 =>
        withGroup("ad3", List(projectUser.email, projectOwner.email)) { realmGroup3 =>
          withWorkspace(project.projectName, "AuthDomains", Set(realmGroup, realmGroup2, realmGroup3), List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))) { workspace =>
            Orchestration.workspaces.setAttributes(project.projectName, workspace, Map("foo" -> "bar")) (projectUser.makeAuthToken())
          } (projectOwner.makeAuthToken())
        } (groupOwnerToken)
      } (groupOwnerToken)
    } (groupOwnerToken)
  }

  it should "not create a workspace with a multi-group auth domain if you're not in all groups" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    intercept[RestException] {
      withGroup("ad", List(projectOwner.email)) { realmGroup =>
        withGroup("ad2", List(projectOwner.email)) { realmGroup2 =>
          withGroup("ad3") { realmGroup3 =>
            withWorkspace(project.projectName, "AuthDomains", Set(realmGroup, realmGroup2, realmGroup3)) { _ =>
              fail("should not have created workspace")
            }(projectOwner.makeAuthToken())
          }(groupOwnerToken)
        }(groupOwnerToken)
      }(groupOwnerToken)
    }
  }

  it should "do the right security when auth domain membership changes" in {
    val groupOwnerToken = groupOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email, projectOwner.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email, projectOwner.email)) { realmGroup2 =>
        val realmGroup2Full = Orchestration.groups.getGroup(realmGroup2)(groupOwnerToken)
        withGroup("ad3", List(realmGroup2Full.groupEmail)) { realmGroup3 =>
          withWorkspace(project.projectName, "AuthDomains", Set(realmGroup, realmGroup3), List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))) { workspace =>
            for (user <- Set(projectOwner, projectUser)) {
              val userToken = user.makeAuthToken()

              // user is in all the right groups, this should work
              Orchestration.workspaces.setAttributes(project.projectName, workspace, Map("foo" -> "bar"))(userToken)

              // remove user from realmGroup2 and they should lose access
              Orchestration.groups.removeUserFromGroup(realmGroup2, user.email, GroupRole.Member)(groupOwnerToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(project.projectName, workspace, Map("foo" -> "bar"))(userToken)
                }
              }

              // add user back to realmGroup2 and they should have access
              Orchestration.groups.addUserToGroup(realmGroup2, user.email, GroupRole.Member)(groupOwnerToken)
              Orchestration.workspaces.setAttributes(project.projectName, workspace, Map("foo" -> "bar"))(userToken)

              // remove user from realmGroup and they should lose access
              Orchestration.groups.removeUserFromGroup(realmGroup, user.email, GroupRole.Member)(groupOwnerToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(project.projectName, workspace, Map("foo" -> "bar"))(userToken)
                }
              }
              // add users back so the cleanup part of withGroup doesn't have a fit
              Orchestration.groups.addUserToGroup(realmGroup2, user.email, GroupRole.Member)(groupOwnerToken)
              Orchestration.groups.addUserToGroup(realmGroup, user.email, GroupRole.Member)(groupOwnerToken)

            }
          }(projectOwner.makeAuthToken())
        }(groupOwnerToken)
      }(groupOwnerToken)
    }(groupOwnerToken)
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
          withCleanBillingProject(projectOwner) { localProject =>
            withWorkspace(localProject, "AuthDomains", Set(realmGroup), List(AclEntry(accessGroupFull.groupEmail, WorkspaceAccessLevel.Writer))) { workspace =>
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
              Orchestration.workspaces.updateAcl(localProject, workspace, accessGroupFull.groupEmail, WorkspaceAccessLevel.NoAccess, None, None)(workspaceOwnerToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(localProject, workspace, Map("foo" -> "bar"))(userToken)
                }
              }
              // add users back so the cleanup part of withGroup doesn't have a fit
              Orchestration.groups.addUserToGroup(nestedGroup, user.email, GroupRole.Member)(groupOwnerToken)

            }(workspaceOwnerToken)
          }
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
          withWorkspace(project.projectName, "AuthDomains", authDomain, List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))) { workspace =>
            val clone = "AuthDomainsClone_" + makeRandomId()
            Orchestration.workspaces.clone(project.projectName, workspace, project.projectName, clone, authDomain)(projectUser.makeAuthToken())
            withCleanUp {
              register cleanUp Orchestration.workspaces.delete(project.projectName, clone)(projectUser.makeAuthToken())
              Orchestration.workspaces.setAttributes(project.projectName, clone, Map("foo" -> "bar"))(projectUser.makeAuthToken())

              Orchestration.groups.removeUserFromGroup(realmGroup2, projectUser.email, GroupRole.Member)(authToken)
              eventually {
                intercept[RestException] {
                  Orchestration.workspaces.setAttributes(project.projectName, clone, Map("foo" -> "bar"))(projectUser.makeAuthToken())
                }
              }
              // add users back so the cleanup part of withGroup doesn't have a fit
              Orchestration.groups.addUserToGroup(realmGroup2, projectUser.email, GroupRole.Member)(authToken)
            }
          } (authToken)
        } (authToken)
      } (authToken)
    } (authToken)
  }

  it should "clone a workspace if the user added a group to the source authorization domain" in {
    val authToken = projectOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email)) { realmGroup2 =>
        withGroup("ad3", List(projectUser.email)) { realmGroup3 =>
          val authDomain = Set(realmGroup, realmGroup2)
          withWorkspace(project.projectName, "AuthDomains", authDomain, List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))) { workspace =>
            val clone = "AuthDomainsClone_" + makeRandomId()
            Orchestration.workspaces.clone(project.projectName, workspace, project.projectName, clone, authDomain + realmGroup3)(projectUser.makeAuthToken())
            withCleanUp {
              register cleanUp Orchestration.workspaces.delete(project.projectName, clone)(projectUser.makeAuthToken())
              Orchestration.workspaces.setAttributes(project.projectName, clone, Map("foo" -> "bar"))(projectUser.makeAuthToken())
            }
          } (authToken)
        } (authToken)
      } (authToken)
    } (authToken)
  }

  it should "not allow changing a workspace's Realm if it exists" in {
    val authToken = projectOwner.makeAuthToken()

    withGroup("ad", List(projectUser.email)) { realmGroup =>
      withGroup("ad2", List(projectUser.email)) { realmGroup2 =>
        withWorkspace(project.projectName, "AuthDomains", Set(realmGroup), List(AclEntry(projectUser.email, WorkspaceAccessLevel.Writer))) { workspace =>
          intercept[RestException] {
            val clone = "AuthDomainsClone_" + makeRandomId()
            Orchestration.workspaces.clone(project.projectName, workspace, project.projectName, clone, Set(realmGroup2))(authToken)
          }
        }(authToken)
      }(authToken)
    }(authToken)
  }
}
