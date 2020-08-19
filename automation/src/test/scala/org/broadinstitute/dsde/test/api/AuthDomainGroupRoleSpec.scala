package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.Orchestration.groups.GroupRole
import org.broadinstitute.dsde.workbench.service.{Rawls, Sam}
import org.broadinstitute.dsde.workbench.service.test.AuthDomainMatcher
import org.scalatest.{FreeSpec, Matchers}


class AuthDomainGroupRoleSpec extends FreeSpec with WorkspaceFixtures with BillingFixtures with GroupFixtures with Matchers {

  "removing permissions from workspace with auth domain" - {

    "+ project owner, + group member, create workspace, - group member" in {

      // a billing project owner and a member of the authdomain group
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner, a authdomain group owner and workspace owner
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, List(student.email)) { projectName =>
        withGroup("group", List(student.email)) { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove a group member "billingProjectOwner" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)

            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "+ project owner, + group member, create workspace, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, ownerEmails = List(student.email)) { projectName =>
        withGroup("group", List(student.email)) { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)

            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "+ project owner, create workspace, + group member, - group member" in {

      // a billing project owner and a member of the authdomain group
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner, a authdomain group owner and workspace owner
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, List(student.email)) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add a group member "billingProjectOwner" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove a group member "billingProjectOwner" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "+ project owner, + group member, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, ownerEmails = List(student.email)) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "+ group member, + project owner, - group member" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group", memberEmails = List(student.email)) { groupName =>
          withCleanUp {
            withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

              // add "student" to billing project with owner role
              Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

              // remove a group member "student" from group
              Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
              AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

            }(projectOwnerToken)
          }
        }(projectOwnerToken)
      }
    }

    "+ group member, + project owner, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group", memberEmails = List(student.email)) { groupName =>
          withCleanUp {
            withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>
              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

              // add "student" to billing project with owner role
              Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

              // remove "student" from billing project
              Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

            }(projectOwnerToken)
          }
        }(projectOwnerToken)
      }
    }

    "create workspace, + project owner, + group member, - group member" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add "student" to billing project with owner role
            Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove a group member "student" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "create workspace, + project owner, + group member, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add "student" to billing project with owner role
            Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "create workspace, + group member, + project owner, - group member" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)

            // add "student" to billing project with owner role
            Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove a group member "student" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)
            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

    "create workspace, + group member, + project owner, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      val projectOwnerToken: AuthToken = billingProjectOwner.makeAuthToken()

      val student = UserPool.chooseStudent
      val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)(projectOwnerToken)

            // add "student" to billing project with owner role
            Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(studentToken)

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)(projectOwnerToken)
            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(studentToken)

          }(projectOwnerToken)
        }(projectOwnerToken)
      }
    }

  }
}
