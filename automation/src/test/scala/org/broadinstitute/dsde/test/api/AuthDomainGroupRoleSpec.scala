package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.test.util.AuthDomainMatcher
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, GroupFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.Orchestration.groups.GroupRole
import org.broadinstitute.dsde.workbench.service.{Rawls, RestException, Sam}
import org.scalatest.{FreeSpec, Matchers}

import scala.util.Try

class AuthDomainGroupRoleSpec extends FreeSpec with WorkspaceFixtures with BillingFixtures with GroupFixtures with Matchers {

  "removing permissions from workspace with auth domain" - {

    "+ project owner, + group member, create workspace, - group member" in {

      // a billing project owner and a member of the authdomain group
      val billingProjectOwner = UserPool.chooseProjectOwner

      // a billing project owner, a authdomain group owner and workspace owner
      val student = UserPool.chooseStudent
      implicit val studentToken: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, List(student.email)) { projectName =>
        withGroup("group", List(billingProjectOwner.email)) { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(billingProjectOwner.makeAuthToken())

            // remove a group member "billingProjectOwner" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, billingProjectOwner.email)
            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(billingProjectOwner.makeAuthToken())
          }
        }
      }
    }

    "+ project owner, + group member, create workspace, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      implicit val token: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent

      withCleanBillingProject(billingProjectOwner, ownerEmails = List(student.email)) { projectName =>
        withGroup("group", List(student.email)) { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(student.makeAuthToken())

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)
            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())
          }
        }
      }
    }

    "+ project owner, create workspace, + group member, - group member" in {

      // a billing project owner and a member of the authdomain group
      val billingProjectOwner = UserPool.chooseProjectOwner

      // a billing project owner, a authdomain group owner and workspace owner
      val student = UserPool.chooseStudent
      implicit val token: AuthToken = student.makeAuthToken()

      withCleanBillingProject(billingProjectOwner, List(student.email)) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(billingProjectOwner.makeAuthToken())

            // add a group member "billingProjectOwner" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, billingProjectOwner.email)
            register cleanUp Try(Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, billingProjectOwner.email)).recover {
              case _: RestException =>
            }
            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(billingProjectOwner.makeAuthToken())

            // remove a group member "billingProjectOwner" from group
            Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, billingProjectOwner.email)
            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(billingProjectOwner.makeAuthToken())
          }
        }
      }
    }

    "+ project owner, + group member, - group member" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      implicit val token: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent

      withCleanBillingProject(billingProjectOwner, ownerEmails = List(student.email)) { projectName =>
        withGroup("group") { groupName =>
          withCleanUp {
            withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

              AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())

              // add a group member "student" to group
              Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)
              register cleanUp Try(Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)).recover {
                case _: RestException =>
              }
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(student.makeAuthToken())

              // remove a group member "student" from group
              Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)
              AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())
            }
          }
        }
      }
    }

    "+ project owner, + group member, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      implicit val token: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent

      withCleanBillingProject(billingProjectOwner, ownerEmails = List(student.email)) { projectName =>
        withGroup("group") { groupName =>
          withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

            AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())

            // add a group member "student" to group
            Sam.user.addUserToPolicy(groupName, GroupRole.Member.toString, student.email)
            register cleanUp Try(Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)).recover {
              case _: RestException =>
            }
            AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(student.makeAuthToken())

            // remove "student" from billing project
            Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)
            AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())
          }
        }
      }
    }

    "+ group member, + project owner, - group member" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      implicit val token: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group", memberEmails = List(student.email)) { groupName =>
          withCleanUp {
            withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>

              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())

              // add "student" to billing project with owner role
              Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)
              register cleanUp Try(Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)).recover {
                case _: RestException =>
              }
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(student.makeAuthToken())

              // remove a group member "student" from group
              Sam.user.removeUserFromPolicy(groupName, GroupRole.Member.toString, student.email)
              AuthDomainMatcher.checkVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())
            }
          }
        }
      }
    }

    "+ group member, + project owner, - project owner" in {

      // a billing project owner, a authdomain group owner and workspace owner
      val billingProjectOwner = UserPool.chooseProjectOwner
      implicit val token: AuthToken = billingProjectOwner.makeAuthToken()

      // a billing project owner and a member of authdomain group
      val student = UserPool.chooseStudent

      withCleanBillingProject(billingProjectOwner) { projectName =>
        withGroup("group", memberEmails = List(student.email)) { groupName =>
          withCleanUp {
            withWorkspace(projectName, "GroupApiSpec_workspace", Set(groupName)) { workspaceName =>
              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())

              // add "student" to billing project with owner role
              Rawls.billing.addUserToBillingProject(projectName, student.email, BillingProjectRole.Owner)
              register cleanUp Try(Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)).recover {
                case _: RestException =>
              }
              AuthDomainMatcher.checkVisibleAndAccessible(projectName, workspaceName, List(groupName))(student.makeAuthToken())

              // remove "student" from billing project
              Rawls.billing.removeUserFromBillingProject(projectName, student.email, BillingProjectRole.Owner)
              AuthDomainMatcher.checkNotVisibleNotAccessible(projectName, workspaceName)(student.makeAuthToken())
            }
          }
        }
      }
    }

  }
}
