package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, MethodFixtures, SubWorkflowFixtures}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.service.BillingProject.Role
import org.broadinstitute.dsde.workbench.service.{Google, Orchestration, Rawls}
import org.scalatest.concurrent.Eventually
import org.scalatest.{FreeSpec, Matchers}


class BillingApiSpec extends FreeSpec with BillingFixtures with MethodFixtures with SubWorkflowFixtures with Matchers with Eventually {

  val owner: Credentials = UserPool.chooseProjectOwner
  val ownerAuthToken: AuthToken = owner.makeAuthToken()

  val studentA = UserPool.chooseStudent
  val studentAToken: AuthToken = studentA.makeAuthToken()

  "A user" - {
    "with a billing account" - {
      "create a new billing project" in {

        implicit val authToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)

        // create a new google billing project
        val billingProjectName = "rawlspec-billingapi-" + makeRandomId()
        register cleanUp deleteBillingProject(billingProjectName) // delete billing project at end of test
        Rawls.billing.createBillingProject(billingProjectName, ServiceTestConfig.Projects.billingAccountId)

        // verify the google billing project is associated with the billing account
        val associatedBillingAccount = Google.billing.getBillingProjectAccount(billingProjectName)
        eventually {
          associatedBillingAccount shouldBe Some(ServiceTestConfig.Projects.billingAccountId)
        }

        // add studentA to google billing project with User role
        Rawls.billing.addUserToBillingProject(billingProjectName, owner.email, Role.Owner)
        Rawls.billing.addUserToBillingProject(billingProjectName, studentA.email, Role.User)

        // verify studentA is a google billing project member
        val members: List[Map[String, String]] = Rawls.billing.listMembersInBillingProject(billingProjectName)
        val emails: List[String] = members.map(_.getOrElse("email", ""))
        emails should (contain(studentA.email) and not contain "")

        // create new workspace
        val workspaceName = randomIdWithPrefix("billingApiSpec")
        register cleanUp Rawls.workspaces.delete(billingProjectName, workspaceName) // delete workspace at end of test
        Rawls.workspaces.create(billingProjectName, workspaceName)
        Orchestration.workspaces.waitForBucketReadAccess(billingProjectName, workspaceName)

        // launch workflows
        val submissionId = Workflow.launchWorkflowOnSimpleMethod(billingProjectName, workspaceName)
        val submissionStatus = "Done"
        Workflow.waitForSubmissionIsStatus(billingProjectName, workspaceName, submissionId, submissionStatus)

        // END
      }
    }

  }


  private def deleteBillingProject(billingProjectName: String)(implicit token: AuthToken): Unit = {
    val projectOwnerInfo = UserInfo(OAuth2BearerToken(token.value), WorkbenchUserId(""), WorkbenchEmail("doesnot@matter.com"), 100)
    Rawls.admin.deleteBillingProject(billingProjectName, projectOwnerInfo)(UserPool.chooseAdmin.makeAuthToken())
  }

}
