package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.{Google, Orchestration, Rawls, RestException}
import org.scalatest.concurrent.Eventually
import org.scalatest.{FreeSpec, Matchers}

import scala.util.Try


class BillingApiSpec extends FreeSpec with BillingFixtures with MethodFixtures with SubWorkflowFixtures
  with Matchers with Eventually {

  /**
    * This test does
    *
    * Given) a registered user who is project owner
    * and)   the project owner can access to the Google billing account
    * When)  the project owner is authenticated with access token
    * Then)  the project owner can create a new Google billing project
    * and)   the project owner can add another user to Google billing project
    * and)   the project owner can create a new workspace with Google billing project
    * and)   the project owner can create new method and method config
    * and)   the project owner can run an analysis and wait for complete successfully
    * and)   the project owner can delete method config
    * and)   the project owner can delete workspace
    * and)   the project owner can delete Google billing project
    *
    */
  
  "A user" - {
    "with a billing account" - {
      "create a new billing project" in {

        val owner: Credentials = UserPool.chooseProjectOwner
        implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)

        // create a new google billing project
        val billingProjectName = "rawls-billingapispec-" + makeRandomId()
        register cleanUp Try(deleteBillingProject(billingProjectName)).recover {
          case _: RestException =>
        }
        Rawls.billing.createBillingProject(billingProjectName, ServiceTestConfig.Projects.billingAccountId)

        // verify the google billing project is created and associated with the billing account
        eventually {
          val associatedBillingAccount = Google.billing.getBillingProjectAccount(billingProjectName)
          associatedBillingAccount shouldBe Some(ServiceTestConfig.Projects.billingAccountId)
        }

        // add studentA to google billing project with USER role
        val studentA = UserPool.chooseStudent
        Rawls.billing.addUserToBillingProject(billingProjectName, owner.email, BillingProjectRole.Owner)
        Rawls.billing.addUserToBillingProject(billingProjectName, studentA.email, BillingProjectRole.User)

        // verify studentA is a member in google billing project
        val members: List[Map[String, String]] = Rawls.billing.listMembersInBillingProject(billingProjectName)
        val emails: List[String] = members.map(_.getOrElse("email", ""))
        emails should (contain(studentA.email) and not contain "")

        // create new workspace
        val workspaceName = randomIdWithPrefix("BillingApiSpec")
        register cleanUp Try(Rawls.workspaces.delete(billingProjectName, workspaceName)).recover {
          case _: RestException =>
        }
        Rawls.workspaces.create(billingProjectName, workspaceName)
        Orchestration.workspaces.waitForBucketReadAccess(billingProjectName, workspaceName)

        // create a method
        withMethod("BillingApiSpec_workspace", MethodData.SimpleMethod) { methodName =>
          val method: Method = MethodData.SimpleMethod.copy(methodName = methodName)

         // val configNamespace = billingProjectName
         // val configName = s"${workspaceName}_${SimpleMethodConfig.configName}"
          val participantId = randomIdWithPrefix("participant")
          val participantEntity = s"entity:participant_id\n$participantId"
          Orchestration.importMetaData(billingProjectName, workspaceName, "entities", participantEntity)

          // create method config in workspace
          Rawls.methodConfigs.createMethodConfigInWorkspace(
            billingProjectName,
            workspaceName,
            method,
            SimpleMethodConfig.configNamespace,
            SimpleMethodConfig.configName,
            1,
            SimpleMethodConfig.inputs,
            SimpleMethodConfig.outputs,
            SimpleMethodConfig.rootEntityType)

          // launch submission
          val submissionId = Rawls.submissions.launchWorkflow(
            billingProjectName,
            workspaceName,
            SimpleMethodConfig.configNamespace,
            SimpleMethodConfig.configName,
            SimpleMethodConfig.rootEntityType,
            participantId,
            "this",
            useCallCache = false)

          // pause a minute because cromwell isn't fast
          Thread.sleep(60 * 1000)

          // monitor submission status until Done
          val submissionStatus = "Done" // submission complete successfully
          Submission.waitUntilSubmissionIsStatus(billingProjectName, workspaceName, submissionId, submissionStatus)
        }

        // clean up
        Rawls.workspaces.delete(billingProjectName, workspaceName)
        deleteBillingProject(billingProjectName)
      }
    }

  }


  private def deleteBillingProject(billingProjectName: String)(implicit token: AuthToken): Unit = {
    val projectOwnerInfo = UserInfo(OAuth2BearerToken(token.value), WorkbenchUserId(""), WorkbenchEmail("doesnot@matter.com"), 100)
    Rawls.admin.deleteBillingProject(billingProjectName, projectOwnerInfo)(UserPool.chooseAdmin.makeAuthToken())
  }

}
