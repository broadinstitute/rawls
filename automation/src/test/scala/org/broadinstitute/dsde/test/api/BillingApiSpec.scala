package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.service.BillingProject.BillingProjectRole
import org.broadinstitute.dsde.workbench.service.SamModel._
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.service.util.Retry.retry
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, RestException, Sam}
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.DurationLong
import scala.util.Try

//noinspection NoTailRecursionAnnotation,RedundantBlock,ScalaUnusedSymbol
@BillingsTest
class BillingApiSpec
    extends AnyFreeSpec
    with MethodFixtures
    with Matchers
    with Eventually
    with TestReporterFixture
    with LazyLogging
    with CleanUp {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(20, Seconds)))

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
    *
    */

  "A user with a billing account" - {
    "can create a new billing project" in {

      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)

      // create a new google billing project
      val billingProjectName = createNewBillingProject(owner)

      // 1/5/2019: This api call encountered error code 403 with "message": "The caller does not have permission" from Google in alpha only.
      // Test works fine after comment out this api call.
      // verify the google billing project is created and associated with the billing account
      /* eventually {
        val associatedBillingAccount = Google.billing.getBillingProjectAccount(billingProjectName)
        associatedBillingAccount shouldBe Some(ServiceTestConfig.Projects.billingAccountId)
      } */

      // add studentA to google billing project with USER role
      val studentA = UserPool.chooseStudent
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
          SimpleMethodConfig.rootEntityType
        )

        // launch submission
        val submissionId = Rawls.submissions.launchWorkflow(
          billingProjectName,
          workspaceName,
          SimpleMethodConfig.configNamespace,
          SimpleMethodConfig.configName,
          SimpleMethodConfig.rootEntityType,
          participantId,
          "this",
          useCallCache = false,
          deleteIntermediateOutputFiles = false,
          useReferenceDisks = false,
          memoryRetryMultiplier = 1.0,
          ignoreEmptyOutputs = false
        )

        // wait until submission complete
        Submission.waitUntilSubmissionComplete(billingProjectName, workspaceName, submissionId)

        // verify submission status is Done
        val expectedStatus = "Done"
        val actualStatus = Submission.getSubmissionStatus(billingProjectName, workspaceName, submissionId)
        withClue(s"Submission $billingProjectName/$workspaceName/$submissionId status should be $expectedStatus") {
          actualStatus shouldBe expectedStatus
        }
      }

      // owner should be able to delete the workspace
      Rawls.workspaces.delete(billingProjectName, workspaceName)
    }

    "can create a new billing project with a service perimeter" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)
      val googleAccessPolicy = ServiceTestConfig.Projects.googleAccessPolicy
      val servicePerimeterName = "automation_test_perimeter"
      val fullyQualifiedServicePerimeterId =
        s"accessPolicies/${googleAccessPolicy}/servicePerimeters/${servicePerimeterName}"
      val encodedServicePerimeterId = URLEncoder.encode(fullyQualifiedServicePerimeterId, UTF_8.name)
      val servicePerimeterResourceType = "service-perimeter"

      val accessPolicyMembership = AccessPolicyMembership(Set(owner.email), Set.empty, Set("owner"))
      val createResourceRequest =
        CreateResourceRequest(encodedServicePerimeterId, Map("owner" -> accessPolicyMembership), Set.empty)

      Sam.user.createResource(servicePerimeterResourceType, createResourceRequest)

      register cleanUp Sam.user.deleteResource(servicePerimeterResourceType, encodedServicePerimeterId)

      // try to create a project with a perimeter. retry up to 3 times for project to reach 'Ready' status
      val billingProjectName =
        createNewBillingProject(owner, servicePerimeterOpt = Option(fullyQualifiedServicePerimeterId))
    }

    "can create a new billing project with v2 api" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)
      val billingProjectName = "rawls-billingapispecV2-" + makeRandomId()
      Rawls.billingV2.createBillingProject(billingProjectName, ServiceTestConfig.Projects.billingAccountId)
      val result = Rawls.billingV2.getBillingProject(billingProjectName).toList
      val expected = List(
        "projectName" -> billingProjectName,
        "billingAccount" -> ServiceTestConfig.Projects.billingAccountId,
        "invalidBillingAccount" -> false,
        "roles" -> List("Owner")
      )
      result should contain allElementsOf expected
      Rawls.billingV2.deleteBillingProject(billingProjectName)
    }
  }

  private def createNewBillingProject(user: Credentials, trials: Int = 3, servicePerimeterOpt: Option[String] = None)(
    implicit token: AuthToken
  ): String = {

    val billingProjectName = "rawls-billingapispec-" + makeRandomId()
    register cleanUp Try(deleteBillingProject(billingProjectName)).recover { case _: RestException =>
    }

    Rawls.billing.createBillingProject(billingProjectName,
                                       ServiceTestConfig.Projects.billingAccountId,
                                       servicePerimeterOpt
    )

    // waiting for creationStatus becomes Error or Ready but not Creating
    val statusOption: Option[String] = retry(30.seconds, 20.minutes) {
      val creationStatusOption: Option[String] = for {
        statusMap <- Try(Rawls.billing.getBillingProjectStatus(billingProjectName)(token)).toOption
        status <- statusMap.get("creationStatus")
      } yield status
      creationStatusOption.filterNot(creationStatus =>
        (creationStatus equals "Creating") || (creationStatus equals "AddingToPerimeter")
      )
    }

    statusOption match {
      case None | Some("Error") if trials > 1 =>
        logger.warn(s"Error or timeout creating billing project $billingProjectName. Retrying ${trials - 1} more times")
        createNewBillingProject(user, trials - 1, servicePerimeterOpt)
      case None =>
        fail(s"timed out waiting billing project $billingProjectName to be ready")
      case Some(status) =>
        withClue(s"Checking status in billing project $billingProjectName") {
          status shouldEqual "Ready"
        }
        billingProjectName
    }

  }

  private def deleteBillingProject(billingProjectName: String)(implicit token: AuthToken): Unit = {
    val projectOwnerInfo =
      UserInfo(OAuth2BearerToken(token.value), WorkbenchUserId("0"), WorkbenchEmail("doesnot@matter.com"), 3600)
    Rawls.admin.deleteBillingProject(billingProjectName, projectOwnerInfo)(
      UserPool.chooseAdmin.makeAuthToken(AuthTokenScopes.billingScopes)
    )
  }

}
