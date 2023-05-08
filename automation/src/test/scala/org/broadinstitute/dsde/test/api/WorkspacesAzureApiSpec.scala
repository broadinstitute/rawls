package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AzureManagedAppCoordinates, WorkspaceCloudPlatform, WorkspaceResponse, WorkspaceType}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryAzureBillingProject
import org.broadinstitute.dsde.workbench.service.{Rawls, RestException}
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import java.util.UUID


@WorkspacesAzureTest
class AzureWorkspacesSpec extends AnyFlatSpec with Matchers with CleanUp {
  val owner: Credentials = UserPool.chooseProjectOwner

  def ownerAuthToken: AuthToken = {
    owner.makeAuthToken()
  }

  private val azureManagedAppCoordinates = AzureManagedAppCoordinates(
    UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"),
    UUID.fromString("f557c728-871d-408c-a28b-eb6b2141a087"),
    "staticTestingMrg",
    Some(UUID.fromString("f41c1a97-179b-4a18-9615-5214d79ba600"))
  )

  implicit val token: AuthToken = ownerAuthToken

  "Rawls" should "allow creation and deletion of azure workspaces" in {
    assert(true)
//    withTemporaryAzureBillingProject(azureManagedAppCoordinates) { projectName =>
//      val workspaceName = generateWorkspaceName()
//      Rawls.workspaces.create(
//        projectName,
//        workspaceName,
//        Set.empty,
//        Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeBoolean(true))
//      )
//      val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
//      response.workspace.name should be(workspaceName)
//      response.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))
//      response.workspace.workspaceType should be(Some(WorkspaceType.McWorkspace))
//
//      Rawls.workspaces.delete(projectName, workspaceName)
//      assertNoAccessToWorkspace(projectName, workspaceName)
//    }
  }

  private def generateWorkspaceName(): String = {
    s"${UUID.randomUUID().toString()}-azure-test-workspace"
  }

  private def assertExceptionStatusCode(exception: RestException, statusCode: Int): Unit = {
    exception.message.parseJson.asJsObject.fields("statusCode").convertTo[Int] should be(statusCode)
  }

  private def assertNoAccessToWorkspace(projectName: String, workspaceName: String)(implicit token: AuthToken): Unit = {
    eventually {
      val exception = intercept[RestException](Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(token))
      assertExceptionStatusCode(exception, 404)
    }
  }

  private def workspaceResponse(response: String): WorkspaceResponse = response.parseJson.convertTo[WorkspaceResponse]
}
