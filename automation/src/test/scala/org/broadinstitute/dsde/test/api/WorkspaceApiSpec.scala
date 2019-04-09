package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service._
import org.broadinstitute.dsde.workbench.util.Retry
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.scalatest.concurrent.Eventually
import org.scalatest.{FreeSpecLike, Matchers}
import spray.json._

class WorkspaceApiSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers with Eventually with GroupFixtures
  with CleanUp with RandomUtil with Retry
  with BillingFixtures with WorkspaceFixtures with SubWorkflowFixtures {

  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val owner: Credentials = UserPool.chooseProjectOwner
  val ownerAuthToken: AuthToken = owner.makeAuthToken()

  "Rawls" - {
    "should allow project owners" - {
      "to create, clone, and delete workspaces" in {
        implicit val token: AuthToken = ownerAuthToken

        withCleanBillingProject(owner) { projectName =>
          val workspaceName = s"${UUID.randomUUID().toString()}-owner-test-workspace"
          val workspaceCloneName = s"$workspaceName-copy"

          Rawls.workspaces.create(projectName, workspaceName)
          getWorkspaceId(projectName, workspaceName) should not be empty

          Rawls.workspaces.clone(projectName, workspaceName, projectName, workspaceCloneName)
          getWorkspaceId(projectName, workspaceCloneName) should not be empty

          Rawls.workspaces.delete(projectName, workspaceName)
          assertNoAccessToWorkspace(projectName, workspaceName)

          Rawls.workspaces.delete(projectName, workspaceCloneName)
          assertNoAccessToWorkspace(projectName, workspaceCloneName)
        }
      }

      "to add readers with can-share access" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-share-reader") { workspaceName =>
            // grant reader access and can-share permission to student A
            val shareWithStudentA: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(true), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentA)(ownerAuthToken)

            val studentAWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(studentAWorkspaceDetails) should equal(WorkspaceAccessLevel.Reader.toString())
            parseShareAccess(studentAWorkspaceDetails) should be(true)

            // check that student A can share the workspace with student B
            val shareWithStudentB: Set[AclEntry] = Set(AclEntry(studentB.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentB)(studentAToken)
            val studentBWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentBToken)
            parseWorkspaceAccessLevel(studentBWorkspaceDetails) should equal(WorkspaceAccessLevel.Reader.toString())
          }(ownerAuthToken)
        }
      }

      "to add readers without can-share access" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-no-share-reader") { workspaceName =>
            // grant reader access to student A
            val shareWithStudentA: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentA)(ownerAuthToken)

            val studentAWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(studentAWorkspaceDetails) should equal(WorkspaceAccessLevel.Reader.toString())
            parseShareAccess(studentAWorkspaceDetails) should be(false)

            // check that student A cannot share the workspace with student B
            val shareWithStudentB: Set[AclEntry] = Set(AclEntry(studentB.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            assertThrows[RestException](Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentB)(studentAToken))
            assertNoAccessToWorkspace(projectName, workspaceName)(studentBToken)
          }(ownerAuthToken)
        }
      }

      "to add writers with can-compute access and then revoke can-compute" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-compute-writer") { workspaceName =>
            val writerCanCompute: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, Some(false), Some(true)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerCanCompute)(ownerAuthToken)

            val canComputeWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(canComputeWorkspaceDetails) should equal(WorkspaceAccessLevel.Writer.toString())
            parseComputeAccess(canComputeWorkspaceDetails) should be(true)

            val revokeCanCompute: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, revokeCanCompute)(ownerAuthToken)

            val noComputeWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(noComputeWorkspaceDetails) should equal(WorkspaceAccessLevel.Writer.toString())
            parseComputeAccess(noComputeWorkspaceDetails) should be(false)
          }(ownerAuthToken)
        }
      }

      "to change a writer's access level to reader" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-reader-to-writer") { workspaceName =>
            val writerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerAccess)(ownerAuthToken)

            val writerWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(writerWorkspaceDetails) should equal(WorkspaceAccessLevel.Writer.toString())
            parseComputeAccess(writerWorkspaceDetails) should be(true)

            val readerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))
            Rawls.workspaces.updateAcl(projectName, workspaceName, readerAccess)(ownerAuthToken)

            val readerWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(readerWorkspaceDetails) should equal(WorkspaceAccessLevel.Reader.toString())
            parseComputeAccess(readerWorkspaceDetails) should be(false)
          }(ownerAuthToken)
        }
      }

      "to change a writer's access level to no access" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-revoke-writer") { workspaceName =>
            val writerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerAccess)(ownerAuthToken)

            val writerWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(writerWorkspaceDetails) should equal(WorkspaceAccessLevel.Writer.toString())

            val noAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.NoAccess))
            Rawls.workspaces.updateAcl(projectName, workspaceName, noAccess)(ownerAuthToken)

            assertNoAccessToWorkspace(projectName, workspaceName)(studentAToken)
          }(ownerAuthToken)
        }
      }

      "to add an owner to a workspace, but not to change the new owner's permissions" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-two-owners") { workspaceName =>
            val ownerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Owner))
            Rawls.workspaces.updateAcl(projectName, workspaceName, ownerAccess)(ownerAuthToken)

            val secondOwnerWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(secondOwnerWorkspaceDetails) should equal(WorkspaceAccessLevel.Owner.toString())
            parseComputeAccess(secondOwnerWorkspaceDetails) should be(true)
            parseShareAccess(secondOwnerWorkspaceDetails) should be(true)

            // try removing new owner's sharing and compute abilities, it should not work
            val changePermissions: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Owner, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, changePermissions)(ownerAuthToken)

            val newOwnerWorkspaceDetails = Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken)
            parseWorkspaceAccessLevel(newOwnerWorkspaceDetails) should equal(WorkspaceAccessLevel.Owner.toString())
            parseComputeAccess(newOwnerWorkspaceDetails) should be(true)
            parseShareAccess(newOwnerWorkspaceDetails) should be(true)
          }(ownerAuthToken)
        }
      }
    }

    "should not allow project owners" - {
      "to grant readers can-compute access" in {
        withCleanBillingProject(owner) { projectName =>
          withWorkspace(projectName, s"${UUID.randomUUID().toString()}-no-compute-reader") { workspaceName =>
            val computeReader: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(false), Some(true)))

            val exception = intercept[RestException](Rawls.workspaces.updateAcl(projectName, workspaceName, computeReader)(ownerAuthToken))

            import DefaultJsonProtocol._
            exception.message.parseJson.asJsObject.fields("message").convertTo[String] should equal("may not grant readers compute access")
          }(ownerAuthToken)
        }
      }
    }
  }

  import DefaultJsonProtocol._

  private def getWorkspaceId(projectName: String, workspaceName: String)(implicit token: AuthToken): String = {
    Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName).parseJson.asJsObject.getFields("workspace").flatMap { workspace =>
      workspace.asJsObject.getFields("workspaceId")
    }.head.convertTo[String]
  }

  private def parseWorkspaceAccessLevel(workspaceDetails: String): String = {
    workspaceDetails.parseJson.asJsObject.fields("accessLevel").convertTo[String]
  }

  private def parseShareAccess(workspaceDetails: String): Boolean = {
    workspaceDetails.parseJson.asJsObject.fields("canShare").convertTo[Boolean]
  }

  private def parseComputeAccess(workspaceDetails: String): Boolean = {
    workspaceDetails.parseJson.asJsObject.fields("canCompute").convertTo[Boolean]
  }

  private def assertNoAccessToWorkspace(projectName: String, workspaceName: String)(implicit token: AuthToken): Unit = {
    val exception = intercept[RestException](Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(token))
    val statusCode = exception.message.parseJson.asJsObject.fields("statusCode").convertTo[Int]
    statusCode should be(404)
  }
}