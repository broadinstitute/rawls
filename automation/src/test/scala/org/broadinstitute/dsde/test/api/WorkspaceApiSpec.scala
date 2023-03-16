package org.broadinstitute.dsde.test.api

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.{withTemporaryAzureBillingProject, withTemporaryBillingProject}
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, HttpGoogleIamDAO, HttpGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.service._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

//noinspection JavaAccessorEmptyParenCall,TypeAnnotation
// reenable test in Jenkins
@WorkspacesTest
class WorkspaceApiSpec
  extends TestKit(ActorSystem("MySpec"))
    with AnyFreeSpecLike
    with Matchers
    with Eventually
    with CleanUp
    with RandomUtil
    with Retry
    with ScalaFutures
    with WorkspaceFixtures
    with MethodFixtures {

  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val owner: Credentials = UserPool.chooseProjectOwner
  def ownerAuthToken: AuthToken = {
    owner.makeAuthToken()
  }

  val operations = Array(Map("op" -> "AddUpdateAttribute", "attributeName" -> "participant1", "addUpdateAttribute" -> "testparticipant"))
  val entity: Array[Map[String, Any]] = Array(Map("name" -> "participant1", "entityType" -> "participant", "operations" -> operations))
  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(20, Seconds)))

  "Rawls" - {

    "should set labels on the underlying Google Project when creating a new Workspace" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)
      val billingProjectName = s"workspaceapi-labels-${makeRandomId()}" // lowercase and hyphens due to google's label and display name requirements
      Rawls.billingV2.createBillingProject(billingProjectName, ServiceTestConfig.Projects.billingAccountId)
      val workspaceName = prependUUID("rbs-project-labels-test")

      implicit val ec: ExecutionContext = ExecutionContext.global
      val source = scala.io.Source.fromFile(RawlsConfig.pathToQAJson)
      val jsonCreds = try source.mkString finally source.close()
      val googleProjectDao = new HttpGoogleProjectDAO("rawls-integration-tests", GoogleCredentialModes.Json(jsonCreds), "workbenchMetricBaseName")

      Rawls.workspaces.create(billingProjectName, workspaceName)
      val createdWorkspaceResponse = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(billingProjectName, workspaceName))
      createdWorkspaceResponse.workspace.name should be(workspaceName)
      val createdWorkspaceGoogleProject = createdWorkspaceResponse.workspace.googleProject

      // verify display name (starts with namespace, ends with name, limited to 30 chars)
      val maybeDisplayName = googleProjectDao.getProjectName(createdWorkspaceGoogleProject.value).futureValue
      maybeDisplayName.getOrElse("") should startWith(createdWorkspaceResponse.workspace.namespace)

      // verify labels exist and that we didn't accidentally forget the buffer labels
      val bufferLabels = Map("vpc-network-name" -> "network", "vpc-subnetwork-name" -> "subnetwork")
      val rawlsLabels = Map("workspacenamespace" -> createdWorkspaceResponse.workspace.namespace,
        "workspaceid" -> createdWorkspaceResponse.workspace.workspaceId,
        "workspacename" -> createdWorkspaceResponse.workspace.name)
      val labels = googleProjectDao.getLabels(createdWorkspaceGoogleProject.value).futureValue
      labels should contain allElementsOf bufferLabels
      labels should contain allElementsOf rawlsLabels

      Rawls.workspaces.delete(billingProjectName, workspaceName)
      Rawls.billingV2.deleteBillingProject(billingProjectName)
    }

    "should grant the proper IAM roles on the underlying google project when creating a workspace" in {
      val owner: Credentials = UserPool.chooseProjectOwner
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.userLoginScopes ++ Seq("https://www.googleapis.com/auth/cloud-platform"))
      withTemporaryBillingProject(billingAccountId) { billingProjectName =>
        withCleanUp {
          val workspaceName = prependUUID("rbs-project-iam-test")

          implicit val ec: ExecutionContext = ExecutionContext.global

          val source = scala.io.Source.fromFile(RawlsConfig.pathToQAJson)
          val jsonCreds = try source.mkString finally source.close()
          val googleIamDaoWithCloudCredentials = new HttpGoogleIamDAO("rawls-integration-tests", GoogleCredentialModes.Json(jsonCreds), "workbenchMetricBaseName")

          Rawls.workspaces.create(billingProjectName, workspaceName)
          register cleanUp Rawls.workspaces.delete(billingProjectName, workspaceName)
          val createdWorkspaceResponse = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(billingProjectName, workspaceName))
          createdWorkspaceResponse.workspace.name should be(workspaceName)
          val createdWorkspaceGoogleProject = createdWorkspaceResponse.workspace.googleProject

          val expectedRoles = Set(
            "terra_billing_project_owner",
            "terra_workspace_can_compute",
            "terra_workspace_nextflow_role",
            "compute.serviceAgent",
            "container.serviceAgent",
            "containerregistry.ServiceAgent",
            "dataflow.serviceAgent",
            "dataproc.serviceAgent",
            "owner",
            "editor",
            "genomics.serviceAgent",
            "lifesciences.serviceAgent",
            "pubsub.serviceAgent"
          )

          implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)

          eventually {
            val iamPermissions = googleIamDaoWithCloudCredentials.getProjectPolicy(GoogleProject(createdWorkspaceGoogleProject.value)).futureValue
            val realRoles: Set[String] = iamPermissions.getBindings().asScala.map(_.getRole.split("/").last).toSet

            // set diff here to filter for only those role bindings we know are expected
            expectedRoles.diff(realRoles) shouldEqual Set.empty
          }
        }
      }
    }

    "should allow project owners" - {

      "give me a token" in {
        implicit val token: AuthToken = ownerAuthToken

        logger.info("TOKEN = "+token.value )
      }
      "to create azure workspaces" in {
        implicit val token: AuthToken = ownerAuthToken

        /**
          * "tenantId": "fad90753-2022-4456-9b0a-c7e5b934e408",
          * "subscriptionId": "f557c728-871d-408c-a28b-eb6b2141a087",
          * "managedResourceGroupId": "staticTestingMrg",
          */
        val azureManagedAppCoordinates = new AzureManagedAppCoordinates(
          UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"),
          UUID.fromString("f557c728-871d-408c-a28b-eb6b2141a087"),
          "staticTestingMrg"
        )
        withTemporaryAzureBillingProject(azureManagedAppCoordinates) { projectName =>
          val workspaceName = prependUUID("azure-test-workspace")
          Rawls.workspaces.create(projectName, workspaceName)
          val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
          response.workspace.name should be(workspaceName)
        }
      }

      "to create, clone, and delete workspaces" in {
        implicit val token: AuthToken = ownerAuthToken

        withTemporaryBillingProject(billingAccountId) { projectName =>
          val workspaceName = prependUUID("owner-test-workspace")
          val workspaceCloneName = s"$workspaceName-copy"

          Rawls.workspaces.create(projectName, workspaceName)
          workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)).workspace.name should be(workspaceName)

          Rawls.workspaces.clone(projectName, workspaceName, projectName, workspaceCloneName)
          workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceCloneName)).workspace.name should be(workspaceCloneName)

          Rawls.workspaces.delete(projectName, workspaceName)
          assertNoAccessToWorkspace(projectName, workspaceName)

          Rawls.workspaces.delete(projectName, workspaceCloneName)
          assertNoAccessToWorkspace(projectName, workspaceCloneName)
        }(owner.makeAuthToken(billingScopes))
      }

      "to delete the google project (from Resource Buffer) in a v2 workspaces (in a v2 billing project) when deleting the workspace" in {
        val owner: Credentials = UserPool.chooseProjectOwner
        implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.billingScopes)
        withTemporaryBillingProject(billingAccountId) { billingProjectName =>
          val workspaceName = prependUUID("rbs-delete-workspace")

          implicit val ec: ExecutionContext = ExecutionContext.global
          val source = scala.io.Source.fromFile(RawlsConfig.pathToQAJson)
          val jsonCreds = try source.mkString finally source.close()
          val googleProjectDao = new HttpGoogleProjectDAO("rawls-integration-tests", GoogleCredentialModes.Json(jsonCreds), "workbenchMetricBaseName")

          Rawls.workspaces.create(billingProjectName, workspaceName)
          val createdWorkspaceResponse = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(billingProjectName, workspaceName))
          createdWorkspaceResponse.workspace.name should be(workspaceName)
          val createdWorkspaceGoogleProject = createdWorkspaceResponse.workspace.googleProject

          // verify that the google project exists
          googleProjectDao.isProjectActive(createdWorkspaceGoogleProject.value).map(isProjectActive => isProjectActive shouldBe true)

          // delete the workspace
          Rawls.workspaces.delete(billingProjectName, workspaceName)
          assertNoAccessToWorkspace(billingProjectName, workspaceName)

          // verify that the google project was deleted
          googleProjectDao.isProjectActive(createdWorkspaceGoogleProject.value).map(isProjectActive => isProjectActive shouldBe false)
        }
      }

      "to get an error message when they try to create a workspace with a bucket region that is invalid" ignore {
        implicit val token: AuthToken = ownerAuthToken
        // Note that this invalid region passes the regexp in `withWorkspaceBucketRegionCheck`, so workspace creation is
        // attempted and fails with the bucket creation error. However, due to bug WOR-296, `withTemporaryBillingProject`
        // is unable to delete the temporary project, causing the test to fail (when this test was first introduced,
        // there was a different way of creating test workspaces that did not involve creating and deleting test billing
        // projects). There is already test coverage of `withWorkspaceBucketRegionCheck` in `WorkspaceApiServiceSpec`,
        // so there is no point in changing the invalid region name to one detected by the regexp, as that would change
        // the code path executed (no workspace creation would be attempted) and simply duplicate existing unittests.
        val invalidRegion = "invalid-region1"

        val exception = withTemporaryBillingProject(billingAccountId) { billingProject =>
          val workspaceName = prependUUID("owner-invalid-region-workspace")
          intercept[RestException] {
            Orchestration.workspaces.create(billingProject, workspaceName, Set.empty, Option(invalidRegion))
          }.message.parseJson.asJsObject
        }(owner.makeAuthToken(billingScopes))

        exception.fields("statusCode").convertTo[Int] should equal(400)
        exception.fields("message").convertTo[String] should startWith("Workspace creation failed. Error trying to create bucket ")
        exception.fields("message").convertTo[String] should endWith regex(s" in Google project (.+) in region `${invalidRegion}`.".r)
      }

      "to add readers with can-share access" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("share-reader")) { workspaceName =>
            // grant reader access and can-share permission to student A
            val shareWithStudentA: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(true), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentA)(ownerAuthToken)

            eventually {
              val studentAWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              studentAWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Read))
              studentAWorkspaceDetails.canShare should equal(Option(true))
            }

            // check that student A can share the workspace with student B
            val shareWithStudentB: Set[AclEntry] = Set(AclEntry(studentB.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentB)(studentAToken)

            eventually {
              val studentBWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentBToken))
              studentBWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Read))
            }
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to add readers without can-share access" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("no-share-reader")) { workspaceName =>
            // grant reader access to student A
            val shareWithStudentA: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentA)(ownerAuthToken)

            eventually {
              val studentAWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              studentAWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Read))
              studentAWorkspaceDetails.canShare should equal(Option(false))
            }

            // check that student A cannot share the workspace with student B
            val shareWithStudentB: Set[AclEntry] = Set(AclEntry(studentB.email, WorkspaceAccessLevel.Reader, Some(false), Some(false)))
            assertExceptionStatusCode(intercept[RestException](Rawls.workspaces.updateAcl(projectName, workspaceName, shareWithStudentB)(studentAToken)), 403)
            assertNoAccessToWorkspace(projectName, workspaceName)(studentBToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to add writers with can-compute access and then revoke can-compute" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("compute-writer")) { workspaceName =>
            val writerCanCompute: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, Some(false), Some(true)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerCanCompute)(ownerAuthToken)

            eventually {
              val canComputeWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              canComputeWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Write))
              canComputeWorkspaceDetails.canCompute should equal(Option(true))
            }

            val revokeCanCompute: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, revokeCanCompute)(ownerAuthToken)

            eventually {
              val noComputeWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              noComputeWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Write))
              noComputeWorkspaceDetails.canCompute should equal(Option(false))
            }
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to change a writer's access level to reader" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("reader-to-writer")) { workspaceName =>
            val writerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerAccess)(ownerAuthToken)

            eventually {
              val writerWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              writerWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Write))
              writerWorkspaceDetails.canCompute should equal(Option(true))
            }

            val readerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))
            Rawls.workspaces.updateAcl(projectName, workspaceName, readerAccess)(ownerAuthToken)

            eventually {
              val readerWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              readerWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Read))
              readerWorkspaceDetails.canCompute should equal(Option(false))
            }
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to change a writer's access level to no access" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("revoke-writer")) { workspaceName =>
            val writerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Writer))
            Rawls.workspaces.updateAcl(projectName, workspaceName, writerAccess)(ownerAuthToken)

            eventually {
              val writerWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              writerWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Write))
            }

            val noAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.NoAccess))
            Rawls.workspaces.updateAcl(projectName, workspaceName, noAccess)(ownerAuthToken)

            assertNoAccessToWorkspace(projectName, workspaceName)(studentAToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to add an owner to a workspace, but not to change the new owner's permissions" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("two-owners")) { workspaceName =>
            val ownerAccess: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Owner))
            Rawls.workspaces.updateAcl(projectName, workspaceName, ownerAccess)(ownerAuthToken)

            eventually {
              val secondOwnerWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              secondOwnerWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Owner))
              secondOwnerWorkspaceDetails.canCompute should equal(Option(true))
              secondOwnerWorkspaceDetails.canShare should equal(Option(true))
            }

            // try removing new owner's sharing and compute abilities, it should not work
            val changePermissions: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Owner, Some(false), Some(false)))
            Rawls.workspaces.updateAcl(projectName, workspaceName, changePermissions)(ownerAuthToken)

            eventually {
              val newOwnerWorkspaceDetails = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(studentAToken))
              newOwnerWorkspaceDetails.accessLevel should equal(Some(WorkspaceAccessLevels.Owner))
              newOwnerWorkspaceDetails.canCompute should equal(Option(true))
              newOwnerWorkspaceDetails.canShare should equal(Option(true))
            }
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      val testAttributes: Map[String, String] = Map("A-key" -> "A-value", "B-key" -> "B-value", "C-key" -> "C-value")
      val testAttributeNamespace = "default"
      val attributeMap: AttributeMap = testAttributes.map(keyValuePairs => AttributeName(testAttributeNamespace, keyValuePairs._1) -> AttributeString(keyValuePairs._2))

      "to add workspace attributes" in {
        implicit val token: AuthToken = ownerAuthToken
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("add-attributes")) { workspaceName =>
            val attributeUpdates = attributeMap.map(attrTuple => AddUpdateAttribute(attrTuple._1, attrTuple._2)).toList

            Rawls.workspaces.updateAttributes(projectName, workspaceName, attributeUpdates)
            eventually {
              workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)).workspace.attributes should be (Option(attributeMap))
            }
          }
        }(owner.makeAuthToken(billingScopes))
      }

      "to remove workspace attributes" in {
        implicit val token: AuthToken = ownerAuthToken
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("delete-attributes"), attributes = Some(testAttributes)) { workspaceName =>
            val attributeNameToRemove = testAttributes.keys.head
            val attributeUpdates = List(RemoveAttribute(AttributeName(testAttributeNamespace, attributeNameToRemove)))

            Rawls.workspaces.updateAttributes(projectName, workspaceName, attributeUpdates)
            eventually {
              workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)).workspace.attributes should be (Option(attributeMap.filter(attribute => attribute._1.name != attributeNameToRemove)))
            }
          }
        }(owner.makeAuthToken(billingScopes))
      }
    }

    "should not allow project owners" - {
      "to grant readers can-compute access" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("no-compute-reader")) { workspaceName =>
            val computeReader: Set[AclEntry] = Set(AclEntry(studentA.email, WorkspaceAccessLevel.Reader, Some(false), Some(true)))

            val exception = intercept[RestException](Rawls.workspaces.updateAcl(projectName, workspaceName, computeReader)(ownerAuthToken))

            exception.message.parseJson.asJsObject.fields("message").convertTo[String] should equal("may not grant readers compute access")
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }
    }

    "should allow readers" - {
      "to clone a requester-pays workspace from a different project into their own project" in {
        implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)

        val user: Credentials = UserPool.chooseAdmin
        val userToken: AuthToken = user.makeAuthToken()

        val workspaceName = prependUUID("requester-pays")
        val workspaceCloneName = s"$workspaceName-copy"

        // user does not belong to the source project
        withTemporaryBillingProject(billingAccountId) { sourceProjectName =>
          withTemporaryBillingProject(billingAccountId) { destProjectName =>
            // The original workspace is in the source project. The user is a Reader on this workspace
            withWorkspace(sourceProjectName, workspaceName, aclEntries = List(AclEntry(user.email, WorkspaceAccessLevel.Reader))) { workspaceName =>
              // Enable requester pays on the original workspace and wait for the change to propagate
              val bucketName = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(sourceProjectName, workspaceName)(ownerAuthToken)).workspace.bucketName
              googleStorageDAO.setRequesterPays(GcsBucketName(bucketName), true).futureValue
              eventually {
                workspaceResponse(Rawls.workspaces.getWorkspaceDetails(sourceProjectName, workspaceName)(userToken)).bucketOptions should contain(WorkspaceBucketOptions(true))
              }

              // The user clones the workspace into their project
              Rawls.workspaces.clone(sourceProjectName, workspaceName, destProjectName, workspaceCloneName)(userToken)
              try
                workspaceResponse(Rawls.workspaces.getWorkspaceDetails(destProjectName, workspaceCloneName)(userToken))
                  .workspace.name should be(workspaceCloneName)
              finally
                Rawls.workspaces.delete(destProjectName, workspaceCloneName)(userToken)
            }(ownerAuthToken)
          }(user.makeAuthToken(billingScopes))
        }(owner.makeAuthToken(billingScopes))
      }
    }

    "should not allow readers" - {
      def getMethodConfig(method: Method, projectName: String, destWorkspaceName: String): Map[String, Any] = Map(
        "name" -> s"destination-${method.methodName}",
        "namespace" -> s"destination-${method.methodNamespace}",
        "workspaceName" -> Map(
          "namespace" -> projectName,
          "name" -> destWorkspaceName)
      )

      "to import method configs from another workspace" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("reader-import-config-dest-workspace"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))) { destWorkspaceName =>
            withWorkspace(projectName, prependUUID("method-config-source-workspace"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))) { sourceWorkspaceName =>
              withMethod("reader-import-from-workspace", MethodData.SimpleMethod) { methodName =>
                val method = MethodData.SimpleMethod.copy(methodName = methodName)

                val destMethodConfig = getMethodConfig(method, projectName, destWorkspaceName)
                val sourceMethodConfig = getMethodConfig(method, projectName, sourceWorkspaceName)

                Rawls.methodConfigs.createMethodConfigInWorkspace(
                  projectName, sourceWorkspaceName, method, method.methodNamespace, method.methodName, 1,
                  Map.empty, Map.empty, method.rootEntityType)(ownerAuthToken)

                eventually {
                  val copyFromWorkspaceException = intercept[RestException] {
                    Rawls.methodConfigs.copyMethodConfigFromWorkspace(sourceMethodConfig, destMethodConfig)(studentAToken)
                  }
                  assertExceptionStatusCode(copyFromWorkspaceException, 403)
                }
              }(ownerAuthToken)
            }(ownerAuthToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to import method configs from the method repo" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("reader-import-config-dest-workspace"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))) { destWorkspaceName =>
            withMethod("reader-import-from-method-repo", MethodData.SimpleMethod) { methodName =>
              val method = MethodData.SimpleMethod.copy(methodName = methodName)

              val methodRepoConfig = Map(
                "methodRepoNamespace" -> SimpleMethodConfig.configNamespace,
                "methodRepoName" -> SimpleMethodConfig.configName,
                "methodRepoSnapshotId" -> SimpleMethodConfig.snapshotId,
                "destination" -> getMethodConfig(method, projectName, destWorkspaceName)
              )

              // studentA needs permission to access the method config or importing from method repo will return 404 not 403
              Orchestration.methodConfigurations.setMethodConfigPermission(
                SimpleMethodConfig.configNamespace,
                SimpleMethodConfig.configName,
                SimpleMethodConfig.snapshotId,
                studentA.email,
                "OWNER"
              )(ownerAuthToken)

              eventually {
                val copyFromMethodRepoException = intercept[RestException] {
                  Rawls.methodConfigs.copyMethodConfigFromMethodRepo(methodRepoConfig)(studentAToken)
                }
                assertExceptionStatusCode(copyFromMethodRepoException, 403)
              }
            }(ownerAuthToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }

      "to launch workflows" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("reader-launch-workflow-fails"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Reader))) { workspaceName =>
            Rawls.entities.importMetaData(projectName, workspaceName, entity)(ownerAuthToken)

            withMethod("reader-cannot-launch-workflows", MethodData.SimpleMethod) { methodName =>
              val method = MethodData.SimpleMethod.copy(methodName = methodName)

              Rawls.methodConfigs.createMethodConfigInWorkspace(projectName, workspaceName,
                method, method.methodNamespace, method.methodName, 1,
                SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)(ownerAuthToken)

              eventually {
                val submissionException = intercept[RestException] {
                  Rawls.submissions.launchWorkflow(
                    billingProject = projectName,
                    workspaceName = workspaceName,
                    methodConfigurationNamespace = method.methodNamespace,
                    methodConfigurationName = method.methodName,
                    entityType = method.rootEntityType,
                    entityName = "participant1",
                    expression = "this",
                    useCallCache = false,
                    deleteIntermediateOutputFiles = false,
                    useReferenceDisks = false,
                    memoryRetryMultiplier = 1.0,
                    ignoreEmptyOutputs = false
                  )(studentAToken)
                }
                assertExceptionStatusCode(submissionException, 403)
              }
            }(ownerAuthToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }
    }

    "should allow writers" - {
      "to launch workflows if they have can-compute permission" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("writer-can-launch-workflow"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, canCompute = Some(true)))) { workspaceName =>
            Rawls.entities.importMetaData(projectName, workspaceName, entity)(ownerAuthToken)

            withMethod("writer-method-succeeds", MethodData.SimpleMethod) { methodName =>
              val method = MethodData.SimpleMethod.copy(methodName = methodName)

              Rawls.methodConfigs.createMethodConfigInWorkspace(projectName, workspaceName,
                method, method.methodNamespace, method.methodName, 1,
                SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)(ownerAuthToken)

              Orchestration.methods.setMethodPermissions(
                method.methodNamespace,
                method.methodName,
                method.snapshotId,
                studentA.email,
                "OWNER"
              )(ownerAuthToken)

              val submissionId = Rawls.submissions.launchWorkflow(
                billingProject = projectName,
                workspaceName = workspaceName,
                methodConfigurationNamespace = method.methodNamespace,
                methodConfigurationName = method.methodName,
                entityType = method.rootEntityType,
                entityName = "participant1",
                expression = "this",
                useCallCache = false,
                deleteIntermediateOutputFiles = false,
                useReferenceDisks = false,
                memoryRetryMultiplier = 1.0,
                ignoreEmptyOutputs = false
              )(studentAToken)
              // make sure the submission has not errored out
              eventually {
                val submissionStatus = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)(studentAToken)._1
                List("Accepted", "Evaluating", "Submitting", "Submitted") should contain (submissionStatus)
              }
            }(ownerAuthToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }
    }

    "should not allow writers" - {
      "to launch workflows if they don't have can-compute permission" in {
        withTemporaryBillingProject(billingAccountId) { projectName =>
          withWorkspace(projectName, prependUUID("writer-cannot-launch-workflow"), aclEntries = List(AclEntry(studentA.email, WorkspaceAccessLevel.Writer, canCompute = Some(false)))) { workspaceName =>
            Rawls.entities.importMetaData(projectName, workspaceName, entity)(ownerAuthToken)

            withMethod("writer-method-fails", MethodData.SimpleMethod) { methodName =>
              val method = MethodData.SimpleMethod.copy(methodName = methodName)

              Rawls.methodConfigs.createMethodConfigInWorkspace(projectName, workspaceName,
                method, method.methodNamespace, method.methodName, 1,
                SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)(ownerAuthToken)

              Orchestration.methods.setMethodPermissions(
                method.methodNamespace,
                method.methodName,
                method.snapshotId,
                studentA.email,
                "OWNER"
              )(ownerAuthToken)

              val submissionException = intercept[RestException] {
                Rawls.submissions.launchWorkflow(
                  billingProject = projectName,
                  workspaceName = workspaceName,
                  methodConfigurationNamespace = method.methodNamespace,
                  methodConfigurationName = method.methodName,
                  entityType = method.rootEntityType,
                  entityName = "participant1",
                  expression = "this",
                  useCallCache = false,
                  deleteIntermediateOutputFiles = false,
                  useReferenceDisks = false,
                  memoryRetryMultiplier = 1.0,
                  ignoreEmptyOutputs = false
                )(studentAToken)
              }
              assertExceptionStatusCode(submissionException, 403)
            }(ownerAuthToken)
          }(ownerAuthToken)
        }(owner.makeAuthToken(billingScopes))
      }
    }
  }

  private def workspaceResponse(response: String): WorkspaceResponse = response.parseJson.convertTo[WorkspaceResponse]

  private def assertNoAccessToWorkspace(projectName: String, workspaceName: String)(implicit token: AuthToken): Unit = {
    eventually {
      val exception = intercept[RestException](Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(token))
      assertExceptionStatusCode(exception, 404)
    }
  }

  private def assertExceptionStatusCode(exception: RestException, statusCode: Int): Unit = {
    exception.message.parseJson.asJsObject.fields("statusCode").convertTo[Int] should be(statusCode)
  }

  private def prependUUID(suffix: String): String = { s"${UUID.randomUUID().toString()}-$suffix" }
}
