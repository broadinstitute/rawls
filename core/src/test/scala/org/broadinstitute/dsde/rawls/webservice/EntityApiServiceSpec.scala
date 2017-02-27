package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.SortDirections.{Descending, Ascending}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.util.Random

/**
 * Created by dvoet on 4/24/15.
 */
class EntityApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "EntityApi" should "return 404 on Entity CRUD when workspace does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}x/entities", httpJson(testData.sample2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "update the workspace last modified date on entity rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", httpJson(EntityName("s2_changed"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "update the workspace last modified date on entity update" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "update the workspace last modified date on entity post and copy" in withTestDataApiServices { services =>

    val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeString("c")))
    val z1 = Entity("z1", "Sample", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("splat") -> attributeList))
    val workspaceSrcName = new WorkspaceName(testData.wsName.namespace, testData.wsName.name + "_copy_source")
    val workspaceSrcRequest = WorkspaceRequest(
      workspace2Name.namespace,
      workspace2Name.name,
      None,
      Map.empty
    )

    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))

    Post("/workspaces", httpJson(workspaceSrcRequest)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        Post(s"/workspaces/${workspaceSrcRequest.namespace}/${workspaceSrcRequest.name}/entities", httpJson(z1)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created, response.entity.asString) {
              status
            }
            assertResult(z1) {
              val ws2 = runAndWait(workspaceQuery.findByName(workspace2Name)).get
              runAndWait(entityQuery.get(SlickWorkspaceContext(ws2), z1.entityType, z1.name)).get
            }

            val sourceWorkspace = WorkspaceName(workspaceSrcRequest.namespace, workspaceSrcRequest.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
              sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created, response.entity.asString) {
                  status
                }
              }
            Get(s"/workspaces/${workspaceSrcRequest.namespace}/${workspaceSrcRequest.name}") ~>
              sealRoute(services.workspaceRoutes) ~>
              check {
                assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
              }
            Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}") ~>
              sealRoute(services.workspaceRoutes) ~>
              check {
                assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
              }
          }
      }
  }

  it should "update the workspace last modified date on entity batchUpsert" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "update the workspace last modified date on entity batchUpdate" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)      }
  }

  it should "return 201 on create entity" in withTestDataApiServices { services =>
    val wsName = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(newSample) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), newSample.entityType, newSample.name)).get
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(wsName)))))) {
          header("Location")
        }
      }
  }

  it should "return 409 conflict on create entity when entity exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(testData.sample2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 403 on create entity with invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val wsName = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map(AttributeName(invalidAttrNamespace, "attribute") -> AttributeString("foo")))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(invalidAttrNamespace))
      }
  }

  it should "return 201 on create entity with library-namespace attributes as curator" in withTestDataApiServices { services =>
    val wsName = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(newSample) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), newSample.entityType, newSample.name)).get
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(wsName)))))) {
          header("Location")
        }
      }
  }

  it should "return 403 on create entity with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val wsName = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(AttributeName.libraryNamespace))
      }
  }

  it should "return 400 when batch upserting an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 204 when batch upserting an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("newSample", "Sample", Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity("newSample", "Sample", Map(AttributeName.withDefaultNS("newAttribute") -> AttributeString("foo"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "Sample", "newSample"))
        }
      }
  }

  it should "return 204 when batch upserting an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
        assertResult(Some(Entity(testData.sample2.name, testData.sample2.entityType, testData.sample2.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("baz"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name))
        }
      }
  }

  it should "return 204 when batch upserting an entities with valid references" in withTestDataApiServices { services =>
    val newEntity = Entity("new_entity", testData.sample2.entityType, Map.empty)
    val referenceList = AttributeEntityReferenceList(Seq(testData.sample2.toReference, newEntity.toReference))
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), referenceList)))
    val update2 = EntityUpdateDefinition(newEntity.name, newEntity.entityType, Seq.empty)
    val blep = httpJson(Seq(update1, update2))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, response.entity.asString) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> referenceList)))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
        assertResult(Some(newEntity)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), newEntity.entityType, newEntity.name))
        }
      }
  }

  it should "return 400 when batch upserting an entity with references that don't exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeEntityReference("bar", "baz"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeEntityReference("bar", "bing"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(2) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 403 when batch upserting an entity with invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute1"), AttributeString("smee"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute2"), AttributeString("blee"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 204 when batch upserting an entity with library-namespace attributes as curator" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, response.entity.asString) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName(AttributeName.libraryNamespace, "newAttribute1") -> AttributeString("wang"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
        assertResult(Some(Entity(testData.sample2.name, testData.sample2.entityType, testData.sample2.attributes + (AttributeName(AttributeName.libraryNamespace, "newAttribute2") -> AttributeString("chung"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name))
        }
      }
  }

  it should "return 403 when batch upserting an entity with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 400 when batch updating an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 400 when batch updating an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition("superDuperNewSample", "Samples", Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        assertResult(1) {
          responseAs[ErrorReport].causes.length
        }
      }
  }

  it should "return 204 when batch updating an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
      }
  }

  it should "return 403 when batch updating an entity with invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute1"), AttributeString("smee"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute2"), AttributeString("blee"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 204 when batch updating an entity with library-namespace attributes as curator" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, response.entity.asString) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName(AttributeName.libraryNamespace, "newAttribute1") -> AttributeString("wang"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
        assertResult(Some(Entity(testData.sample2.name, testData.sample2.entityType, testData.sample2.attributes + (AttributeName(AttributeName.libraryNamespace, "newAttribute2") -> AttributeString("chung"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name))
        }
      }
  }

  it should "return 403 when batch updating an entity with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 200 on get entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(testData.sample2) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name)).get
        }
        assertResult(testData.sample2) {
          responseAs[Entity]
        }
      }
  }

  it should "return 200 on list entity types" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val entityTypes = runAndWait(entityQuery.getEntityTypeMetadata(SlickWorkspaceContext(testData.workspace)))
        assertResult(entityTypes) {
          responseAs[Map[String, EntityTypeMetadata]]
        }
      }
  }

  it should "return 200 on list all samples" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val samples = runAndWait(entityQuery.list(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType))
        assertSameElements(responseAs[Array[Entity]], samples)
      }
  }

  it should "return 404 on non-existing entity" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name)).get.attributes.get(AttributeName.withDefaultNS("boo"))
        }
      }
  }

  it should "return 200 on remove attribute from entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("bar")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name)).get.attributes.get(AttributeName.withDefaultNS("bar"))
        }
      }
  }

  it should "return 404 on update to non-existing entity" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}x", httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 on updating entity with invalid-namespace attributes" in withTestDataApiServices { services =>
    val name = AttributeName("invalid", "misc")
    val attr = AttributeString("meh")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 200 on updating entity with library-namespace attributes as curator" in withTestDataApiServices { services =>
    val name = AttributeName(AttributeName.libraryNamespace, "reader")
    val attr = AttributeString("me")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(attr)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name)).get.attributes.get(name)
        }
      }
  }

  it should "return 403 on updating entity (add) with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val name = AttributeName(AttributeName.libraryNamespace, "reader")
    val attr = AttributeString("me")

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 403 on updating entity (remove) with library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    val name = AttributeName(AttributeName.libraryNamespace, "reader")
    val attr = AttributeString("me")

    // first add as curator

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(attr)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name)).get.attributes.get(name)
        }
      }

    // then remove as non-curator

    revokeCuratorRole(services)

    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(RemoveAttribute(name): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}", httpJson(Seq(RemoveListMember(AttributeName.withDefaultNS("grip"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample1.entityType}/${testData.sample1.name}", httpJson(Seq(AddListMember(AttributeName.withDefaultNS("somefoo"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", httpJson(EntityName("sample1"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}/rename", httpJson(EntityName("s2_changed"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(true) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, "s2_changed")).isDefined
        }
      }
  }

  it should "return 404 on entity rename, entity does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/foox/rename", httpJson(EntityName("s2_changed"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, "s2_changed"))
        }
      }
  }

  /*
    following 2 tests disabled until entity deletion is re-implemented (see GAWB-422)
   */
  ignore should "*DISABLED* return 204 on entity delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample2.entityType, testData.sample2.name))
        }
      }
  }
  ignore should "*DISABLED* return 404 entity delete, entity does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/${testData.sample2.entityType}/s2_changed") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "this.samples.type")) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array("normal", "tumor", "tumor")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 400 on failing to parse an expression" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities/SampleSet/sset1/evaluate", HttpEntity(ContentTypes.`application/json`, "nonexistent.anything")) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeString("c")))
  val z1 = Entity("z1", "Sample", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("splat") -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace, testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    None,
    Map.empty
  )

  it should "return 201 for copying entities into a workspace with no conflicts" in withTestDataApiServices { services =>
    Post("/workspaces", httpJson(workspace2Request)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(workspace2Request) {
          val ws = runAndWait(workspaceQuery.findByName(workspace2Request.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }

        Post(s"/workspaces/${workspace2Request.namespace}/${workspace2Request.name}/entities", httpJson(z1)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created, response.entity.asString) {
              status
            }
            assertResult(z1) {
              val ws2 = runAndWait(workspaceQuery.findByName(workspace2Name)).get
              runAndWait(entityQuery.get(SlickWorkspaceContext(ws2), z1.entityType, z1.name)).get
            }

            val sourceWorkspace = WorkspaceName(workspace2Request.namespace, workspace2Request.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
              sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created, response.entity.asString) {
                  status
                }
                assertResult(z1) {
                  runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), z1.entityType, z1.name)).get
                }
              }
          }
      }
  }

  it should "return 201 for copying entities with existing library-namespace attributes as non-curator" in withTestDataApiServices { services =>
    Post("/workspaces", httpJson(workspace2Request)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(workspace2Request) {
          val ws = runAndWait(workspaceQuery.findByName(workspace2Request.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.realm, ws.attributes)
        }

        // first add Entity as curator

        val libraryEnt = z1.copy(attributes = z1.attributes + (AttributeName(AttributeName.libraryNamespace, "whatever") -> AttributeNumber(1.23456)))

        Post(s"/workspaces/${workspace2Request.namespace}/${workspace2Request.name}/entities", httpJson(libraryEnt)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created, response.entity.asString) {
              status
            }
            assertResult(libraryEnt) {
              val ws2 = runAndWait(workspaceQuery.findByName(workspace2Name)).get
              runAndWait(entityQuery.get(SlickWorkspaceContext(ws2), libraryEnt.entityType, libraryEnt.name)).get
            }

            // then copy Entity as non-curator

            revokeCuratorRole(services)

            val sourceWorkspace = WorkspaceName(workspace2Request.namespace, workspace2Request.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, libraryEnt.entityType, Seq(libraryEnt.name))
            Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
              sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created, response.entity.asString) {
                  status
                }
                assertResult(libraryEnt) {
                  runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), libraryEnt.entityType, libraryEnt.name)).get
                }
              }
          }
      }
  }

  it should "return 409 for copying entities into a workspace with conflicts" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("sample1"))
    Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 422 when copying entities from a Realm-protected workspace into one not in that Realm" in withTestDataApiServices { services =>
    val srcWorkspace = testData.workspaceWithRealm
    val srcWorkspaceName = srcWorkspace.toWorkspaceName

    // add an entity to a workspace with a Realm

    Post(s"/workspaces/${srcWorkspace.namespace}/${srcWorkspace.name}/entities", httpJson(z1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(z1) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(srcWorkspace), z1.entityType, z1.name)).get
        }
      }

    // attempt to copy an entity to a workspace with the wrong Realm

    val newRealm = RawlsGroup(RawlsGroupName("a-new-realm-for-testing"), RawlsGroupEmail("president@realm.example.com"), Set(testData.userOwner), Set.empty)
    val newRealmRef: RawlsRealmRef = RawlsRealmRef(newRealm.groupName)

    runAndWait(rawlsGroupQuery.save(newRealm))
    runAndWait(rawlsGroupQuery.setGroupAsRealm(newRealmRef))

    val wrongRealmCloneRequest = WorkspaceRequest(namespace = testData.workspace.namespace, name = "copy_add_realm", Option(newRealmRef), Map.empty)
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/clone", httpJson(wrongRealmCloneRequest)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(Some(newRealmRef)) {
          responseAs[Workspace].realm
        }
      }

    val destWorkspaceWrongRealmName = wrongRealmCloneRequest.toWorkspaceName

    val wrongRealmCopyDef = EntityCopyDefinition(srcWorkspaceName, destWorkspaceWrongRealmName, "Sample", Seq("z1"))
    Post("/workspaces/entities/copy", httpJson(wrongRealmCopyDef)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }

    // attempt to copy an entity to a workspace with no Realm

    val destWorkspaceNoRealmName = testData.workspace.toWorkspaceName

    val noRealmCopyDef = EntityCopyDefinition(srcWorkspaceName, destWorkspaceNoRealmName, "Sample", Seq("z1"))
    Post("/workspaces/entities/copy", httpJson(noRealmCopyDef)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "not allow dots in user-defined strings" in withTestDataApiServices { services =>
    val dotSample = Entity("sample.with.dots.in.name", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/entities", httpJson(dotSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  class PaginationTestData extends TestData {
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set())
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set())

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

    val numEntities = 100
    val vocab1Strings = Map(0 -> "foo", 1 -> "bar", 2 -> "baz")
    val vocab2Strings = Map(0 -> "bim", 1 -> "bam")
    val entityType = "page_entity"
    val entities = Random.shuffle(for (i <- 1 to numEntities) yield Entity(s"entity_$i", entityType, Map(
      AttributeName.withDefaultNS("number") -> AttributeNumber(Math.random()),
      AttributeName.withDefaultNS("random") -> AttributeString(UUID.randomUUID().toString),
      AttributeName.withDefaultNS("sparse") -> (if (i % 2 == 0) AttributeNull else AttributeNumber(i.toDouble)),
      AttributeName.withDefaultNS("vocab1") -> AttributeString(vocab1Strings(i % vocab1Strings.size)),
      AttributeName.withDefaultNS("vocab2") -> AttributeString(vocab2Strings(i % vocab2Strings.size)),
      AttributeName.withDefaultNS("mixed") -> (i % 3 match {
        case 0 => AttributeString(s"$i")
        case 1 => AttributeNumber(i.toDouble)
        case 2 => AttributeValueList(1 to i map (AttributeNumber(_)) reverse)
      }),
      AttributeName.withDefaultNS("mixedNumeric") -> (i % 2 match {
        case 0 => AttributeNumber(i.toDouble)
        case 1 => AttributeValueList(1 to i map (AttributeNumber(_)) reverse)
      })
    )))

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        rawlsUserQuery.save(userOwner),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        workspaceQuery.save(workspace),
        entityQuery.save(SlickWorkspaceContext(workspace), entities)
      )
    }
  }

  val paginationTestData = new PaginationTestData()

  def withPaginationTestDataApiServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(paginationTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  val defaultQuery = EntityQuery(1, 10, "name", Ascending, None)

  def calculateNumPages(count: Int, pageSize: Int) = Math.ceil(count.toDouble / pageSize).toInt

  it should "return 400 bad request on entity query when page is not a number" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?page=asdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page size is not a number" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?pageSize=asdfasdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page is <= 0" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?page=-1") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page size is <= 0" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?pageSize=-1") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page > page count" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?page=10000000") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query for unknown sort direction" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortDirection=asdfasdfasdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 200 OK on entity query for unknown sort field" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=asdfasdfasdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "asdfasdfasdf"),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.name).take(defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return 404 not found on entity query for workspace that does not exist" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}xxx/entityQuery/${paginationTestData.entityType}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 OK on entity query when no query params are given" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery,
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.name).take(defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return 200 OK on entity query when there are no entities of given type" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/blarf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery,
          EntityQueryResultMetadata(0, 0, 0),
          Seq.empty)) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return 200 OK on entity query when all results are filtered" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?filterTerms=qqq") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(filterTerms = Option("qqq")),
          EntityQueryResultMetadata(paginationTestData.entities.size, 0, 0),
          Seq.empty)) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return the right page on entity query" in withPaginationTestDataApiServices { services =>
    val page = 5
    val offset = (page - 1) * defaultQuery.pageSize
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?page=$page") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(page = page),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.name).slice(offset, offset + defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return the right page size on entity query" in withPaginationTestDataApiServices { services =>
    val pageSize = 23
    val offset = (defaultQuery.page - 1) * pageSize
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?pageSize=$pageSize") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(pageSize = pageSize),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, pageSize)),
          paginationTestData.entities.sortBy(_.name).slice(offset, offset + pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return sorted results on entity query for number field" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=number") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "number"),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.attributes(AttributeName.withDefaultNS("number")).asInstanceOf[AttributeNumber].value).take(defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return sorted results on entity query for string field" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=random&sordDirection=asc") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "random"),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.attributes(AttributeName.withDefaultNS("random")).asInstanceOf[AttributeString].value).take(defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  private def entityLessThan(attributeName: String)(e1: Entity, e2: Entity): Boolean = {
    (e1.attributes(AttributeName.withDefaultNS(attributeName)), e2.attributes(AttributeName.withDefaultNS(attributeName))) match {
      case (AttributeNumber(v1), AttributeNumber(v2)) if v1 == v2 => e1.name < e2.name
      case (AttributeNumber(v1), AttributeNumber(v2)) => v1 < v2
      case (AttributeNumber(v), _) => true
      case (_, AttributeNumber(v)) => false

      case (AttributeString(v1), AttributeString(v2)) if v1 == v2 => e1.name < e2.name
      case (AttributeString(v1), AttributeString(v2)) => v1 < v2
      case (AttributeString(v), _) => true
      case (_, AttributeString(v)) => false

      case (l1: AttributeList[_], l2: AttributeList[_]) if l1.list.length == l2.list.length => e1.name < e2.name
      case (l1: AttributeList[_], l2: AttributeList[_]) => l1.list.length < l2.list.length
      case (l1: AttributeList[_], _) => true
      case (_, l2: AttributeList[_]) => false

      case (_, _) => throw new RawlsException("case not covered")

    }
  }

  it should "return sorted results on entity query for mixed typed field" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=mixed&pageSize=${paginationTestData.numEntities}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "mixed", pageSize = paginationTestData.numEntities),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, 1),
          paginationTestData.entities.sortWith(entityLessThan("mixed")))) {
          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return sorted results on entity query for mixed numeric-type field including lists" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=mixedNumeric&pageSize=${paginationTestData.numEntities}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "mixedNumeric", pageSize = paginationTestData.numEntities),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, 1),
          paginationTestData.entities.sortWith(entityLessThan("mixedNumeric")))) {
          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return sorted results on entity query for sparse field" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=sparse&pageSize=${paginationTestData.numEntities}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }

        // all the entities without values should be last, drop them then make sure the resulting list is sorted right
        val resultEntities = responseAs[EntityQueryResponse].results
        assertResult(paginationTestData.entities.filter(_.attributes.getOrElse(AttributeName.withDefaultNS("sparse"), AttributeNull) != AttributeNull).sortBy(_.attributes(AttributeName.withDefaultNS("sparse")).asInstanceOf[AttributeNumber].value)) {
          resultEntities.dropWhile(_.attributes.getOrElse(AttributeName.withDefaultNS("sparse"), AttributeNull) == AttributeNull)
        }
      }
  }

  it should "return sorted results on entity query descending" in withPaginationTestDataApiServices { services =>
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?sortField=random&sortDirection=desc") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(sortField = "random", sortDirection = Descending),
          EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)),
          paginationTestData.entities.sortBy(_.attributes(AttributeName.withDefaultNS("random")).asInstanceOf[AttributeString].value).reverse.take(defaultQuery.pageSize))) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return filtered results on entity query" in withPaginationTestDataApiServices { services =>
    val pageSize = paginationTestData.entities.size
    val vocab1Term = paginationTestData.vocab1Strings(0)
    val vocab2Term = paginationTestData.vocab2Strings(1)
    val expectedEntities = paginationTestData.entities.
      filter(e => e.attributes(AttributeName.withDefaultNS("vocab1")) == AttributeString(vocab1Term) && e.attributes(AttributeName.withDefaultNS("vocab2")) == AttributeString(vocab2Term)).
      sortBy(_.name)
    Get(s"/workspaces/${paginationTestData.workspace.namespace}/${paginationTestData.workspace.name}/entityQuery/${paginationTestData.entityType}?pageSize=$pageSize&filterTerms=$vocab1Term%20$vocab2Term") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(EntityQueryResponse(
          defaultQuery.copy(pageSize = pageSize, filterTerms = Option(s"$vocab1Term $vocab2Term")),
          EntityQueryResultMetadata(paginationTestData.numEntities, expectedEntities.size, calculateNumPages(expectedEntities.size, pageSize)),
          expectedEntities)) {

          responseAs[EntityQueryResponse]
        }
      }
  }
}
