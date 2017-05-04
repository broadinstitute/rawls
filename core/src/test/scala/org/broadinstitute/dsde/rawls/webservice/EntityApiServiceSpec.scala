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

  def withConstantTestDataApiServices[T](testCode: TestApiService => T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def dbId(ent: Entity): Long = runAndWait(entityQuery.getEntityRecords(SlickWorkspaceContext(testData.workspace).workspaceId, Set(ent.toReference))).head.id
  def dbName(id: Long): String = runAndWait(entityQuery.getEntities(Seq(id))).head._2.name

  "EntityApi" should "return 404 on Entity CRUD when workspace does not exist" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.copy(name = "DNE").path}/entities", httpJson(testData.sample2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "update the workspace last modified date on entity rename" in withTestDataApiServices { services =>
    Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("s2_changed"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "update the workspace last modified date on entity update" in withTestDataApiServices { services =>
    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    Get(testData.workspace.path) ~>
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

        Post(s"${workspaceSrcRequest.path}/entities", httpJson(z1)) ~>
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
            Get(workspaceSrcRequest.path) ~>
              sealRoute(services.workspaceRoutes) ~>
              check {
                assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
              }
            Get(testData.wsName.path) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }
  }

  it should "update the workspace last modified date on entity batchUpdate" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(Entity(testData.sample1.name, testData.sample1.entityType, testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))))) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)      }
  }

  it should "return 201 on create entity" in withTestDataApiServices { services =>
    val newSample = Entity("sampleNew", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
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

        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(testData.workspace)))))) {
          header("Location")
        }
      }
  }

  it should "return 409 conflict on create entity when entity exists" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities", httpJson(testData.sample2)) ~>
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

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(invalidAttrNamespace))
      }
  }

  it should "return 403 on create entity with library-namespace attributes " in withTestDataApiServices { services =>
    revokeCuratorRole(services)

    val wsName = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newSample = Entity("sampleNew", "sample", Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  // entity and attribute counts, regardless of deleted status
  def countEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listEntities(SlickWorkspaceContext(testData.workspace)))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  // entity and attribute counts, non-deleted only
  def countActiveEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listActiveEntities(SlickWorkspaceContext(testData.workspace)))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  it should "return 204 on unreferenced entity delete" in withTestDataApiServices { services =>
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1)(entityCount2)
    assertResult(attributeCount1)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }

  it should "return 204 on entity delete covering all references" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val e1 = Entity("e1", "Generic", Map.empty)
    val e2 = Entity("e2", "Generic", Map(AttributeName.withDefaultNS("link") -> e1.toReference))

    Post(s"${testData.workspace.path}/entities", httpJson(e1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e1, e2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e2.entityType, e2.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 2)(entityCount2)
    assertResult(attributeCount1 + 1)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }


  it should "return 204 on entity delete covering all references including cycles" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val e1 = Entity("e1", "Generic", Map.empty)
    val e2 = Entity("e2", "Generic", Map(AttributeName.withDefaultNS("link") -> e1.toReference))
    val e3 = Entity("e3", "Generic", Map(AttributeName.withDefaultNS("link") -> e2.toReference))

    Post(s"${testData.workspace.path}/entities", httpJson(e1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e3)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val update = EntityUpdateDefinition(e1.name, e1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("link"), e3.toReference)))
    val new_e1 = e1.copy(attributes = Map(AttributeName.withDefaultNS("link") -> e3.toReference))

    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(new_e1)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e1, e2, e3))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e2.entityType, e2.name))
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e3.entityType, e3.name))
        }
      }


    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 3)(entityCount2)
    assertResult(attributeCount1 + 3)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }

  it should "return 409 with the set of references on referenced entity delete" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val e1 = Entity("e1", "Generic", Map.empty)
    val e2 = Entity("e2", "Generic", Map(AttributeName.withDefaultNS("link") -> e1.toReference))

    Post(s"${testData.workspace.path}/entities", httpJson(e1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }

        val expected = Seq(e1, e2) map  { _.toReference }
        assertSameElements(expected, responseAs[Seq[AttributeEntityReference]])
        assertResult(Some(e1)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
        assertResult(Some(e2)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e2.entityType, e2.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 2)(entityCount2)
    assertResult(attributeCount1 + 1)(attributeCount2)
    assertResult(activeEntityCount1 + 2)(activeEntityCount2)
    assertResult(activeAttributeCount1 + 1)(activeAttributeCount2)
  }

  it should "return 409 with the set of references on referenced entity delete with a cycle" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val e1 = Entity("e1", "Generic", Map.empty)
    val e2 = Entity("e2", "Generic", Map(AttributeName.withDefaultNS("link") -> e1.toReference))
    val e3 = Entity("e3", "Generic", Map(AttributeName.withDefaultNS("link") -> e2.toReference))

    Post(s"${testData.workspace.path}/entities", httpJson(e1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities", httpJson(e3)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val update = EntityUpdateDefinition(e1.name, e1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("link"), e3.toReference)))
    val new_e1 = e1.copy(attributes = Map(AttributeName.withDefaultNS("link") -> e3.toReference))

    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(new_e1)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
        val expected = Seq(e1, e2, e3) map { _.toReference }
        assertSameElements(expected, responseAs[Seq[AttributeEntityReference]])
        assertResult(Some(new_e1)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e1.entityType, e1.name))
        }
        assertResult(Some(e2)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e2.entityType, e2.name))
        }
        assertResult(Some(e3)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e3.entityType, e3.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 3)(entityCount2)
    assertResult(attributeCount1 + 3)(attributeCount2)
    assertResult(activeEntityCount1 + 3)(activeEntityCount2)
    assertResult(activeAttributeCount1 + 3)(activeAttributeCount2)
  }

  it should "return 400 on entity delete where not all entities exist" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val request = EntityDeleteRequest(testData.sample2.copy(name = "DNE1"), testData.sample2.copy(name = "DNE2"))

    Post(s"${testData.workspace.path}/entities/delete", httpJson(request)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1)(entityCount2)
    assertResult(attributeCount1)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }

  it should "return 400 on entity delete where some entities do not exist" in withTestDataApiServices { services =>
    val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

    val request = EntityDeleteRequest(testData.sample2, testData.sample2.copy(name = "DNE"))

    Post(s"${testData.workspace.path}/entities/delete", httpJson(request)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1)(entityCount2)
    assertResult(attributeCount1)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }

  it should "return 201 on create entity for a previously deleted entity" in withTestDataApiServices { services =>
    val attrs1 = Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("alive") -> AttributeBoolean(false))
    val attrs2 = Map(AttributeName.withDefaultNS("type") -> AttributeString("normal"), AttributeName.withDefaultNS("answer") -> AttributeNumber(42))
    val attrs3 = Map(AttributeName.withDefaultNS("cout") -> AttributeString("wackwack"))

    val sample = Entity("ABCD", "Sample", attrs1)

    Post(s"${testData.workspace.path}/entities", httpJson(sample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(sample) {
          responseAs[Entity]
        }
        assertResult(sample) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), sample.entityType, sample.name)).get
        }
      }

    val oldId = dbId(sample)

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(sample))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    val sampleNewAttrs = sample.copy(attributes = attrs2)

    Post(s"${testData.workspace.path}/entities", httpJson(sampleNewAttrs)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(sampleNewAttrs) {
          responseAs[Entity]
        }
        assertResult(sampleNewAttrs) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), sample.entityType, sample.name)).get
        }
      }

    val newId = dbId(sampleNewAttrs)
    assert(oldId != newId)

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(sampleNewAttrs))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    val sampleAttrs3 = sample.copy(attributes = attrs3)

    Post(s"${testData.workspace.path}/entities", httpJson(sampleAttrs3)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(sampleAttrs3) {
          responseAs[Entity]
        }
        assertResult(sampleAttrs3) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), sample.entityType, sample.name)).get
        }
      }

    val id3 = dbId(sampleAttrs3)

    assert(oldId != id3)
    assert(newId != id3)
  }

  it should "return 400 when batch upserting an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1))) ~>
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

  it should "return 204 when batch upserting an entity that was previously deleted" in withTestDataApiServices { services =>

    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val oldId = dbId(e)

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    val update = EntityUpdateDefinition(e.name, e.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz"))))
    val updatedEntity = e.copy(attributes = Map(AttributeName.withDefaultNS("newAttribute") -> AttributeString("baz")))
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(Some(updatedEntity)) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    val newId = dbId(e)

    assert { oldId != newId }
  }

  it should "return 204 when batch upserting an entity with valid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz"))))
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 403 when batch upserting an entity with library-namespace attributes " in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) {
          status
        }
      }
  }

  it should "return 400 when batch updating an entity with invalid update operations" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a"))))
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
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

  it should "return 400 when batch updating an entity that was deleted" in withTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    val update1 = EntityUpdateDefinition(e.name, e.entityType, Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz"))))
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
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
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
          status
        }
      }
  }

  it should "return 403 when batch updating an entity with library-namespace attributes" in withTestDataApiServices { services =>
    revokeCuratorRole(services)
    val update1 = EntityUpdateDefinition(testData.sample1.name, testData.sample1.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang"))))
    val update2 = EntityUpdateDefinition(testData.sample2.name, testData.sample2.entityType, Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))))
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) {
          status
        }
      }
  }

  it should "return 200 on get entity" in withTestDataApiServices { services =>
    Get(testData.sample2.path(testData.workspace)) ~>
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

  // metadata attribute name sequences have undefined ordering, so can't compare directly
  def assertMetadataMapsEqual(a: Map[String, EntityTypeMetadata], b: Map[String, EntityTypeMetadata]): Unit = {
    assert(a.size == b.size, "Maps are not equal: sizes differ")
    assertSameElements(a.keySet, b.keySet)
    a.keysIterator.foreach { key =>
      val aMeta = a(key)
      val bMeta = b(key)
      assert(aMeta.count == bMeta.count, s"Map counts differ for Entity Type $key")
      assert(aMeta.idName == bMeta.idName, s"ID names counts differ for Entity Type $key")
      assertSameElements(aMeta.attributeNames, bMeta.attributeNames)
    }
  }

  val expectedMetadataMap: Map[String, EntityTypeMetadata] = Map(
    "Sample" -> EntityTypeMetadata(8, "Sample_id", Seq("quot", "somefoo", "thingies", "type", "whatsit", "confused", "tumortype", "cycle")),
    "Aliquot" -> EntityTypeMetadata(2, "Aliquot_id", Seq()),
    "Pair" -> EntityTypeMetadata(2, "Pair_id", Seq("case", "control", "whatsit")),
    "SampleSet" -> EntityTypeMetadata(5, "SampleSet_id", Seq("samples", "hasSamples")),
    "PairSet" -> EntityTypeMetadata(1, "PairSet_id", Seq("pairs")),
    "Individual" -> EntityTypeMetadata(2, "Individual_id", Seq("sset"))
  )
  it should "return 200 on list entity types" in withConstantTestDataApiServices { services =>
    Get(s"${constantData.workspace.path}/entities") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertMetadataMapsEqual(expectedMetadataMap, responseAs[Map[String, EntityTypeMetadata]])
        assertMetadataMapsEqual(expectedMetadataMap, runAndWait(entityQuery.getEntityTypeMetadata(SlickWorkspaceContext(constantData.workspace))))
      }
  }

  it should "not include deleted entities in EntityTypeMetadata" in withConstantTestDataApiServices { services =>
    val newSample = Entity("foo", "Sample", Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(123)))

    Post(s"${constantData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(newSample))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    Get(s"${constantData.workspace.path}/entities") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertMetadataMapsEqual(expectedMetadataMap, responseAs[Map[String, EntityTypeMetadata]])
        assertMetadataMapsEqual(expectedMetadataMap, runAndWait(entityQuery.getEntityTypeMetadata(SlickWorkspaceContext(constantData.workspace))))
      }
  }

  it should "return 200 on list all samples" in withConstantTestDataApiServices { services =>
    val expected = Seq(constantData.sample1,
      constantData.sample2,
      constantData.sample3,
      constantData.sample4,
      constantData.sample5,
      constantData.sample6,
      constantData.sample7,
      constantData.sample8)

    Get(s"${constantData.workspace.path}/entities/Sample") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(SlickWorkspaceContext(constantData.workspace), "Sample"))
        assertSameElements(responseAs[Array[Entity]], expected)
        assertSameElements(dbSamples, expected)
      }
  }

  it should "not list deleted samples" in withConstantTestDataApiServices { services =>
    val expected = Seq(constantData.sample1,
      constantData.sample2,
      constantData.sample3,
      constantData.sample4,
      constantData.sample5,
      constantData.sample6,
      constantData.sample7,
      constantData.sample8)

    val newSample = Entity("foo", "Sample", Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(123)))

    Post(s"${constantData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Get(s"${constantData.workspace.path}/entities/Sample") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(SlickWorkspaceContext(constantData.workspace), "Sample"))
        assertSameElements(responseAs[Array[Entity]], expected :+ newSample)
        assertSameElements(dbSamples, expected :+ newSample)
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(newSample))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    Get(s"${constantData.workspace.path}/entities/Sample") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(SlickWorkspaceContext(constantData.workspace), "Sample"))
        assertSameElements(responseAs[Array[Entity]], expected)
        assertSameElements(dbSamples, expected)
      }
  }

  it should "return 404 on get non-existing entity" in withTestDataApiServices { services =>
    Get(testData.sample2.copy(name = "DNE").path(testData.workspace)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on get deleted entity accessed by its hidden name" in withTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(123)))

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Get(e.path(testData.workspace)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(e) {
          responseAs[Entity]
        }
      }

    val id = dbId(e)
    val oldName = dbName(id)

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    val newName = dbName(id)
    assert(oldName != newName)

    Get(e.path(testData.workspace)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

    val newEnt = e.copy(name = newName)

    Get(newEnt.path(testData.workspace)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val respEnt = responseAs[Entity]
        assertResult(e.entityType) { respEnt.entityType }
        assert(e.name != respEnt.name)
        assertResult(newEnt.name) { respEnt.name }

        assertResult(1) { respEnt.attributes.size }

        // same attribute namespace and value but the attribute name has been hidden/renamed on deletion
        val respAttr = respEnt.attributes.head
        val eAttr = e.attributes.head

        assertResult(eAttr._1.namespace) { respAttr._1.namespace }
        assert(respAttr._1.name.contains(eAttr._1.name + "_"))
        assertResult(eAttr._2) { respAttr._2 }
      }
  }

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
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
    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("bar")): AttributeUpdateOperation))) ~>
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
    Patch(testData.sample2.copy(name = "DNE").path(testData.workspace), httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 on update to a deleted entity" in withTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    Patch(e.path(testData.workspace), httpJson(Seq(AddUpdateAttribute(AttributeName.withDefaultNS("baz"), AttributeString("bang")): AttributeUpdateOperation))) ~>
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

    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 403 on updating entity with library-namespace attributes " in withTestDataApiServices { services =>
    val name = AttributeName(AttributeName.libraryNamespace, "reader")
    val attr = AttributeString("me")

    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[String]) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(testData.sample2.path(testData.workspace), httpJson(Seq(RemoveListMember(AttributeName.withDefaultNS("grip"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(testData.sample1.path(testData.workspace), httpJson(Seq(AddListMember(AttributeName.withDefaultNS("somefoo"), AttributeString("adsf")): AttributeUpdateOperation))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 409 on entity rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("sample1"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on entity rename" in withTestDataApiServices { services =>
    Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("s2_changed"))) ~>
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
    Post(s"${testData.sample2.copy(name = "foox").path(testData.workspace)}/rename", httpJson(EntityName("s2_changed"))) ~>
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

  it should "return 404 on entity rename, entity was deleted" in withTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    Post(s"${e.path(testData.workspace)}/rename", httpJson(EntityName("baz"))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 on successfully parsing an expression" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate", httpJsonStr("this.samples.type")) ~>
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
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate", httpJsonStr("nonexistent.anything")) ~>
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
          WorkspaceRequest(ws.namespace, ws.name, ws.authorizationDomain, ws.attributes)
        }

        Post(s"${workspace2Request.path}/entities", httpJson(z1)) ~>
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

  it should "return 409 for copying entities into a workspace with conflicts" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("sample1"))
    Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }

        assertResult(EntityCopyResponse(Seq.empty, Seq(EntityHardConflict("Sample", "sample1")), Seq.empty)) {
          responseAs[EntityCopyResponse]
        }
      }
  }

  it should "return 409 for soft conflicts multiple levels down" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val newWorkspace = WorkspaceName(testData.workspace.namespace, "my-brand-new-workspace")

    val newWorkspaceCreate = WorkspaceRequest(newWorkspace.namespace, newWorkspace.name, None, Map.empty)

    val copyAliquot1 = EntityCopyDefinition(sourceWorkspace, newWorkspace, testData.aliquot1.entityType, Seq(testData.aliquot1.name))
    val copySample3 = EntityCopyDefinition(sourceWorkspace, newWorkspace, testData.sample3.entityType, Seq(testData.sample3.name))

    Post("/workspaces", httpJson(newWorkspaceCreate)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
      }

    Post("/workspaces/entities/copy", httpJson(copyAliquot1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        val copyResponse = responseAs[EntityCopyResponse]

        assertSameElements(Seq(testData.aliquot1).map(_.toReference), copyResponse.entitiesCopied)
        assertSameElements(Seq.empty, copyResponse.hardConflicts)
        assertSameElements(Seq.empty, copyResponse.softConflicts)
      }

    Post("/workspaces/entities/copy", httpJson(copySample3)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict, response.entity.asString) {
          status
        }

        val copyResponse = responseAs[EntityCopyResponse]

        assertSameElements(Seq.empty, copyResponse.entitiesCopied)
        assertSameElements(Seq.empty, copyResponse.hardConflicts)

        val expectedSoftConflicts = Seq(
          EntitySoftConflict(testData.sample3.entityType, testData.sample3.name, Seq(
            EntitySoftConflict(testData.sample1.entityType, testData.sample1.name, Seq(
              EntitySoftConflict(testData.aliquot1.entityType, testData.aliquot1.name, Seq.empty))))))

        assertSameElements(expectedSoftConflicts, copyResponse.softConflicts)
      }
  }

  it should "return 409 for copying entities into a workspace with subtree conflicts, but successfully copy when asked to" in withTestDataApiServices { services =>
    val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
    val entityCopyDefinition1 = EntityCopyDefinition(sourceWorkspace, testData.controlledWorkspace.toWorkspaceName, testData.sample1.entityType, Seq(testData.sample1.name))
    //this will cause a soft conflict because it references sample1
    val entityCopyDefinition2 = EntityCopyDefinition(sourceWorkspace, testData.controlledWorkspace.toWorkspaceName, testData.sample3.entityType, Seq(testData.sample3.name))

    Post("/workspaces/entities/copy", httpJson(entityCopyDefinition1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        val copyResponse = responseAs[EntityCopyResponse]

        assertSameElements(Seq(testData.sample1, testData.aliquot1).map(_.toReference), copyResponse.entitiesCopied)
        assertSameElements(Seq.empty, copyResponse.hardConflicts)
        assertSameElements(Seq.empty, copyResponse.softConflicts)
      }

    val expectedSoftConflictResponse = EntityCopyResponse(Seq.empty, Seq.empty, Seq(
      EntitySoftConflict(testData.sample3.entityType, testData.sample3.name, Seq(
        EntitySoftConflict(testData.sample1.entityType, testData.sample1.name, Seq.empty),
        EntitySoftConflict(testData.sample1.entityType, testData.sample1.name, Seq(
          EntitySoftConflict(testData.aliquot1.entityType, testData.aliquot1.name, Seq.empty)))))))

    //test the default case of no parameter set
    Post("/workspaces/entities/copy", httpJson(entityCopyDefinition2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict, response.entity.asString) {
          status
        }
        assertResult(expectedSoftConflictResponse) {
          responseAs[EntityCopyResponse]
        }
      }

    Post("/workspaces/entities/copy?linkExistingEntities=false", httpJson(entityCopyDefinition2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict, response.entity.asString) {
          status
        }
        assertResult(expectedSoftConflictResponse) {
          responseAs[EntityCopyResponse]
        }
      }

    Post("/workspaces/entities/copy?linkExistingEntities=true", httpJson(entityCopyDefinition2)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(EntityCopyResponse(Seq(testData.sample3).map(_.toReference), Seq.empty, Seq.empty)) {
          responseAs[EntityCopyResponse]
        }
      }
  }

  it should "return 422 when copying entities from a Realm-protected workspace into one not in that Realm" in withTestDataApiServices { services =>
    val srcWorkspace = testData.workspaceWithRealm
    val srcWorkspaceName = srcWorkspace.toWorkspaceName

    // add an entity to a workspace with a Realm

    Post(s"${srcWorkspace.path}/entities", httpJson(z1)) ~>
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

    val newRealm = makeManagedGroup("a-new-realm-for-testing", Set(testData.userOwner))
    runAndWait(rawlsGroupQuery.save(newRealm.membersGroup))
    runAndWait(rawlsGroupQuery.save(newRealm.adminsGroup))
    runAndWait(managedGroupQuery.createManagedGroup(newRealm))

    val wrongRealmCloneRequest = WorkspaceRequest(namespace = testData.workspace.namespace, name = "copy_add_realm", Option(newRealm), Map.empty)
    Post(s"${testData.workspace.path}/clone", httpJson(wrongRealmCloneRequest)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
        assertResult(Some(ManagedGroup.toRef(newRealm))) {
          responseAs[Workspace].authorizationDomain
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
    Post(s"${testData.workspace.path}/entities", httpJson(dotSample)) ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=asdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page size is not a number" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=asdfasdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page is <= 0" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=-1") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page size is <= 0" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=-1") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query when page > page count" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=10000000") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 bad request on entity query for unknown sort direction" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortDirection=asdfasdfasdf") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 200 OK on entity query for unknown sort field" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=asdfasdfasdf") ~>
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
    Get(s"${paginationTestData.workspace.copy(name = "DNE").path}/entityQuery/${paginationTestData.entityType}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 OK on entity query when no query params are given" in withPaginationTestDataApiServices { services =>
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/blarf") ~>
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

  it should "not return deleted entities on entity query" in withPaginationTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map.empty)

    Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(e) {
          responseAs[Entity]
        }
      }

    Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), e.entityType, e.name))
        }
      }

    Get(s"${paginationTestData.workspace.path}/entityQuery/bar") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?filterTerms=qqq") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=$page") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=$pageSize") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=number") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=random&sordDirection=asc") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=mixed&pageSize=${paginationTestData.numEntities}") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=mixedNumeric&pageSize=${paginationTestData.numEntities}") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=sparse&pageSize=${paginationTestData.numEntities}") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=random&sortDirection=desc") ~>
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
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=$pageSize&filterTerms=$vocab1Term%20$vocab2Term") ~>
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
