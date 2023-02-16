package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Location, OAuth2BearerToken}
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData}
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.SortDirections.{Ascending, Descending}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Random

/**
 * Created by dvoet on 4/24/15.
 */
class EntityApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives
  case class TestApiServiceForAuthDomains(dataSource: SlickDataSource,
                                          gcsDAO: MockGoogleServicesDAO,
                                          gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectives {
    override val samDAO: MockSamDAOForAuthDomains = new MockSamDAOForAuthDomains(dataSource)
  }
  case class TestApiServiceForMockedEntityService(dataSource: SlickDataSource,
                                                  gcsDAO: MockGoogleServicesDAO,
                                                  gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectives {
    val service = mock[EntityService]
    override val entityServiceConstructor = RawlsRequestContext => service
    when(
      service.entityTypeMetadata(any[WorkspaceName],
                                 any[Option[DataReferenceName]],
                                 any[Option[GoogleProjectId]],
                                 any[Boolean]
      )
    )
      .thenReturn(
        Future.successful(Map("Test" -> EntityTypeMetadata(5000, "for-test", List("Attribute1", "Attribute2"))))
      )

  }

  class MockSamDAOForAuthDomains(slickDataSource: SlickDataSource) extends MockSamDAO(slickDataSource) {
    val authDomains = new TrieMap[(SamResourceTypeName, String), Set[String]]()

    override def createResourceFull(resourceTypeName: SamResourceTypeName,
                                    resourceId: String,
                                    policies: Map[SamResourcePolicyName, SamPolicy],
                                    authDomain: Set[String],
                                    ctx: RawlsRequestContext,
                                    parent: Option[SamFullyQualifiedResourceId]
    ): Future[SamCreateResourceResponse] = {
      authDomains.put((resourceTypeName, resourceId), authDomain)
      super.createResourceFull(resourceTypeName, resourceId, policies, authDomain, ctx, parent)
    }

    override def getResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                                       resourceId: String,
                                       ctx: RawlsRequestContext
    ): Future[Seq[String]] =
      Future.successful(authDomains.getOrElse((resourceTypeName, resourceId), Set.empty).toSeq)
  }

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withApiServicesForAuthDomains[T](dataSource: SlickDataSource)(testCode: TestApiServiceForAuthDomains => T): T = {
    val apiService =
      new TestApiServiceForAuthDomains(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withApiServicesMockedEntityService[T](
    dataSource: SlickDataSource
  )(testCode: TestApiServiceForMockedEntityService => T): T = {
    val apiService =
      TestApiServiceForMockedEntityService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withEmptyDatabaseApiServicesForAuthDomains[T](testCode: TestApiServiceForAuthDomains => T): T =
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServicesForAuthDomains(dataSource)(testCode)
    }

  def withConstantTestDataApiServices[T](testCode: TestApiService => T): T =
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withMockedEntityService[T](testCode: TestApiServiceForMockedEntityService => T): T =
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServicesMockedEntityService(dataSource)(testCode)
    }

  def dbId(ent: Entity): Long = runAndWait(
    entityQuery.getEntityRecords(testData.workspace.workspaceIdAsUUID, Set(ent.toReference))
  ).head.id
  def dbName(id: Long): String = runAndWait(
    entityQuery.getEntities(testData.workspace.workspaceIdAsUUID, Seq(id))
  ).head._2.name

  def entityOfSize(size: Long) = {
    val json =
      s"""[{"name":"${"0" * (size - 56).toInt}","entityType":"participant","operations":[]}]""" // template already includes 56 bytes, subtract that from repeated string

    assert(json.getBytes.length == size)

    HttpEntity(ContentTypes.`application/json`, json)
  }

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
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("s2_changed"))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      Get(testData.workspace.path) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
    } { capturedMetrics =>
      val AttributeEntityReference(eType, _) = testData.sample2.toReference
      val postPath = s"workspaces.redacted.redacted.entities.$eType.redacted.rename"
      val getPath = s"workspaces.redacted.redacted"

      val expected = expectedHttpRequestMetrics("post", postPath, StatusCodes.NoContent.intValue, 1) ++
        expectedHttpRequestMetrics("get", getPath, StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "update the workspace last modified date on entity update" in withTestDataApiServices { services =>
    Patch(
      testData.sample2.path(testData.workspace),
      httpJson(
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
      )
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
      }
  }

  it should "update the workspace last modified date on entity post and copy" in withTestDataApiServices { services =>
    val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeString("c")))
    val z1 = Entity(
      "z1",
      "Sample",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("x"),
        AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
        AttributeName.withDefaultNS("splat") -> attributeList
      )
    )
    val workspaceSrcName = new WorkspaceName(testData.wsName.namespace, testData.wsName.name + "_copy_source")
    val workspaceSrcRequest = WorkspaceRequest(
      workspace2Name.namespace,
      workspace2Name.name,
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
            assertResult(StatusCodes.Created) {
              status
            }
            assertResult(z1) {
              val ws2 = runAndWait(workspaceQuery.findByName(workspace2Name)).get
              runAndWait(entityQuery.get(ws2, z1.entityType, z1.name)).get
            }

            val sourceWorkspace = WorkspaceName(workspaceSrcRequest.namespace, workspaceSrcRequest.name)
            val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
            Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
              sealRoute(services.entityRoutes) ~>
              check {
                assertResult(StatusCodes.Created) {
                  status
                }
              }
            Get(workspaceSrcRequest.path) ~>
              sealRoute(services.workspaceRoutes) ~>
              check {
                assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
              }
            Get(testData.wsName.path) ~>
              sealRoute(services.workspaceRoutes) ~>
              check {
                assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
              }
          }
      }
  }

  it should "update the workspace last modified date on entity batchUpsert" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(
      testData.sample1.name,
      testData.sample1.entityType,
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")))
    )
    val update2 = EntityUpdateDefinition(
      testData.sample2.name,
      testData.sample2.entityType,
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz")))
    )
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(
          Some(
            Entity(testData.sample1.name,
                   testData.sample1.entityType,
                   testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))
            )
          )
        ) {
          runAndWait(entityQuery.get(testData.workspace, testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
      }
  }

  it should "allow an entity of the max content size to be consumed by batchUpsert" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/batchUpsert", entityOfSize(services.batchUpsertMaxBytes)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          // under the hood this will throw a 500. that's expected because the entity name is larger than the DB allows.
          // all we care about for this test is that we do not hit a Payload Too Large error
          assert(status != StatusCodes.PayloadTooLarge)
        }
  }

  it should "not allow an entity larger than the max content size to be consumed by batchUpsert" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/batchUpsert", entityOfSize(services.batchUpsertMaxBytes + 1)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.PayloadTooLarge) {
            status
          }
        }
  }

  it should "update the workspace last modified date on entity batchUpdate" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(
      testData.sample1.name,
      testData.sample1.entityType,
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")))
    )
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(
          Some(
            Entity(testData.sample1.name,
                   testData.sample1.entityType,
                   testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))
            )
          )
        ) {
          runAndWait(entityQuery.get(testData.workspace, testData.sample1.entityType, testData.sample1.name))
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
      }
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
          runAndWait(entityQuery.get(testData.workspace, newSample.entityType, newSample.name)).get
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(
          Some(
            Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(testData.workspace))))
          )
        ) {
          header("Location")
        }
      }
  }

  it should "return 201 on create entity if entity name contains a dot" in withTestDataApiServices { services =>
    val newSample = Entity("sample.new", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assertResult(newSample) {
          runAndWait(entityQuery.get(testData.workspace, newSample.entityType, newSample.name)).get
        }
        assertResult(newSample) {
          responseAs[Entity]
        }

        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(
          Some(
            Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newSample.path(testData.workspace))))
          )
        ) {
          header("Location")
        }
      }
  }

  it should "return 400 on create entity if entity name is the empty string" in withTestDataApiServices { services =>
    val newSample = Entity("", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 on create entity if entity name is whitespace-only" in withTestDataApiServices { services =>
    val newSample = Entity("   ", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 on create entity if entity name has illegal characters" in withTestDataApiServices { services =>
    val newSample = Entity("*!$!*", "sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
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

  List("default", "tag", "pfb", "import", "system", "sys", "tdr") foreach { namespace =>
    it should s"return 201 on create entity with [$namespace]-namespaced attributes" in withTestDataApiServices {
      services =>
        val newSample =
          Entity("sampleNew", "sample", Map(AttributeName(namespace, "attribute") -> AttributeString("foo")))

        Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
          }
    }
  }
  List("defaultX", "library", " tag", "bfp", "imp", "TDR", "xxx", "") foreach { namespace =>
    it should s"return 403 on create entity with [$namespace]-namespaced attributes" in withTestDataApiServices {
      services =>
        val newSample =
          Entity("sampleNew", "sample", Map(AttributeName(namespace, "attribute") -> AttributeString("foo")))

        Post(s"${testData.workspace.path}/entities", httpJson(newSample)) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            assertResult(StatusCodes.Forbidden) {
              status
            }

            val errorText = responseAs[ErrorReport].message
            assert(errorText.contains(namespace))
          }
    }
  }

  // entity and attribute counts, regardless of deleted status
  def countEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listEntities(testData.workspace))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  // entity and attribute counts, non-deleted only
  def countActiveEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listActiveEntities(testData.workspace))
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
          runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
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
          runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
        }
        assertResult(None) {
          runAndWait(entityQuery.get(testData.workspace, e2.entityType, e2.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 2)(entityCount2)
    assertResult(attributeCount1 + 1)(attributeCount2)
    assertResult(activeEntityCount1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)
  }

  it should "return 204 on entity delete covering all references including cycles" in withTestDataApiServices {
    services =>
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

      val update = EntityUpdateDefinition(e1.name,
                                          e1.entityType,
                                          Seq(AddUpdateAttribute(AttributeName.withDefaultNS("link"), e3.toReference))
      )
      val new_e1 = e1.copy(attributes = Map(AttributeName.withDefaultNS("link") -> e3.toReference))

      Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(Some(new_e1)) {
            runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
          }
        }

      Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e1, e2, e3))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(None) {
            runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
          }
          assertResult(None) {
            runAndWait(entityQuery.get(testData.workspace, e2.entityType, e2.name))
          }
          assertResult(None) {
            runAndWait(entityQuery.get(testData.workspace, e3.entityType, e3.name))
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

        val expected = Seq(e1, e2) map { _.toReference }
        assertSameElements(expected, responseAs[Seq[AttributeEntityReference]])
        assertResult(Some(e1)) {
          runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
        }
        assertResult(Some(e2)) {
          runAndWait(entityQuery.get(testData.workspace, e2.entityType, e2.name))
        }
      }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

    assertResult(entityCount1 + 2)(entityCount2)
    assertResult(attributeCount1 + 1)(attributeCount2)
    assertResult(activeEntityCount1 + 2)(activeEntityCount2)
    assertResult(activeAttributeCount1 + 1)(activeAttributeCount2)
  }

  it should "return 409 with the set of references on referenced entity delete with a cycle" in withTestDataApiServices {
    services =>
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

      val update = EntityUpdateDefinition(e1.name,
                                          e1.entityType,
                                          Seq(AddUpdateAttribute(AttributeName.withDefaultNS("link"), e3.toReference))
      )
      val new_e1 = e1.copy(attributes = Map(AttributeName.withDefaultNS("link") -> e3.toReference))

      Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(Some(new_e1)) {
            runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
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
            runAndWait(entityQuery.get(testData.workspace, e1.entityType, e1.name))
          }
          assertResult(Some(e2)) {
            runAndWait(entityQuery.get(testData.workspace, e2.entityType, e2.name))
          }
          assertResult(Some(e3)) {
            runAndWait(entityQuery.get(testData.workspace, e3.entityType, e3.name))
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

  it should "return 204 when deleting an entity type that has no references to it" in withTestDataApiServices {
    services =>
      val entities = Seq.tabulate(100) { i =>
        Entity(
          s"foo$i",
          "typeToDelete",
          Map(
            AttributeName.withDefaultNS("hello") -> AttributeString("world"),
            AttributeName.withDefaultNS("foo") -> AttributeString("bar")
          )
        )
      }

      runAndWait(entityQuery.save(testData.workspace, entities))

      val (entityCountBefore, attributeCountBefore) = countEntitiesAttrs(testData.workspace)
      val (activeEntityCountBefore, activeAttributeCountBefore) = countActiveEntitiesAttrs(testData.workspace)

      Delete(s"${testData.workspace.path}/entityTypes/typeToDelete") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(Seq.empty) {
            runAndWait(entityQuery.listActiveEntitiesOfType(testData.workspace, "typeToDelete")).toSeq
          }
        }

      val (entityCountAfter, attributeCountAfter) = countEntitiesAttrs(testData.workspace)
      val (activeEntityCountAfter, activeAttributeCountAfter) = countActiveEntitiesAttrs(testData.workspace)

      // Ensure that the entity and attribute count (including "deleted" entities is the same before and after
      assertResult(entityCountBefore)(entityCountAfter)
      assertResult(attributeCountBefore)(attributeCountAfter)

      // Ensure that we have marked the appropriate number of entities and attributes as "deleted"
      assertResult(activeEntityCountBefore - entities.size)(activeEntityCountAfter)
      assertResult(activeAttributeCountBefore - entities.flatMap(_.attributes).size)(activeAttributeCountAfter)
  }

  it should "return 409 when deleting an entity type that has references to it" in withTestDataApiServices { services =>
    val entities = Seq.tabulate(100) { i =>
      Entity(s"foo$i", "typeToDelete", Map.empty)
    }

    val referringEntities = Seq.tabulate(50) { i =>
      Entity(s"foo$i",
             "referringType",
             Map(AttributeName.withDefaultNS("type") -> AttributeEntityReference("typeToDelete", s"foo$i"))
      )
    }

    runAndWait(entityQuery.save(testData.workspace, entities))
    runAndWait(entityQuery.save(testData.workspace, referringEntities))

    val (entityCountBefore, attributeCountBefore) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCountBefore, activeAttributeCountBefore) = countActiveEntitiesAttrs(testData.workspace)

    Delete(s"${testData.workspace.path}/entityTypes/typeToDelete") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
        assertResult(true) {
          responseAs[String].contains("50")
        }
        assertResult(true) {
          runAndWait(entityQuery.listActiveEntitiesOfType(testData.workspace, "typeToDelete")).iterator.nonEmpty
        }
      }

    val (entityCountAfter, attributeCountAfter) = countEntitiesAttrs(testData.workspace)
    val (activeEntityCountAfter, activeAttributeCountAfter) = countActiveEntitiesAttrs(testData.workspace)

    // Ensure that we have not deleted any entities or their attributes
    assertResult(entityCountBefore)(entityCountAfter)
    assertResult(attributeCountBefore)(attributeCountAfter)
    assertResult(activeEntityCountBefore)(activeEntityCountAfter)
    assertResult(activeAttributeCountBefore)(activeAttributeCountAfter)
  }

  it should "return 201 on create entity for a previously deleted entity" in withTestDataApiServices { services =>
    val attrs1 = Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
                     AttributeName.withDefaultNS("alive") -> AttributeBoolean(false)
    )
    val attrs2 = Map(AttributeName.withDefaultNS("type") -> AttributeString("normal"),
                     AttributeName.withDefaultNS("answer") -> AttributeNumber(42)
    )
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
          runAndWait(entityQuery.get(testData.workspace, sample.entityType, sample.name)).get
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
          runAndWait(entityQuery.get(testData.workspace, sample.entityType, sample.name)).get
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
          runAndWait(entityQuery.get(testData.workspace, sample.entityType, sample.name)).get
        }
      }

    val id3 = dbId(sampleAttrs3)

    assert(oldId != id3)
    assert(newId != id3)
  }

  it should "return 400 when batch upserting an entity with invalid update operations" in withTestDataApiServices {
    services =>
      val update1 =
        EntityUpdateDefinition(testData.sample1.name,
                               testData.sample1.entityType,
                               Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a")))
        )
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

  it should "return 204 when batch upserting nothing" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq.empty[EntityUpdateDefinition])) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 204 when batch upserting an entity that does not yet exist" in withTestDataApiServices { services =>
    val update1 = EntityUpdateDefinition(
      "newSample",
      "Sample",
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))
    )
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(
          Some(
            Entity("newSample", "Sample", Map(AttributeName.withDefaultNS("newAttribute") -> AttributeString("foo")))
          )
        ) {
          runAndWait(entityQuery.get(testData.workspace, "Sample", "newSample"))
        }
      }
  }

  it should "return 204 when batch upserting an entity that was previously deleted" in withTestDataApiServices {
    services =>
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
            runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
          }
        }

      val update = EntityUpdateDefinition(
        e.name,
        e.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz")))
      )
      val updatedEntity =
        e.copy(attributes = Map(AttributeName.withDefaultNS("newAttribute") -> AttributeString("baz")))
      Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(Some(updatedEntity)) {
            runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
          }
        }

      val newId = dbId(e)

      assert(oldId != newId)
  }

  it should "return 204 when batch upserting an entity with valid update operations" in withTestDataApiServices {
    services =>
      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz")))
      )
      Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(
            Some(
              Entity(
                testData.sample1.name,
                testData.sample1.entityType,
                testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))
              )
            )
          ) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample1.entityType, testData.sample1.name))
          }
          assertResult(
            Some(
              Entity(
                testData.sample2.name,
                testData.sample2.entityType,
                testData.sample2.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("baz"))
              )
            )
          ) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample2.entityType, testData.sample2.name))
          }
        }
  }

  it should "return 204 when batch upserting an entities with valid references" in withTestDataApiServices { services =>
    val newEntity = Entity("new_entity", testData.sample2.entityType, Map.empty)
    val referenceList = AttributeEntityReferenceList(Seq(testData.sample2.toReference, newEntity.toReference))
    val update1 =
      EntityUpdateDefinition(testData.sample1.name,
                             testData.sample1.entityType,
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), referenceList))
      )
    val update2 = EntityUpdateDefinition(newEntity.name, newEntity.entityType, Seq.empty)
    val blep = httpJson(Seq(update1, update2))
    Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(
          Some(
            Entity(testData.sample1.name,
                   testData.sample1.entityType,
                   testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> referenceList)
            )
          )
        ) {
          runAndWait(entityQuery.get(testData.workspace, testData.sample1.entityType, testData.sample1.name))
        }
        assertResult(Some(newEntity)) {
          runAndWait(entityQuery.get(testData.workspace, newEntity.entityType, newEntity.name))
        }
      }
  }

  it should "return 400 when batch upserting an entity with references that don't exist" in withTestDataApiServices {
    services =>
      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeEntityReference("bar", "baz")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeEntityReference("bar", "bing")))
      )
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

  it should "return 403 when batch upserting an entity with invalid-namespace attributes" in withTestDataApiServices {
    services =>
      val invalidAttrNamespace = "invalid"

      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute1"), AttributeString("smee")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute2"), AttributeString("blee")))
      )
      Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
            status
          }
        }
  }

  it should "return 403 when batch upserting an entity with library-namespace attributes " in withTestDataApiServices {
    services =>
      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(
          AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))
        )
      )
      Post(s"${testData.workspace.path}/entities/batchUpsert", httpJson(Seq(update1, update2))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 400 when batch updating an entity with invalid update operations" in withTestDataApiServices {
    services =>
      val update1 =
        EntityUpdateDefinition(testData.sample1.name,
                               testData.sample1.entityType,
                               Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a")))
        )
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
    val update1 = EntityUpdateDefinition(
      "superDuperNewSample",
      "Samples",
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))
    )
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
          runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
        }
      }

    val update1 = EntityUpdateDefinition(
      e.name,
      e.entityType,
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("baz")))
    )
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

  it should "return 204 when batch updating an entity with valid update operations" in withTestDataApiServices {
    services =>
      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")))
      )
      Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(
            Some(
              Entity(
                testData.sample1.name,
                testData.sample1.entityType,
                testData.sample1.attributes + (AttributeName.withDefaultNS("newAttribute") -> AttributeString("bar"))
              )
            )
          ) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample1.entityType, testData.sample1.name))
          }
        }
  }

  it should "return 204 when batch updating nothing" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq.empty[EntityUpdateDefinition])) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 403 when batch updating an entity with invalid-namespace attributes" in withTestDataApiServices {
    services =>
      val invalidAttrNamespace = "invalid"

      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute1"), AttributeString("smee")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(AddUpdateAttribute(AttributeName(invalidAttrNamespace, "newAttribute2"), AttributeString("blee")))
      )
      Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden, responseAs[ErrorReport]) {
            status
          }
        }
  }

  it should "return 403 when batch updating an entity with library-namespace attributes" in withTestDataApiServices {
    services =>
      revokeCuratorRole(services)
      val update1 = EntityUpdateDefinition(
        testData.sample1.name,
        testData.sample1.entityType,
        Seq(AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute1"), AttributeString("wang")))
      )
      val update2 = EntityUpdateDefinition(
        testData.sample2.name,
        testData.sample2.entityType,
        Seq(
          AddUpdateAttribute(AttributeName(AttributeName.libraryNamespace, "newAttribute2"), AttributeString("chung"))
        )
      )
      Post(s"${testData.workspace.path}/entities/batchUpdate", httpJson(Seq(update1, update2))) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 200 on get entity" in withTestDataApiServices { services =>
    withStatsD {
      Get(testData.sample2.path(testData.workspace)) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          assertResult(testData.sample2) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample2.entityType, testData.sample2.name)).get
          }
          assertResult(testData.sample2) {
            responseAs[Entity]
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected = expectedHttpRequestMetrics("get", s"$wsPathForRequestMetrics", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  // metadata attribute name sequences should be returned in case-insensitive string order
  def assertMetadataMapsEqual(a: Map[String, EntityTypeMetadata], b: Map[String, EntityTypeMetadata]): Unit = {
    assert(a.size == b.size, "Maps are not equal: sizes differ")
    assertSameElements(a.keySet, b.keySet)
    a.keysIterator.foreach { key =>
      val aMeta = a(key)
      val bMeta = b(key)
      assert(aMeta.count == bMeta.count, s"Map counts differ for Entity Type $key")
      assert(aMeta.idName == bMeta.idName, s"ID names counts differ for Entity Type $key")
      aMeta.attributeNames should contain theSameElementsInOrderAs bMeta.attributeNames
    }
  }

  val expectedMetadataMap: Map[String, EntityTypeMetadata] = Map(
    "Sample" -> EntityTypeMetadata(
      8,
      "Sample_id",
      Seq("confused", "cycle", "quot", "somefoo", "thingies", "tumortype", "type", "whatsit")
    ),
    "Aliquot" -> EntityTypeMetadata(2, "Aliquot_id", Seq()),
    "Pair" -> EntityTypeMetadata(2, "Pair_id", Seq("case", "control", "whatsit")),
    "SampleSet" -> EntityTypeMetadata(5, "SampleSet_id", Seq("hasSamples", "samples")),
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
      }
  }

  it should "return 200 on list all samples" in withConstantTestDataApiServices { services =>
    val expected = Seq(
      constantData.sample1,
      constantData.sample2,
      constantData.sample3,
      constantData.sample4,
      constantData.sample5,
      constantData.sample6,
      constantData.sample7,
      constantData.sample8
    )

    Get(s"${constantData.workspace.path}/entities/Sample") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(constantData.workspace, "Sample"))
        assertSameElements(responseAs[Array[Entity]], expected)
        assertSameElements(dbSamples, expected)
      }
  }

  it should "not list deleted samples" in withConstantTestDataApiServices { services =>
    val expected = Seq(
      constantData.sample1,
      constantData.sample2,
      constantData.sample3,
      constantData.sample4,
      constantData.sample5,
      constantData.sample6,
      constantData.sample7,
      constantData.sample8
    )

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

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(constantData.workspace, "Sample"))
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

        val dbSamples = runAndWait(entityQuery.listActiveEntitiesOfType(constantData.workspace, "Sample"))
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
        assertResult(e.entityType)(respEnt.entityType)
        assert(e.name != respEnt.name)
        assertResult(newEnt.name)(respEnt.name)

        assertResult(1)(respEnt.attributes.size)

        // same attribute namespace and value but the attribute name has been hidden/renamed on deletion
        val respAttr = respEnt.attributes.head
        val eAttr = e.attributes.head

        assertResult(eAttr._1.namespace)(respAttr._1.namespace)
        assert(respAttr._1.name.contains(eAttr._1.name + "_"))
        assertResult(eAttr._2)(respAttr._2)
      }
  }

  it should "return 200 on update entity" in withTestDataApiServices { services =>
    withStatsD {
      Patch(
        testData.sample2.path(testData.workspace),
        httpJson(
          Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
        )
      ) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK, responseAs[String]) {
            status
          }
          assertResult(Option(AttributeString("bang"))) {
            runAndWait(
              entityQuery.get(testData.workspace, testData.sample2.entityType, testData.sample2.name)
            ).get.attributes.get(AttributeName.withDefaultNS("boo"))
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected = expectedHttpRequestMetrics("patch", s"$wsPathForRequestMetrics", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 200 on remove attribute from entity" in withTestDataApiServices { services =>
    Patch(testData.sample2.path(testData.workspace),
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("bar")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(None) {
          runAndWait(
            entityQuery.get(testData.workspace, testData.sample2.entityType, testData.sample2.name)
          ).get.attributes.get(AttributeName.withDefaultNS("bar"))
        }
      }
  }

  it should "return 404 on update to non-existing entity" in withTestDataApiServices { services =>
    Patch(
      testData.sample2.copy(name = "DNE").path(testData.workspace),
      httpJson(
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
      )
    ) ~>
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
          runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
        }
      }

    Patch(
      e.path(testData.workspace),
      httpJson(
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("baz"), AttributeString("bang")): AttributeUpdateOperation)
      )
    ) ~>
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

    Patch(testData.sample2.path(testData.workspace),
          httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))
    ) ~>
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

    Patch(testData.sample2.path(testData.workspace),
          httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[String]) {
          status
        }
      }
  }

  it should "return 400 on remove from an attribute that is not a list" in withTestDataApiServices { services =>
    Patch(
      testData.sample2.path(testData.workspace),
      httpJson(
        Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("adsf")): AttributeUpdateOperation)
      )
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on remove from list attribute that does not exist" in withTestDataApiServices { services =>
    Patch(
      testData.sample2.path(testData.workspace),
      httpJson(
        Seq(RemoveListMember(AttributeName.withDefaultNS("grip"), AttributeString("adsf")): AttributeUpdateOperation)
      )
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }
  it should "return 400 on add to list attribute that is not a list" in withTestDataApiServices { services =>
    Patch(
      testData.sample1.path(testData.workspace),
      httpJson(
        Seq(AddListMember(AttributeName.withDefaultNS("somefoo"), AttributeString("adsf")): AttributeUpdateOperation)
      )
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return an error in real json when patching entity with badly shaped request body" in withTestDataApiServices {
    services =>
      Patch(
        testData.sample1.path(testData.workspace),
        httpJson(
          AddListMember(AttributeName.withDefaultNS("somefoo"), AttributeString("adsf")): AttributeUpdateOperation
        )
      ) ~>
        sealRoute(services.entityRoutes)(rejectionHandler = RawlsApiService.rejectionHandler) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
          responseAs[JsObject]
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
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("s2_changed"))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(true) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample2.entityType, "s2_changed")).isDefined
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.rename", StatusCodes.NoContent.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 204 on entity rename if new name contains a dot" in withTestDataApiServices { services =>
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("s2.changed"))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(true) {
            runAndWait(entityQuery.get(testData.workspace, testData.sample2.entityType, "s2.changed")).isDefined
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.rename", StatusCodes.NoContent.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 on entity rename if new name is empty string" in withTestDataApiServices { services =>
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName(""))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.rename", StatusCodes.BadRequest.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 on entity rename if new name is whitespace-only" in withTestDataApiServices { services =>
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("   "))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.rename", StatusCodes.BadRequest.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 on entity rename if new name has illegal characters" in withTestDataApiServices { services =>
    withStatsD {
      Post(s"${testData.sample2.path(testData.workspace)}/rename", httpJson(EntityName("*!$!*"))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.entities.${testData.sample2.entityType}.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.rename", StatusCodes.BadRequest.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 404 on entity rename, entity does not exist" in withTestDataApiServices { services =>
    Post(s"${testData.sample2.copy(name = "foox").path(testData.workspace)}/rename",
         httpJson(EntityName("s2_changed"))
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        assertResult(None) {
          runAndWait(entityQuery.get(testData.workspace, testData.sample2.entityType, "s2_changed"))
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
          runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
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

  class DeleteAttributeNameTestData extends TestData {
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "fake-bucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    val entitiesWithDefaultNamespace = Entity(
      "name1",
      "type1",
      Map(
        AttributeName.withDefaultNS("hello") -> AttributeString("world"),
        AttributeName.withDefaultNS("hello") -> AttributeString("brazil"),
        AttributeName.withDefaultNS("hello") -> AttributeString("pluto"),
        AttributeName.withDefaultNS("hi") -> AttributeString("hades"),
        AttributeName.withDefaultNS("salutations") -> AttributeString("hades")
      )
    )

    val entitiesWithNonDefaultNamespace = Entity(
      "name2",
      "type1",
      Map(
        AttributeName.withDefaultNS("hello") -> AttributeString("world"),
        AttributeName.fromDelimitedName("othernamespace:yo") -> AttributeString("world"),
        AttributeName.fromDelimitedName("othernamespace:yo") -> AttributeString("atlantis")
      )
    )

    val entities = List(entitiesWithDefaultNamespace, entitiesWithNonDefaultNamespace)

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        entityQuery.save(workspace, entities)
      )
    }
  }

  val deleteAttributeNameTestData = new DeleteAttributeNameTestData()

  def withDeleteAttributeNameTestDataApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(deleteAttributeNameTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  it should "return a 204 when deleting multiple attributes of default namespace" in withDeleteAttributeNameTestDataApiServices {
    services =>
      Delete(s"${testData.workspace.path}/entities/type1?attributeNames=hello,hi") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(None) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name1")).get.attributes
              .get(AttributeName.withDefaultNS("hello"))
          }
          assertResult(None) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name1")).get.attributes
              .get(AttributeName.withDefaultNS("hi"))
          }
          assertResult(Some(AttributeString("hades"))) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name1")).get.attributes
              .get(AttributeName.withDefaultNS("salutations"))
          }
        }
  }

  it should "return a 204 when deleting attributes with a specified default namespace" in withDeleteAttributeNameTestDataApiServices {
    services =>
      Delete(s"${testData.workspace.path}/entities/type1?attributeNames=default:hello") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(None) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name1")).get.attributes
              .get(AttributeName.withDefaultNS("hello"))
          }
        }
  }

  it should "return a 204 when deleting attributes with a non-default namespace" in withDeleteAttributeNameTestDataApiServices {
    services =>
      Delete(s"${testData.workspace.path}/entities/type1?attributeNames=othernamespace:yo") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(Some(AttributeString("world"))) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name2")).get.attributes
              .get(AttributeName.withDefaultNS("hello"))
          }
          assertResult(None) {
            runAndWait(entityQuery.get(deleteAttributeNameTestData.workspace, "type1", "name2")).get.attributes
              .get(AttributeName.fromDelimitedName("othernamespace:yo"))
          }
        }
  }

  it should "return 400 when deleting entity attribute that does not exist" in withDeleteAttributeNameTestDataApiServices {
    services =>
      Delete(s"${testData.workspace.path}/entities/type1?attributeNames=fakeattribute") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 when deleting entity attributes with no query params" in withDeleteAttributeNameTestDataApiServices {
    services =>
      Delete(s"${testData.workspace.path}/entities/type1") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  // evaluate expressions

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

  it should "return 200 on successfully parsing a raw JSON" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
         httpJsonStr("""{"example":"foo", "array": [1, 2, 3]}""")
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          Array(
            JsObject(
              Map("example" -> JsString("foo"), "array" -> JsArray(Vector(JsNumber(1), JsNumber(2), JsNumber(3))))
            )
          )
        ) {
          responseAs[Array[JsObject]]
        }
      }
  }

  it should "return 200 on successfully parsing a JSON with single attribute reference" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
           httpJsonStr("""{"example":"foo", "array": this.samples.type}""")
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(
            Array(
              JsObject(
                Map("example" -> JsString("foo"),
                    "array" -> JsArray(Vector(JsString("normal"), JsString("tumor"), JsString("tumor")))
                )
              )
            )
          ) {
            responseAs[Array[JsObject]]
          }
        }
  }

  it should "return 200 on successfully parsing a nested JSON with multiple attribute reference" in withTestDataApiServices {
    services =>
      Post(
        s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
        httpJsonStr("""{"array": this.samples.type, "details": {"values": this.samples.type}}""")
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          val attrRefArray = JsArray(Vector(JsString("normal"), JsString("tumor"), JsString("tumor")))
          assertResult(
            Array(JsObject(Map("array" -> attrRefArray, "details" -> JsObject(Map("values" -> attrRefArray)))))
          ) {
            responseAs[Array[JsObject]]
          }
        }
  }

  it should "return 200 on successfully parsing a raw array" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate", httpJsonStr("""[1, 2, 3, 4, 5]""")) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Array(1, 2, 3, 4, 5)) {
          responseAs[Array[Int]]
        }
      }
  }

  it should "return 200 on successfully parsing a raw heterogeneous array" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
         httpJsonStr("""[1, 2, 3, "foo", "bar", true]""")
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          JsArray(Vector(JsNumber(1), JsNumber(2), JsNumber(3), JsString("foo"), JsString("bar"), JsBoolean(true)))
        ) {
          responseAs[JsArray]
        }
      }
  }

  it should "return 200 on successfully parsing a heterogeneous array with attribute references" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
           httpJsonStr("""[1, 2, 3, this.samples.type, ["foo", "bar", this.samples.type]]""")
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          val attrRefArray = JsArray(Vector(JsString("normal"), JsString("tumor"), JsString("tumor")))
          assertResult(
            Array(
              JsArray(
                Vector(JsNumber(1),
                       JsNumber(2),
                       JsNumber(3),
                       attrRefArray,
                       JsArray(JsString("foo"), JsString("bar"), attrRefArray)
                )
              )
            )
          ) {
            responseAs[Array[JsArray]]
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

  it should "return 400 while parsing invalid expression with invalid reference in array" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
           httpJsonStr("""[1, 2, "foo", invalid.ref]""")
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 while parsing invalid expression with invalid reference in JSON" in withTestDataApiServices {
    services =>
      Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
           httpJsonStr("""{"example": [1, 2], "foo": invalid.ref}""")
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 while parsing expression with non-existent reference" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/entities/SampleSet/sset1/evaluate",
         httpJsonStr("""{"example": [1, 2], "foo": nonexistent.anything}""")
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeString("c")))
  val z1 = Entity(
    "z1",
    "Sample",
    Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("x"),
      AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
      AttributeName.withDefaultNS("splat") -> attributeList
    )
  )
  val workspace2Name = new WorkspaceName(testData.wsName.namespace, testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 201 for copying entities into a workspace with no conflicts" in withTestDataApiServices {
    services =>
      Post("/workspaces", httpJson(workspace2Request)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(workspace2Request) {
            val ws = runAndWait(workspaceQuery.findByName(workspace2Request.toWorkspaceName)).get
            WorkspaceRequest(ws.namespace, ws.name, ws.attributes)
          }

          Post(s"${workspace2Request.path}/entities", httpJson(z1)) ~>
            sealRoute(services.entityRoutes) ~>
            check {
              assertResult(StatusCodes.Created) {
                status
              }
              assertResult(z1) {
                val ws2 = runAndWait(workspaceQuery.findByName(workspace2Name)).get
                runAndWait(entityQuery.get(ws2, z1.entityType, z1.name)).get
              }

              val sourceWorkspace = WorkspaceName(workspace2Request.namespace, workspace2Request.name)
              val entityCopyDefinition = EntityCopyDefinition(sourceWorkspace, testData.wsName, "Sample", Seq("z1"))
              Post("/workspaces/entities/copy", httpJson(entityCopyDefinition)) ~>
                sealRoute(services.entityRoutes) ~>
                check {
                  assertResult(StatusCodes.Created) {
                    status
                  }
                  assertResult(z1) {
                    runAndWait(entityQuery.get(testData.workspace, z1.entityType, z1.name)).get
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

    val newWorkspaceCreate = WorkspaceRequest(newWorkspace.namespace, newWorkspace.name, Map.empty)

    val copyAliquot1 =
      EntityCopyDefinition(sourceWorkspace, newWorkspace, testData.aliquot1.entityType, Seq(testData.aliquot1.name))
    val copySample3 =
      EntityCopyDefinition(sourceWorkspace, newWorkspace, testData.sample3.entityType, Seq(testData.sample3.name))

    Post("/workspaces", httpJson(newWorkspaceCreate)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post("/workspaces/entities/copy", httpJson(copyAliquot1)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
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
        assertResult(StatusCodes.Conflict) {
          status
        }

        val copyResponse = responseAs[EntityCopyResponse]

        assertSameElements(Seq.empty, copyResponse.entitiesCopied)
        assertSameElements(Seq.empty, copyResponse.hardConflicts)

        val expectedSoftConflicts = Seq(
          EntitySoftConflict(
            testData.sample3.entityType,
            testData.sample3.name,
            Seq(
              EntitySoftConflict(
                testData.sample1.entityType,
                testData.sample1.name,
                Seq(EntitySoftConflict(testData.aliquot1.entityType, testData.aliquot1.name, Seq.empty))
              )
            )
          )
        )

        assertSameElements(expectedSoftConflicts, copyResponse.softConflicts)
      }
  }

  it should "return 409 for copying entities into a workspace with subtree conflicts, but successfully copy when asked to" in withTestDataApiServices {
    services =>
      val sourceWorkspace = WorkspaceName(testData.workspace.namespace, testData.workspace.name)
      val entityCopyDefinition1 = EntityCopyDefinition(sourceWorkspace,
                                                       testData.controlledWorkspace.toWorkspaceName,
                                                       testData.sample1.entityType,
                                                       Seq(testData.sample1.name)
      )
      // this will cause a soft conflict because it references sample1
      val entityCopyDefinition2 = EntityCopyDefinition(sourceWorkspace,
                                                       testData.controlledWorkspace.toWorkspaceName,
                                                       testData.sample3.entityType,
                                                       Seq(testData.sample3.name)
      )

      withStatsD {
        Post("/workspaces/entities/copy", httpJson(entityCopyDefinition1)) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }

            val copyResponse = responseAs[EntityCopyResponse]

            assertSameElements(Seq(testData.sample1, testData.aliquot1).map(_.toReference), copyResponse.entitiesCopied)
            assertSameElements(Seq.empty, copyResponse.hardConflicts)
            assertSameElements(Seq.empty, copyResponse.softConflicts)
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("post", "workspaces.entities.copy", StatusCodes.Created.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }

      val expectedSoftConflictResponse = EntityCopyResponse(
        Seq.empty,
        Seq.empty,
        Seq(
          EntitySoftConflict(
            testData.sample3.entityType,
            testData.sample3.name,
            Seq(
              EntitySoftConflict(testData.sample1.entityType, testData.sample1.name, Seq.empty),
              EntitySoftConflict(
                testData.sample1.entityType,
                testData.sample1.name,
                Seq(EntitySoftConflict(testData.aliquot1.entityType, testData.aliquot1.name, Seq.empty))
              )
            )
          )
        )
      )

      // test the default case of no parameter set
      withStatsD {
        Post("/workspaces/entities/copy", httpJson(entityCopyDefinition2)) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.Conflict) {
              status
            }
            assertResult(expectedSoftConflictResponse) {
              responseAs[EntityCopyResponse]
            }
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("post", "workspaces.entities.copy", StatusCodes.Conflict.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }

      withStatsD {
        Post("/workspaces/entities/copy?linkExistingEntities=false", httpJson(entityCopyDefinition2)) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.Conflict) {
              status
            }
            assertResult(expectedSoftConflictResponse) {
              responseAs[EntityCopyResponse]
            }
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("post", "workspaces.entities.copy", StatusCodes.Conflict.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }

      Post("/workspaces/entities/copy?linkExistingEntities=true", httpJson(entityCopyDefinition2)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(EntityCopyResponse(Seq(testData.sample3).map(_.toReference), Seq.empty, Seq.empty)) {
            responseAs[EntityCopyResponse]
          }
        }
  }

  it should "return 422 when copying entities from a Realm-protected workspace into one not in that Realm" in withEmptyDatabaseApiServicesForAuthDomains {
    services =>
      runAndWait(rawlsBillingProjectQuery.create(testData.billingProject))
      val authDomain = Set(testData.dbGapAuthorizedUsersGroup)

      // add an entity to a workspace with a Realm
      val x = WorkspaceRequest(namespace = testData.workspaceWithRealm.namespace,
                               testData.workspaceWithRealm.name,
                               Map.empty,
                               Option(authDomain)
      )
      Post("/workspaces", x) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
        }

//    Post(s"${testData.workspaceWithRealm.path}/entities", httpJson(z1)) ~>
//      sealRoute(services.entityRoutes) ~>
//      check {
//        assertResult(StatusCodes.Created) {
//          status
//        }
//        assertResult(Some(z1)) {
//          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspaceWithRealm), z1.entityType, z1.name))
//        }
//      }
//
      // attempt to copy an entity to a workspace with the wrong Realm
      val wrongRealmCloneRequest = WorkspaceRequest(namespace = testData.workspaceWithRealm.namespace,
                                                    name = "copy_add_realm",
                                                    Map.empty,
                                                    Option(Set(testData.realm2))
      )
      Post(s"/workspaces", httpJson(wrongRealmCloneRequest)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val wrongRealmCopyDef = EntityCopyDefinition(testData.workspaceWithRealm.toWorkspaceName,
                                                   wrongRealmCloneRequest.toWorkspaceName,
                                                   "Sample",
                                                   Seq("z1")
      )
      Post("/workspaces/entities/copy", httpJson(wrongRealmCopyDef)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.UnprocessableEntity) {
            status
          }
        }

      // attempt to copy an entity to a workspace with no Realm
      val x2 = WorkspaceRequest(namespace = testData.workspace.namespace,
                                testData.workspace.name,
                                Map.empty,
                                Option(Set.empty)
      )
      Post("/workspaces", x2) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
        }

      val noRealmCopyDef = EntityCopyDefinition(testData.workspaceWithRealm.toWorkspaceName,
                                                testData.workspace.toWorkspaceName,
                                                "Sample",
                                                Seq("z1")
      )
      Post("/workspaces/entities/copy", httpJson(noRealmCopyDef)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.UnprocessableEntity) {
            status
          }
        }
  }

  it should "return 204 when copying entities from an auth-domain protected workspace into one with a compatible auth-domain" in withEmptyDatabaseApiServicesForAuthDomains {
    services =>
      runAndWait(rawlsBillingProjectQuery.create(testData.billingProject))
      val srcWorkspaceName = WorkspaceName(testData.workspaceWithRealm.namespace, "source_ws")

      val authDomain = Set(testData.dbGapAuthorizedUsersGroup)

      val x = WorkspaceRequest(namespace = testData.workspaceWithRealm.namespace,
                               name = "source_ws",
                               Map.empty,
                               Option(authDomain)
      )
      Post("/workspaces", x) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
        }

      val destCloneRequest = WorkspaceRequest(namespace = testData.workspaceWithRealm.namespace,
                                              name = "copy_of_source_ws",
                                              Map.empty,
                                              Option(authDomain)
      )
      Post(s"/workspaces/${srcWorkspaceName.namespace}/source_ws/clone", httpJson(destCloneRequest)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(Some(authDomain.map(_.membersGroupName.value))) {
            services.samDAO.authDomains.get((SamResourceTypeNames.workspace, responseAs[WorkspaceDetails].workspaceId))
          }
        }

      Post(s"/workspaces/${srcWorkspaceName.namespace}/${srcWorkspaceName.name}/entities", httpJson(z1)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val destWorkspaceName = destCloneRequest.toWorkspaceName

      val copyDef = EntityCopyDefinition(srcWorkspaceName, destWorkspaceName, "Sample", Seq("z1"))
      Post("/workspaces/entities/copy", httpJson(copyDef)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
  }

  it should "return 422 when copying entities from an auth domain protected workspace with only a partial match of AD groups" in withEmptyDatabaseApiServicesForAuthDomains {
    services =>
      runAndWait(rawlsBillingProjectQuery.create(testData.billingProject))
      val singleAuthDomain = Set(testData.dbGapAuthorizedUsersGroup)
      val doubleAuthDomain = Set(testData.dbGapAuthorizedUsersGroup, testData.realm)

      val x = WorkspaceRequest(testData.workspaceWithRealm.namespace,
                               testData.workspaceWithRealm.name,
                               Map.empty,
                               Option(singleAuthDomain)
      )
      Post("/workspaces", x) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
        }

      val y = WorkspaceRequest(testData.workspaceWithMultiGroupAD.namespace,
                               testData.workspaceWithMultiGroupAD.name,
                               Map.empty,
                               Option(doubleAuthDomain)
      )
      Post("/workspaces", y) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
        }

      val wrongRealmCopyDef = EntityCopyDefinition(testData.workspaceWithMultiGroupAD.toWorkspaceName,
                                                   testData.workspaceWithRealm.toWorkspaceName,
                                                   "Sample",
                                                   Seq("z1")
      )
      Post("/workspaces/entities/copy", httpJson(wrongRealmCopyDef)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.UnprocessableEntity) {
            status
          }
        }
  }

  it should "not allow dots in user-defined strings" in withTestDataApiServices { services =>
    // entity names are validated by validateEntityName, which allows dots;
    // entity types are valaidated by validateUserDefinedString, which does not.
    val dotSample = Entity("sample.with.dots.in.name",
                           "entity.type.with.dots",
                           Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"))
    )
    Post(s"${testData.workspace.path}/entities", httpJson(dotSample)) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  class PaginationTestData extends TestData {
    val userOwner = RawlsUser(
      UserInfo(RawlsUserEmail("owner-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212345")
      )
    )
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set())
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set())

    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    val numEntities = 100
    val vocab1Strings = Map(0 -> "foo", 1 -> "bar", 2 -> "baz")
    val vocab2Strings = Map(0 -> "bim", 1 -> "bam")
    val entityType = "page_entity"
    val entities = Random.shuffle(
      for (i <- 1 to numEntities)
        yield Entity(
          s"entity_$i",
          entityType,
          Map(
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
            }),
            // pfb:number collides with default:number unless namespaces are honored
            AttributeName.fromDelimitedName("pfb:number") -> AttributeNumber(Math.random())
          )
        )
    )

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        entityQuery.save(workspace, entities)
      )
    }
  }

  val paginationTestData = new PaginationTestData()

  def withPaginationTestDataApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(paginationTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  val defaultQuery = EntityQuery(1, 10, "name", Ascending, None)

  def calculateNumPages(count: Int, pageSize: Int) = Math.ceil(count.toDouble / pageSize).toInt

  "entityQuery API" should "return 400 bad request on entity query when page is not a number" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=asdf") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 bad request on entity query when page size is not a number" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=asdfasdf") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 bad request on entity query when page is <= 0" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=-1") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 bad request on entity query when page size is <= 0" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=-1") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 bad request on entity query when page > page count" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=10000000") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 bad request on entity query for unknown sort direction" in withPaginationTestDataApiServices {
    services =>
      Get(
        s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortDirection=asdfasdfasdf"
      ) ~>
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
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(sortField = "asdfasdfasdf"),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      paginationTestData.numEntities,
                                      calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
            ),
            paginationTestData.entities.sortBy(_.name).take(defaultQuery.pageSize)
          )
        ) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return 404 not found on entity query for workspace that does not exist" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.copy(name = "DNE").path}/entityQuery/${paginationTestData.entityType}") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 200 OK on entity query when no query params are given" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(
            EntityQueryResponse(
              defaultQuery,
              EntityQueryResultMetadata(paginationTestData.numEntities,
                                        paginationTestData.numEntities,
                                        calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
              ),
              paginationTestData.entities.sortBy(_.name).take(defaultQuery.pageSize)
            )
          ) {

            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "return 200 OK on entity query when there are no entities of given type" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/blarf") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(EntityQueryResponse(defaultQuery, EntityQueryResultMetadata(0, 0, 0), Seq.empty)) {

            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "not return deleted entities on entity query" in withPaginationTestDataApiServices { services =>
    val e = Entity("foo", "bar", Map.empty)
    withStatsD {
      Post(s"${testData.workspace.path}/entities", httpJson(e)) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(e) {
            responseAs[Entity]
          }
        }

      Post(s"${testData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(e))) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
          assertResult(None) {
            runAndWait(entityQuery.get(testData.workspace, e.entityType, e.name))
          }
        }

      Get(s"${paginationTestData.workspace.path}/entityQuery/bar") ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(EntityQueryResponse(defaultQuery, EntityQueryResultMetadata(0, 0, 0), Seq.empty)) {

            responseAs[EntityQueryResponse]
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected =
        expectedHttpRequestMetrics("get", s"$wsPathForRequestMetrics.entityQuery.bar", StatusCodes.OK.intValue, 1) ++
          expectedHttpRequestMetrics("post",
                                     s"$wsPathForRequestMetrics.entities.delete",
                                     StatusCodes.NoContent.intValue,
                                     1
          ) ++
          expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.entities", StatusCodes.Created.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 200 OK on entity query when all results are filtered" in withPaginationTestDataApiServices {
    services =>
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?filterTerms=qqq") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(
            EntityQueryResponse(defaultQuery.copy(filterTerms = Option("qqq")),
                                EntityQueryResultMetadata(paginationTestData.entities.size, 0, 0),
                                Seq.empty
            )
          ) {

            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "return 200 OK and use default AND operator on filterTerms when a filterOperator is not specified" in withTestDataApiServices {
    services =>
      val fooEntity =
        Entity("firstSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo")))
      val barEntity =
        Entity("secondSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("bar")))
      val fooBarEntity = Entity(
        "thirdSample",
        "sample",
        Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo"),
            AttributeName.withDefaultNS("attrname2") -> AttributeString("bar")
        )
      )

      runAndWait(entityQuery.save(testData.workspaceNoEntities, Seq(fooEntity, barEntity, fooBarEntity)))

      Get(s"${testData.workspaceNoEntities.path}/entityQuery/${fooEntity.entityType}?filterTerms=foo%20bar") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Seq(fooBarEntity)) {
            responseAs[EntityQueryResponse].results
          }
        }
  }

  it should "return 200 OK and use AND operator on filterTerms when AND filterOperator is specified" in withTestDataApiServices {
    services =>
      val fooEntity =
        Entity("firstSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo")))
      val barEntity =
        Entity("secondSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("bar")))
      val fooBarEntity = Entity(
        "thirdSample",
        "sample",
        Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo"),
            AttributeName.withDefaultNS("attrname2") -> AttributeString("bar")
        )
      )

      runAndWait(entityQuery.save(testData.workspaceNoEntities, Seq(fooEntity, barEntity, fooBarEntity)))

      Get(
        s"${testData.workspaceNoEntities.path}/entityQuery/${fooEntity.entityType}?filterTerms=foo%20bar&filterOperator=AND"
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Seq(fooBarEntity)) {
            responseAs[EntityQueryResponse].results
          }
        }
  }

  it should "return 200 OK and use OR operator on filterTerms when OR filterOperator is specified" in withTestDataApiServices {
    services =>
      val fooEntity =
        Entity("firstSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo")))
      val barEntity =
        Entity("secondSample", "sample", Map(AttributeName.withDefaultNS("attrname") -> AttributeString("bar")))
      val fooBarEntity = Entity(
        "thirdSample",
        "sample",
        Map(AttributeName.withDefaultNS("attrname") -> AttributeString("foo"),
            AttributeName.withDefaultNS("attrname2") -> AttributeString("bar")
        )
      )

      runAndWait(entityQuery.save(testData.workspaceNoEntities, Seq(fooEntity, barEntity, fooBarEntity)))

      Get(
        s"${testData.workspaceNoEntities.path}/entityQuery/${fooEntity.entityType}?filterTerms=foo%20bar&filterOperator=OR"
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Seq(fooEntity, barEntity, fooBarEntity)) {
            responseAs[EntityQueryResponse].results
          }
        }
  }

  it should "return the right page on entity query" in withPaginationTestDataApiServices { services =>
    val page = 5
    val offset = (page - 1) * defaultQuery.pageSize
    Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?page=$page") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(page = page),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      paginationTestData.numEntities,
                                      calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
            ),
            paginationTestData.entities.sortBy(_.name).slice(offset, offset + defaultQuery.pageSize)
          )
        ) {

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
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(pageSize = pageSize),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      paginationTestData.numEntities,
                                      calculateNumPages(paginationTestData.numEntities, pageSize)
            ),
            paginationTestData.entities.sortBy(_.name).slice(offset, offset + pageSize)
          )
        ) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return sorted results on entity query for number field" in withPaginationTestDataApiServices { services =>
    withStatsD {
      Get(s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=number") ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(
            EntityQueryResponse(
              defaultQuery.copy(sortField = "number"),
              EntityQueryResultMetadata(paginationTestData.numEntities,
                                        paginationTestData.numEntities,
                                        calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
              ),
              paginationTestData.entities
                .sortBy(_.attributes(AttributeName.withDefaultNS("number")).asInstanceOf[AttributeNumber].value)
                .take(defaultQuery.pageSize)
            )
          ) {

            responseAs[EntityQueryResponse]
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected =
        expectedHttpRequestMetrics("get",
                                   s"$wsPathForRequestMetrics.entityQuery.${paginationTestData.entityType}",
                                   StatusCodes.OK.intValue,
                                   1
        )
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return sorted results on entity query for string field" in withPaginationTestDataApiServices { services =>
    Get(
      s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=random&sordDirection=asc"
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(sortField = "random"),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      paginationTestData.numEntities,
                                      calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
            ),
            paginationTestData.entities
              .sortBy(_.attributes(AttributeName.withDefaultNS("random")).asInstanceOf[AttributeString].value)
              .take(defaultQuery.pageSize)
          )
        ) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  private def entityLessThan(attributeName: String)(e1: Entity, e2: Entity): Boolean =
    (e1.attributes(AttributeName.withDefaultNS(attributeName)),
     e2.attributes(AttributeName.withDefaultNS(attributeName))
    ) match {
      case (AttributeNumber(v1), AttributeNumber(v2)) if v1 == v2 => e1.name < e2.name
      case (AttributeNumber(v1), AttributeNumber(v2))             => v1 < v2
      case (AttributeNumber(v), _)                                => true
      case (_, AttributeNumber(v))                                => false

      case (AttributeString(v1), AttributeString(v2)) if v1 == v2 => e1.name < e2.name
      case (AttributeString(v1), AttributeString(v2))             => v1 < v2
      case (AttributeString(v), _)                                => true
      case (_, AttributeString(v))                                => false

      case (l1: AttributeList[_], l2: AttributeList[_]) if l1.list.length == l2.list.length => e1.name < e2.name
      case (l1: AttributeList[_], l2: AttributeList[_]) => l1.list.length < l2.list.length
      case (l1: AttributeList[_], _)                    => true
      case (_, l2: AttributeList[_])                    => false

      case (_, _) => throw new RawlsException("case not covered")

    }

  it should "return sorted results on entity query for mixed typed field" in withPaginationTestDataApiServices {
    services =>
      Get(
        s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=mixed&pageSize=${paginationTestData.numEntities}"
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          assertResult(
            EntityQueryResponse(
              defaultQuery.copy(sortField = "mixed", pageSize = paginationTestData.numEntities),
              EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, 1),
              paginationTestData.entities.sortWith(entityLessThan("mixed"))
            )
          ) {
            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "return sorted results on entity query for mixed numeric-type field including lists" in withPaginationTestDataApiServices {
    services =>
      Get(
        s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=mixedNumeric&pageSize=${paginationTestData.numEntities}"
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          assertResult(
            EntityQueryResponse(
              defaultQuery.copy(sortField = "mixedNumeric", pageSize = paginationTestData.numEntities),
              EntityQueryResultMetadata(paginationTestData.numEntities, paginationTestData.numEntities, 1),
              paginationTestData.entities.sortWith(entityLessThan("mixedNumeric"))
            )
          ) {
            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "return sorted results on entity query for sparse field" in withPaginationTestDataApiServices { services =>
    Get(
      s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=sparse&pageSize=${paginationTestData.numEntities}"
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        // all the entities without values should be last, drop them then make sure the resulting list is sorted right
        val resultEntities = responseAs[EntityQueryResponse].results
        assertResult(
          paginationTestData.entities
            .filter(_.attributes.getOrElse(AttributeName.withDefaultNS("sparse"), AttributeNull) != AttributeNull)
            .sortBy(_.attributes(AttributeName.withDefaultNS("sparse")).asInstanceOf[AttributeNumber].value)
        ) {
          resultEntities.dropWhile(
            _.attributes.getOrElse(AttributeName.withDefaultNS("sparse"), AttributeNull) == AttributeNull
          )
        }
      }
  }

  it should "return sorted results on entity query for namespaced attributes" in withPaginationTestDataApiServices {
    services =>
      val sortAttr = AttributeName.fromDelimitedName("pfb:number")

      Get(
        s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=${toDelimitedName(sortAttr)}"
      ) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(
            EntityQueryResponse(
              defaultQuery.copy(sortField = toDelimitedName(sortAttr)),
              EntityQueryResultMetadata(paginationTestData.numEntities,
                                        paginationTestData.numEntities,
                                        calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
              ),
              paginationTestData.entities
                .sortBy(_.attributes(sortAttr).asInstanceOf[AttributeNumber].value)
                .take(defaultQuery.pageSize)
            )
          ) {

            responseAs[EntityQueryResponse]
          }
        }
  }

  it should "return sorted results on entity query descending" in withPaginationTestDataApiServices { services =>
    Get(
      s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?sortField=random&sortDirection=desc"
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(sortField = "random", sortDirection = Descending),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      paginationTestData.numEntities,
                                      calculateNumPages(paginationTestData.numEntities, defaultQuery.pageSize)
            ),
            paginationTestData.entities
              .sortBy(_.attributes(AttributeName.withDefaultNS("random")).asInstanceOf[AttributeString].value)
              .reverse
              .take(defaultQuery.pageSize)
          )
        ) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  it should "return filtered results on entity query" in withPaginationTestDataApiServices { services =>
    val pageSize = paginationTestData.entities.size
    val vocab1Term = paginationTestData.vocab1Strings(0)
    val vocab2Term = paginationTestData.vocab2Strings(1)
    val expectedEntities = paginationTestData.entities
      .filter(e =>
        e.attributes(AttributeName.withDefaultNS("vocab1")) == AttributeString(vocab1Term) && e.attributes(
          AttributeName.withDefaultNS("vocab2")
        ) == AttributeString(vocab2Term)
      )
      .sortBy(_.name)
    Get(
      s"${paginationTestData.workspace.path}/entityQuery/${paginationTestData.entityType}?pageSize=$pageSize&filterTerms=$vocab1Term%20$vocab2Term"
    ) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(
          EntityQueryResponse(
            defaultQuery.copy(pageSize = pageSize, filterTerms = Option(s"$vocab1Term $vocab2Term")),
            EntityQueryResultMetadata(paginationTestData.numEntities,
                                      expectedEntities.size,
                                      calculateNumPages(expectedEntities.size, pageSize)
            ),
            expectedEntities
          )
        ) {

          responseAs[EntityQueryResponse]
        }
      }
  }

  // *********** START entityQuery field-selection tests

  // creates 30 entities, in groups of 10; each group has different attributes, with some overlap.
  class FieldSelectionTestData extends TestData {
    val userOwner = RawlsUser(
      UserInfo(RawlsUserEmail("owner-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212345")
      )
    )
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set())
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set())

    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    val colorGroup1 = List("allgroups", "group1and2", "red", "yellow", "blue", "brown", "orange")
    val colorGroup2 = List("allgroups", "group1and2", "group2and3", "green", "violet", "black", "carnation", "white")
    val colorGroup3 = List("allgroups", "group2and3", "dandelion", "cerulean", "apricot", "scarlet", "gray")

    val numEntities = 100
    val entityType = "fieldselect"

    val entities = List(colorGroup1, colorGroup2, colorGroup3).zipWithIndex.flatMap { case (colorList, groupIdx) =>
      (0 to 9) map { idx =>
        val attributes =
          colorList.map(color => AttributeName.withDefaultNS(color) -> AttributeString(s"$color value")).toMap
        // default sorting for entityQuery is by name, so ensure the names we generate here are in
        // ascending order of insertion
        Entity(s"$groupIdx-$idx", entityType, attributes)
      }
    }

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        entityQuery.save(workspace, entities)
      )
    }
  }

  val fieldSelectionTestData = new FieldSelectionTestData()

  def withFieldSelectionTestDataApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(fieldSelectionTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  val fieldSelectionApiPath =
    s"${fieldSelectionTestData.workspace.path}/entityQuery/${fieldSelectionTestData.entityType}"

  def assertFieldSelection(actualResponse: EntityQueryResponse, expectedFields: List[String], expectedPageSize: Int) = {
    withClue("when checking results page size, ") {
      actualResponse.results.size shouldBe expectedPageSize
    }
    // calculate actual field list
    val actual = actualResponse.results.flatMap(e => e.attributes.keySet.map(a => AttributeName.toDelimitedName(a)))
    actual.toSet should contain theSameElementsAs expectedFields.toSet
  }

  it should "return all attributes, name, and entityType if the fields parameter is omitted (all entities)" in withFieldSelectionTestDataApiServices {
    services =>
      // query for all entities
      Get(s"$fieldSelectionApiPath?pageSize=${fieldSelectionTestData.entities.size}") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected =
            fieldSelectionTestData.colorGroup1 ++ fieldSelectionTestData.colorGroup2 ++ fieldSelectionTestData.colorGroup3
          assertFieldSelection(resp, expected, fieldSelectionTestData.entities.size)
        }
  }

  it should "return all attributes, name, and entityType if the fields parameter is omitted (first page)" in withFieldSelectionTestDataApiServices {
    services =>
      // query for first page of results
      Get(s"$fieldSelectionApiPath?pageSize=10") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected = fieldSelectionTestData.colorGroup1
          assertFieldSelection(resp, expected, 10)
        }
  }

  it should "return all attributes, name, and entityType if the fields parameter is omitted (second page)" in withFieldSelectionTestDataApiServices {
    services =>
      // query for second page of results
      Get(s"$fieldSelectionApiPath?pageSize=10&page=2") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected = fieldSelectionTestData.colorGroup2
          assertFieldSelection(resp, expected, 10)
        }
  }

  it should "return all attributes, name, and entityType if the fields parameter is omitted (second page extending into third page)" in withFieldSelectionTestDataApiServices {
    services =>
      // query for the second page of results, with a page size that extends us into the third
      // group of entities from fieldSelectionTestData
      Get(s"$fieldSelectionApiPath?pageSize=15&page=2") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected = fieldSelectionTestData.colorGroup2 ++ fieldSelectionTestData.colorGroup3
          assertFieldSelection(resp, expected, 15)
        }
  }

  List(List("name"), List("entityType"), List("name", "entityType")) foreach { fieldsUnderTest =>
    it should s"return [${fieldsUnderTest.mkString(", ")}] if requested in fields" ignore withFieldSelectionTestDataApiServices {
      services =>
        Get(s"$fieldSelectionApiPath?pageSize=25&page=1&fields=${fieldsUnderTest.mkString(",")}") ~>
          sealRoute(services.entityRoutes) ~>
          check {
            status shouldBe StatusCodes.OK
            val resp = responseAs[EntityQueryResponse]
            assertFieldSelection(resp, fieldsUnderTest, 25)
          }
    }
  }

  /*
    val colorGroup1 = List("allgroups", "group1and2", "red", "yellow", "blue", "brown", "orange")
    val colorGroup2 = List("allgroups", "group1and2", "group2and3", "green", "violet", "black", "carnation", "white")
    val colorGroup3 = List("allgroups", "group2and3", "dandelion", "cerulean", "apricot", "scarlet", "gray")
   */
  case class FieldsTestCase(pageSize: Int, page: Int, fieldsUnderTest: List[String])
  // see FieldSelectionTestData for expected attribute names
  val testCases = List(
    // test cases for the first group
    FieldsTestCase(10, 1, fieldSelectionTestData.colorGroup1),
    FieldsTestCase(10, 1, List("allgroups", "group1and2", "red")),
    FieldsTestCase(10, 1, List("red", "yellow")),
    FieldsTestCase(10, 1, List("orange")),
    // test cases for the second group
    FieldsTestCase(10, 2, fieldSelectionTestData.colorGroup2),
    FieldsTestCase(10, 2, List("allgroups", "group1and2", "group2and3", "green")),
    FieldsTestCase(10, 2, List("black", "carnation")),
    FieldsTestCase(10, 2, List("violet")),
    // test cases for the third group
    FieldsTestCase(10, 3, fieldSelectionTestData.colorGroup3),
    FieldsTestCase(10, 3, List("allgroups", "group2and3", "dandelion")),
    FieldsTestCase(10, 3, List("cerulean", "apricot")),
    FieldsTestCase(10, 3, List("gray")),
    // test cases for all three groups combined
    FieldsTestCase(
      25,
      1,
      fieldSelectionTestData.colorGroup1 ++ fieldSelectionTestData.colorGroup2 ++ fieldSelectionTestData.colorGroup3
    ),
    FieldsTestCase(25, 1, List("allgroups", "group1and2", "group2and3", "red", "green", "dandelion")),
    FieldsTestCase(25, 1, List("yellow", "blue", "violet", "black", "cerulean", "apricot")),
    FieldsTestCase(25, 1, List("yellow", "blue")),
    FieldsTestCase(25, 1, List("violet", "black")),
    FieldsTestCase(25, 1, List("cerulean", "apricot")),
    FieldsTestCase(25, 1, List("orange", "scarlet"))
  )

  testCases foreach { testCase =>
    val fieldsUnderTest = testCase.fieldsUnderTest
    it should s"return only the requested fields (pageSize ${testCase.pageSize}, page ${testCase.page}, fields ${testCase.fieldsUnderTest})" in withFieldSelectionTestDataApiServices {
      services =>
        Get(
          s"$fieldSelectionApiPath?pageSize=${testCase.pageSize}&page=${testCase.page}&fields=${fieldsUnderTest.mkString(",")}"
        ) ~>
          sealRoute(services.entityRoutes) ~>
          check {
            status shouldBe StatusCodes.OK
            val resp = responseAs[EntityQueryResponse]
            assertFieldSelection(resp, fieldsUnderTest, testCase.pageSize)
          }
    }
  }

  it should "return no attributes if requested field exists, but not in this page of results" in withFieldSelectionTestDataApiServices {
    services =>
      // get the first page of results, but request a field from the second page
      Get(s"$fieldSelectionApiPath?pageSize=10&page=1&fields=violet") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          assertFieldSelection(resp, List(), 10)
        }
  }

  it should "return no attributes if unrecognized field names in parameter" in withFieldSelectionTestDataApiServices {
    services =>
      // request a totally nonexistent field
      Get(s"$fieldSelectionApiPath?pageSize=10&page=1&fields=nonexistent") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          assertFieldSelection(resp, List(), 10)
        }
  }

  it should "return requested fields in conjunction with sort and filter" in withFieldSelectionTestDataApiServices {
    services =>
      // request all 30 results, but use a filter term that should only return the third page.
      // request fields from all pages, but expect only the third page's fields back.
      Get(
        s"$fieldSelectionApiPath?pageSize=30&page=1&filterTerms=gray&sortField=red&sortDirection=desc&fields=yellow,green,apricot"
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected = List("apricot")
          assertFieldSelection(resp, expected, 10)
        }
  }

  it should "return no attributes if field list is empty, i.e. 'fields='" in withFieldSelectionTestDataApiServices {
    services =>
      // query for all entities
      Get(s"$fieldSelectionApiPath?pageSize=${fieldSelectionTestData.entities.size}&fields=") ~>
        sealRoute(services.entityRoutes) ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[EntityQueryResponse]
          val expected = List()
          assertFieldSelection(resp, expected, fieldSelectionTestData.entities.size)
        }
  }
  // *********** END entityQuery field-selection tests

  "Entity type metadata API" should "pass true by default to useCache argument" in withMockedEntityService { services =>
    Get(s"${constantData.workspace.path}/entities") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    val argumentCaptor = captor[Boolean]
    verify(services.service).entityTypeMetadata(any[WorkspaceName],
                                                any[Option[DataReferenceName]],
                                                any[Option[GoogleProjectId]],
                                                argumentCaptor.capture()
    )
    assert(argumentCaptor.getValue)
  }

  it should "respect useCache parameter if set to false" in withMockedEntityService { services =>
    Get(s"${constantData.workspace.path}/entities?useCache=false") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    val argumentCaptor = captor[Boolean]
    verify(services.service).entityTypeMetadata(any[WorkspaceName],
                                                any[Option[DataReferenceName]],
                                                any[Option[GoogleProjectId]],
                                                argumentCaptor.capture()
    )
    assert(!argumentCaptor.getValue)
  }

  it should "default to true if useCache set to some value other than 'false'" in withMockedEntityService { services =>
    Get(s"${constantData.workspace.path}/entities?useCache=mysterious") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
    val argumentCaptor = captor[Boolean]
    verify(services.service).entityTypeMetadata(any[WorkspaceName],
                                                any[Option[DataReferenceName]],
                                                any[Option[GoogleProjectId]],
                                                argumentCaptor.capture()
    )
    assert(argumentCaptor.getValue)
  }

}
