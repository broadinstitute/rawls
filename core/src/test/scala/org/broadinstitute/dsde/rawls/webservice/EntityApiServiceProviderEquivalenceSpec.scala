package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{handleExceptions, handleRejections}
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddListMember,
  AddUpdateAttribute,
  CreateAttributeEntityReferenceList,
  CreateAttributeValueList,
  EntityUpdateDefinition
}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.CompactDataTablesConfig
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  AttributeValueRawJson,
  CompactDataTablesSetting,
  Entity,
  EntityTypeMetadata,
  Workspace
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsNumber, JsObject, JsString}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class EntityApiServiceProviderEquivalenceSpec extends ApiServiceSpec with SprayJsonSupport {

  // START boilerplate to get access to service APIs
  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      setupProviders(apiService) // tweaks to minimalTestData for this test suite
      testCode(apiService)
    } finally
      apiService.cleanupSupervisor
  }

  // withMinimalTestDatabase has two workspaces, each starts with no entities
  def withProviderEquivalenceApiServices[T](testCode: TestApiService => T): T =
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  // END boilerplate to get access to service APIs

  // shorthand variables for use in tests below
  private val legacyWs = minimalTestData.workspace // workspace using legacy data tables
  private val compactWs = minimalTestData.workspace2 // workspace using compact ("Quicksilver") data tables

  private val atMost = Duration("60 seconds") // timeout for Await() in tests

  // define different entities for use in tests
  private val entityTestCases = Map(
    "no attributes" -> Entity(UUID.randomUUID().toString, "myType", Map()),
    "simple attributes" -> Entity(
      UUID.randomUUID().toString,
      "myType",
      Map(
        AttributeName.withDefaultNS("str") -> AttributeString("hello"),
        AttributeName.withDefaultNS("num") -> AttributeNumber(42)
      )
    ),
    "namespaced attributes" -> Entity(
      UUID.randomUUID().toString,
      "myType",
      Map(
        AttributeName.fromDelimitedName("pfb:str") -> AttributeString("hello"),
        AttributeName.fromDelimitedName("import:num") -> AttributeNumber(42)
      )
    ),
    "attribute value lists" -> Entity(
      UUID.randomUUID().toString,
      "myType",
      Map(
        AttributeName.withDefaultNS("str") -> AttributeString("hello"),
        AttributeName.withDefaultNS("list") -> AttributeValueList(
          Seq(
            AttributeNumber(10),
            AttributeNumber(11),
            AttributeNumber(12)
          )
        )
      )
    ),
    "raw json attributes" -> Entity(
      UUID.randomUUID().toString,
      "myType",
      Map(
        AttributeName.withDefaultNS("str") -> AttributeString("hello"),
        AttributeName.withDefaultNS("jsonAttr") -> AttributeValueRawJson(
          JsObject(
            ("innerString", JsString("something")),
            ("innerNumber", JsNumber(123)),
            ("innerArray", JsArray(JsNumber(10), JsNumber(11), JsNumber(12)))
          )
        )
      )
    )
  )

  behavior of "POST and GET single entity"
  entityTestCases foreach { case (descriptor, entity) =>
    it should s"work for an entity with $descriptor" in withProviderEquivalenceApiServices { services =>
      // create entities; this validates the POST api
      createEntity(compactWs, entity, services)
      createEntity(legacyWs, entity, services)

      // re-retrieve the entities via the GET api
      val compact = getEntity(compactWs, entity.entityType, entity.name, services)
      val legacy = getEntity(legacyWs, entity.entityType, entity.name, services)

      compact shouldBe legacy
      compact shouldBe entity
      legacy shouldBe entity
    }
  }

  it should s"work for an entity with references" in withProviderEquivalenceApiServices { services =>
    val targetType = "target"

    // create entities to serve as reference targets
    List("one", "two", "three") foreach { entityName =>
      createEntity(compactWs, Entity(entityName, targetType, Map()), services)
      createEntity(legacyWs, Entity(entityName, targetType, Map()), services)
    }

    // define test entity
    val entity = Entity(
      "sourceName",
      "sourceType",
      Map(
        AttributeName.withDefaultNS("singleRef") -> AttributeEntityReference(targetType, "one"),
        AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference(targetType, "two"),
            AttributeEntityReference(targetType, "three")
          )
        )
      )
    )

    // create entities with references
    createEntity(compactWs, entity, services)
    createEntity(legacyWs, entity, services)

    // re-retrieve the entities via the GET api
    val compact = getEntity(compactWs, entity.entityType, entity.name, services)
    val legacy = getEntity(legacyWs, entity.entityType, entity.name, services)

    compact shouldBe legacy
    compact shouldBe entity
    legacy shouldBe entity
  }

  behavior of "GET entity type metadata"

  it should "work for legacy and compact workspaces" in withProviderEquivalenceApiServices { services =>
    entityTestCases.foreach { case (_, entity) =>
      // create entities; this validates the POST api
      createEntity(compactWs, entity, services)
      createEntity(legacyWs, entity, services)
    }

    val compactMetadata = getEntityTypeMetadata(compactWs, services)
    val legacyMetadata = getEntityTypeMetadata(legacyWs, services)

    compactMetadata shouldBe legacyMetadata
  }

  behavior of "POST batchUpsert"

  it should "create new entities" in withProviderEquivalenceApiServices { services =>
    // define the upsert payload
    val ent1 = AttributeEntityReference("typeA", "name1")
    val ent2 = AttributeEntityReference("typeA", "name2")
    val ent3 = AttributeEntityReference("typeB", "name3")
    val payload = Seq(
      EntityUpdateDefinition(
        ent1.entityName,
        ent1.entityType,
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("attr1"), AttributeString("value1")),
          AddUpdateAttribute(AttributeName.withDefaultNS("attr2"), AttributeString("value2"))
        )
      ),
      EntityUpdateDefinition(
        ent2.entityName,
        ent2.entityType,
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("attr1"), AttributeString("value1")),
          CreateAttributeValueList(AttributeName.withDefaultNS("valueList")),
          AddListMember(AttributeName.withDefaultNS("valueList"), AttributeString("list1")),
          AddListMember(AttributeName.withDefaultNS("valueList"), AttributeString("list2"))
        )
      ),
      EntityUpdateDefinition(
        ent3.entityName,
        ent3.entityType,
        Seq(
          CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refList")),
          AddListMember(AttributeName.withDefaultNS("refList"), AttributeEntityReference("typeA", "name1")),
          AddListMember(AttributeName.withDefaultNS("refList"), AttributeEntityReference("typeA", "name2"))
        )
      )
    )
    // perform the upserts
    Seq(legacyWs, compactWs) foreach { ws =>
      withClue(s"POST batchUpsert for workspace ${ws.toWorkspaceName}") {
        Post(s"/workspaces/${ws.namespace}/${ws.name}/entities/batchUpsert", httpJson(payload)) ~>
          withHandlers(services.entityRoutes(userInfo = userInfo)) ~>
          check {
            status shouldBe StatusCodes.NoContent
          }
      }
    }
    // compare the resultant entities
    Seq(ent1, ent2, ent3) foreach { ent =>
      val legacyEntity = getEntity(legacyWs, ent.entityType, ent.entityName, services)
      val compactEntity = getEntity(compactWs, ent.entityType, ent.entityName, services)
      legacyEntity shouldBe compactEntity
    }
  }

  behavior of "GET listEntities"

  it should "list entities" in withProviderEquivalenceApiServices { services =>
    // Create entities in both workspaces
    val entitiesToCreate = Seq(
      Entity("ent1", "myType", Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
      Entity("ent2", "myType", Map(AttributeName.withDefaultNS("baz") -> AttributeString("qux")))
    )
    entitiesToCreate.foreach { entity =>
      createEntity(compactWs, entity, services)
      createEntity(legacyWs, entity, services)
    }

    // List entities in both workspaces
    def listEntities(ws: Workspace): Seq[Entity] =
      Get(s"/workspaces/${ws.namespace}/${ws.name}/entities/myType") ~>
        withHandlers(services.entityRoutes(userInfo = userInfo)) ~>
        check {
          status shouldBe StatusCodes.OK
          responseAs[Seq[Entity]]
        }

    val compactEntities = listEntities(compactWs)
    val legacyEntities = listEntities(legacyWs)

    compactEntities should contain theSameElementsAs legacyEntities
    compactEntities.map(_.name) should contain theSameElementsAs entitiesToCreate.map(_.name)
  }

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  private def setupProviders(services: TestApiService): Unit = {
    // configure compactWs to use compact ("Quicksilver") data tables
    // no changes to legacyWs, so that will use legacy data tables
    val compactDataTablesSetting = CompactDataTablesSetting(CompactDataTablesConfig(enabled = true))
    val workspaceSettingService = services.workspaceSettingServiceConstructor(testContext)
    val workspaceSettingResponse = Await.result(
      workspaceSettingService.setWorkspaceSettings(compactWs.toWorkspaceName, List(compactDataTablesSetting)),
      atMost
    )
    workspaceSettingResponse.successes should have size 1
  }

  private def withHandlers(route: server.Route): server.Route =
    (handleExceptions(RawlsApiService.exceptionHandler) & handleRejections(RawlsApiService.rejectionHandler)) {
      route
    }

  /* create an entity in a workspace, validate its creation */
  private def createEntity(ws: Workspace, entity: Entity, services: TestApiService): Entity =
    withClue(s"createEntity helper for workspace ${ws.toWorkspaceName}") {
      Post(s"/workspaces/${ws.namespace}/${ws.name}/entities", httpJson(entity)) ~>
        withHandlers(services.entityRoutes(userInfo = userInfo)) ~>
        check {
          status shouldBe StatusCodes.Created
          val created = responseAs[Entity]
          created shouldBe entity
          created
        }
    }

  /* get an entity in a workspace */
  private def getEntity(ws: Workspace, entityType: String, entityName: String, services: TestApiService): Entity =
    withClue(s"getEntity helper for workspace ${ws.toWorkspaceName}") {
      Get(s"/workspaces/${ws.namespace}/${ws.name}/entities/$entityType/$entityName") ~>
        withHandlers(services.entityRoutes(userInfo = userInfo)) ~>
        check {
          status shouldBe StatusCodes.OK
          val retrieved = responseAs[Entity]
          retrieved
        }
    }

  /* get entity type metadata for a workspace */
  private def getEntityTypeMetadata(ws: Workspace, services: TestApiService): Map[String, EntityTypeMetadata] =
    withClue(s"getEntityTypeMetadata helper for workspace ${ws.toWorkspaceName}") {
      Get(s"/workspaces/${ws.namespace}/${ws.name}/entities") ~>
        withHandlers(services.entityRoutes(userInfo = userInfo)) ~>
        check {
          status shouldBe StatusCodes.OK
          val retrieved = responseAs[Map[String, EntityTypeMetadata]]
          retrieved
        }
    }

}
