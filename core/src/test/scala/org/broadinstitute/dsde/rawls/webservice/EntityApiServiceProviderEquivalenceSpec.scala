package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{handleExceptions, handleRejections}
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
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
  ErrorReport,
  Workspace
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json.{JsArray, JsNumber, JsObject, JsString}

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
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  // withMinimalTestDatabase has two workspaces, each starts with no entities
  def withMinimalDatabaseApiServices[T](testCode: TestApiService => T): T =
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
    "no attributes" -> Entity("myName", "myType", Map()),
    "simple attributes" -> Entity("myName",
                                  "myType",
                                  Map(
                                    AttributeName.withDefaultNS("str") -> AttributeString("hello"),
                                    AttributeName.withDefaultNS("num") -> AttributeNumber(42)
                                  )
    ),
    "namespaced attributes" -> Entity(
      "myName",
      "myType",
      Map(
        AttributeName.fromDelimitedName("pfb:str") -> AttributeString("hello"),
        AttributeName.fromDelimitedName("import:num") -> AttributeNumber(42)
      )
    ),
    "attribute value lists" -> Entity(
      "myName",
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
      "myName",
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
    it should s"work for an entity with $descriptor" in withMinimalDatabaseApiServices { services =>
      setupProviders(services) // needs to be inside withMinimalDatabaseApiServices

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

  it should s"work for an entity with references" in withMinimalDatabaseApiServices { services =>
    setupProviders(services) // needs to be inside withMinimalDatabaseApiServices

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

  behavior of "setupProviders helper"

  // this verifies that setup is correct; compactWs should use CompactEntityProvider and legacyWs should use
  // LocalEntityProvider
  it should " succeed for legacy, fail for compact on list_entities (not implemented yet)" in withMinimalDatabaseApiServices {
    services =>
      setupProviders(services) // needs to be inside withMinimalDatabaseApiServices

      withClue("For legacy workspace,") {
        Get(s"/workspaces/${legacyWs.namespace}/${legacyWs.name}/entities/some-type") ~>
          withHandlers(services.entityRoutes()) ~>
          check {
            status shouldBe StatusCodes.OK
          }
      }
      withClue("For compact workspace,") {
        Get(s"/workspaces/${compactWs.namespace}/${compactWs.name}/entities/some-type") ~>
          withHandlers(services.entityRoutes()) ~>
          check {
            status shouldBe StatusCodes.NotImplemented
            responseAs[ErrorReport].message shouldBe "list all entities not supported for compact data tables."
          }
      }
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
        withHandlers(services.entityRoutes()) ~>
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
        withHandlers(services.entityRoutes()) ~>
        check {
          status shouldBe StatusCodes.OK
          val retrieved = responseAs[Entity]
          retrieved
        }
    }

}
