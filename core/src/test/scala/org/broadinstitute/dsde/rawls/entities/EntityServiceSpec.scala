package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  EntityAndAttributesResult,
  EntityAttributeRecord,
  EntityRecord,
  TestDriverComponent
}
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactory,
  MockBigQueryServiceFactory,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{
  MockDataRepoDAO,
  MockSamDAO,
  MockWorkspaceManagerDAO,
  RemoteServicesMockServer
}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddListMember,
  AddUpdateAttribute,
  CreateAttributeEntityReferenceList,
  CreateAttributeValueList,
  RemoveAttribute,
  RemoveListMember
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeEntityReference,
  AttributeEntityReferenceEmptyList,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeRename,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  Entity,
  EntityQuery,
  EntityTypeRename,
  RawlsRequestContext,
  RawlsUser,
  SortDirections,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.broadinstitute.dsde.rawls.workspace.{AttributeNotFoundException, AttributeUpdateOperationException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

class EntityServiceSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with TestDriverComponent
    with RawlsTestUtils
    with Eventually
    with ScalaFutures
    with MockitoTestUtils
    with RawlsStatsDTestUtils
    with BeforeAndAfterAll {

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val s1 = Entity(
    "s1",
    "samples",
    Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("x"),
      AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
      AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
        Seq(AttributeEntityReference("participant", "p1"))
      ),
      AttributeName.withDefaultNS("splat") -> attributeList
    )
  )
  val workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    currentTime(),
    currentTime(),
    "test",
    Map.empty
  )

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit
    val executionContext: ExecutionContext
  ) extends EntityApiService
      with MockUserInfoDirectivesWithUser {
    private val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    lazy val entityService: EntityService = entityServiceConstructor(ctx1)

    def actorRefFactory = system
    val samDAO = new MockSamDAO(dataSource)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      EntityManager.defaultEntityManager(
        dataSource,
        new MockWorkspaceManagerDAO(),
        new MockDataRepoDAO(mockServer.mockServerBaseUrl),
        samDAO,
        bigQueryServiceFactory,
        DataRepoEntityProviderConfig(100, 10, 0),
        testConf.getBoolean("entityStatisticsCache.enabled"),
        testConf.getDuration("entities.queryTimeout"),
        workbenchMetricBaseName
      ),
      7 // <-- specifically chosen to be lower than the number of samples in "workspace" within testData
    ) _
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  "EntityService" should "add attribute to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("foo"))) {
      services.entityService
        .applyOperationsToEntity(
          s1,
          Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")))
        )
        .attributes
        .get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "update attribute in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("biz"))) {
      services.entityService
        .applyOperationsToEntity(s1,
                                 Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("biz")))
        )
        .attributes
        .get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "remove attribute from entity" in withTestDataServices { services =>
    assertResult(None) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(RemoveAttribute(AttributeName.withDefaultNS("foo"))))
        .attributes
        .get(AttributeName.withDefaultNS("foo"))
    }
  }

  it should "add item to existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(attributeList.list :+ AttributeString("new")))) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeString("new"))))
        .attributes
        .get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "add item to non-existing list in entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("new"))))) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("bob"), AttributeString("new"))))
        .attributes
        .get(AttributeName.withDefaultNS("bob"))
    }
  }

  it should "throw AttributeUpdateOperationException when trying to create a new empty list by inserting AttributeNull" in withTestDataServices {
    services =>
      intercept[AttributeUpdateOperationException] {
        services.entityService
          .applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("nolisthere"), AttributeNull)))
          .attributes
          .get(AttributeName.withDefaultNS("nolisthere"))
      }
  }

  it should "create empty AttributeEntityReferenceList" in withTestDataServices { services =>
    assertResult(Some(AttributeEntityReferenceEmptyList)) {
      services.entityService
        .applyOperationsToEntity(s1,
                                 Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("emptyRefList")))
        )
        .attributes
        .get(AttributeName.withDefaultNS("emptyRefList"))
    }
  }

  it should "not wipe existing AttributeEntityReferenceList when calling CreateAttributeEntityReferenceList" in withTestDataServices {
    services =>
      assertResult(Some(s1.attributes(AttributeName.withDefaultNS("refs")))) {
        services.entityService
          .applyOperationsToEntity(s1, Seq(CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refs"))))
          .attributes
          .get(AttributeName.withDefaultNS("refs"))
      }
  }

  it should "create empty AttributeValueList" in withTestDataServices { services =>
    assertResult(Some(AttributeValueEmptyList)) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("emptyValList"))))
        .attributes
        .get(AttributeName.withDefaultNS("emptyValList"))
    }
  }

  it should "not wipe existing AttributeValueList when calling CreateAttributeValueList" in withTestDataServices {
    services =>
      assertResult(Some(s1.attributes(AttributeName.withDefaultNS("splat")))) {
        services.entityService
          .applyOperationsToEntity(s1, Seq(CreateAttributeValueList(AttributeName.withDefaultNS("splat"))))
          .attributes
          .get(AttributeName.withDefaultNS("splat"))
      }
  }

  it should "do nothing to existing lists when adding AttributeNull" in withTestDataServices { services =>
    assertResult(Some(attributeList)) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(AddListMember(AttributeName.withDefaultNS("splat"), AttributeNull)))
        .attributes
        .get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "remove item from existing listing entity" in withTestDataServices { services =>
    assertResult(Some(AttributeValueList(Seq(AttributeString("b"), AttributeBoolean(true))))) {
      services.entityService
        .applyOperationsToEntity(s1, Seq(RemoveListMember(AttributeName.withDefaultNS("splat"), AttributeString("a"))))
        .attributes
        .get(AttributeName.withDefaultNS("splat"))
    }
  }

  it should "throw AttributeNotFoundException when removing from a list that does not exist" in withTestDataServices {
    services =>
      intercept[AttributeNotFoundException] {
        services.entityService.applyOperationsToEntity(
          s1,
          Seq(RemoveListMember(AttributeName.withDefaultNS("bingo"), AttributeString("a")))
        )
      }
  }

  it should "throw AttributeUpdateOperationException when remove from an attribute that is not a list" in withTestDataServices {
    services =>
      intercept[AttributeUpdateOperationException] {
        services.entityService.applyOperationsToEntity(
          s1,
          Seq(RemoveListMember(AttributeName.withDefaultNS("foo"), AttributeString("a")))
        )
      }
  }

  it should "throw AttributeUpdateOperationException when adding to an attribute that is not a list" in withTestDataServices {
    services =>
      intercept[AttributeUpdateOperationException] {
        services.entityService.applyOperationsToEntity(
          s1,
          Seq(AddListMember(AttributeName.withDefaultNS("foo"), AttributeString("a")))
        )
      }
  }

  it should "apply attribute updates in order to entity" in withTestDataServices { services =>
    assertResult(Some(AttributeString("splat"))) {
      services.entityService
        .applyOperationsToEntity(
          s1,
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo")),
            AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("bar")),
            AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("splat"))
          )
        )
        .attributes
        .get(AttributeName.withDefaultNS("newAttribute"))
    }
  }

  it should "fail to rename an entity type to a name already in use" in withTestDataServices { services =>
    val waitDuration = Duration(10, SECONDS)
    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.entityService.renameEntityType(testData.wsName,
                                                           testData.pair1.entityType,
                                                           EntityTypeRename(testData.pair1.entityType)
                   ),
                   waitDuration
      )
    }
    ex.errorReport.message shouldBe "Pair already exists as an entity type"
  }

  it should "rename an entity type as long as the selected name is not in use" in withTestDataServices { services =>
    val waitDuration = Duration(10, SECONDS)
    assertResult(2) {
      Await.result(services.entityService.renameEntityType(testData.wsName,
                                                           testData.pair1.entityType,
                                                           EntityTypeRename("newPair")
                   ),
                   waitDuration
      )
    }
    // verify there are no longer any entities under the old entity name
    val entitySource =
      Await.result(services.entityService.listEntities(testData.wsName, testData.pair1.entityType), waitDuration)
    val queryResult = Await.result(entitySource.runWith(Sink.seq), waitDuration)
    assert(queryResult.isEmpty)
  }

  it should "throw an error when trying to rename entity that does not exist" in withTestDataServices { services =>
    val waitDuration = Duration(10, SECONDS)
    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        services.entityService.renameEntityType(testData.wsName, "non-existent-type", EntityTypeRename("new-name")),
        waitDuration
      )
    }
    ex.errorReport.message shouldBe "Can't find entity type non-existent-type"
    ex.errorReport.statusCode shouldBe Some(StatusCodes.NotFound)
  }

  it should "respect page size limits for queryEntities" in withTestDataServices { services =>
    val waitDuration = Duration(10, SECONDS)

    // the entityServiceConstructor inside TestApiService sets a pageSizeLimit of 7, so:
    // these should pass
    List(1, 5, 6, 7) foreach { pageSize =>
      withClue(s"for page size '$pageSize':") {
        val entityQuery = EntityQuery(1, pageSize, "name", SortDirections.Ascending, None)
        val queryResult = Await.result(
          services.entityService.queryEntities(testData.wsName, None, testData.sample1.entityType, entityQuery, None),
          waitDuration
        )
        queryResult.results should not be empty
      }
    }
    // these should fail
    List(8, 100, 250000, Int.MaxValue) foreach { pageSize =>
      withClue(s"for page size '$pageSize':") {
        val entityQuery = EntityQuery(1, pageSize, "name", SortDirections.Ascending, None)
        val ex = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            services.entityService.queryEntities(testData.wsName, None, testData.sample1.entityType, entityQuery, None),
            waitDuration
          )
        }
        ex.errorReport.message shouldBe "Page size cannot exceed 7"
      }
    }
  }

  it should "fail to rename an attribute name to a name already in use" in withTestDataServices { services =>
    val waitDuration = Duration(10, SECONDS)
    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        services.entityService.renameAttribute(testData.wsName,
                                               testData.pair1.entityType,
                                               AttributeName.withDefaultNS("case"),
                                               AttributeRename(AttributeName.withDefaultNS("control"))
        ),
        waitDuration
      )
    }
    ex.errorReport.message shouldBe "control already exists as an attribute name"
    ex.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  it should "rename an attribute name as long as the selected name is not in use" in withTestDataServices { services =>
    val oldAttributeName = AttributeName.withDefaultNS("case")
    val waitDuration = Duration(10, SECONDS)
    val rowsUpdated = Await.result(
      services.entityService.renameAttribute(testData.wsName,
                                             testData.pair1.entityType,
                                             oldAttributeName,
                                             AttributeRename(AttributeName.withDefaultNS("newAttributeName"))
      ),
      waitDuration
    )
    rowsUpdated shouldBe 2

    // verify there are no longer any attributes under the old attribute name
    val queryResult =
      Await.result(services.entityService.listEntities(testData.wsName, testData.pair1.entityType), waitDuration)
    queryResult map { entity =>
      entity.attributes.keySet shouldNot contain(oldAttributeName)
    }
  }

  it should "throw an error when trying to rename an attribute that does not exist" in withTestDataServices {
    services =>
      val waitDuration = Duration(10, SECONDS)
      val ex = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.entityService.renameAttribute(testData.wsName,
                                                 testData.pair1.entityType,
                                                 AttributeName.withDefaultNS("non-existent-attribute"),
                                                 AttributeRename(AttributeName.withDefaultNS("any"))
          ),
          waitDuration
        )
      }
      ex.errorReport.message shouldBe "Can't find attribute name non-existent-attribute"
      ex.errorReport.statusCode shouldBe Some(StatusCodes.NotFound)
  }

  // all following tests can use the same exemplar data, no need to re-create it for each unit test
  withTestDataServices { services =>
    // helper methods for these tests
    val workspaceId = UUID.randomUUID()
    val entityRecordProto = EntityRecord(1, "my-name", "my-type", workspaceId, 1, false, None)
    val entityAttributeRecordProto =
      EntityAttributeRecord(1, 1, "default", "attrname", None, None, None, None, None, None, None, false, None)

    "EntityService.gatherEntities" should "handle empty list" in {
      val input = Source.empty[EntityAndAttributesResult]
      val actual =
        EntityStreamingUtils.gatherEntities(services.entityService.dataSource, input).runWith(Sink.seq).futureValue

      assertResult(0, "actual result should be empty")(actual.size)
    }

    it should "handle single-entity list" in {
      val input = Source.fromIterator[EntityAndAttributesResult](() =>
        Seq(
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "first", valueString = Option("foo"))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "second", valueString = Option("bar"))),
            None
          )
        ).iterator
      )

      val actual =
        EntityStreamingUtils.gatherEntities(services.entityService.dataSource, input).runWith(Sink.seq).futureValue

      assertResult(
        Seq(
          Entity("my-name",
                 "my-type",
                 Map(AttributeName.withDefaultNS("first") -> AttributeString("foo"),
                     AttributeName.withDefaultNS("second") -> AttributeString("bar")
                 )
          )
        ),
        "actual result should have one entity"
      )(actual)
    }

    it should "handle multiple-entity list" in {
      val input = Source.fromIterator[EntityAndAttributesResult](() =>
        Seq(
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "first", valueString = Option("foo"))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "second", valueEntityRef = Option(999))),
            Option(entityRecordProto.copy(id = 999, entityType = "referenced-type", name = "referenced-entity"))
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 2, name = "entity-two"),
            Option(entityAttributeRecordProto.copy(name = "first", valueString = Option("baz"))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 3, name = "entity-three"),
            Option(entityAttributeRecordProto.copy(name = "more", valueNumber = Option(34))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 3, name = "entity-three"),
            Option(entityAttributeRecordProto.copy(name = "moremore", valueEntityRef = Option(999))),
            Option(entityRecordProto.copy(id = 999, entityType = "referenced-type", name = "referenced-entity"))
          )
        ).iterator
      )

      val actual =
        EntityStreamingUtils.gatherEntities(services.entityService.dataSource, input).runWith(Sink.seq).futureValue

      assertResult(
        Seq(
          Entity(
            "my-name",
            "my-type",
            Map(
              AttributeName.withDefaultNS("first") -> AttributeString("foo"),
              AttributeName.withDefaultNS("second") -> AttributeEntityReference("referenced-type", "referenced-entity")
            )
          ),
          Entity("entity-two", "my-type", Map(AttributeName.withDefaultNS("first") -> AttributeString("baz"))),
          Entity(
            "entity-three",
            "my-type",
            Map(
              AttributeName.withDefaultNS("more") -> AttributeNumber(34),
              AttributeName.withDefaultNS("moremore") -> AttributeEntityReference("referenced-type",
                                                                                  "referenced-entity"
              )
            )
          )
        ),
        "actual result should have three entities"
      )(actual)
    }

    it should "handle an entity with no attributes" in {
      // entity "entity-two" has no attrs
      val input = Source.fromIterator[EntityAndAttributesResult](() =>
        Seq(
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "first", valueString = Option("foo"))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto,
            Option(entityAttributeRecordProto.copy(name = "second", valueString = Option("bar"))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 2, name = "entity-two"),
            None,
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 3, name = "entity-three"),
            Option(entityAttributeRecordProto.copy(name = "more", valueNumber = Option(34))),
            None
          ),
          EntityAndAttributesResult(
            entityRecordProto.copy(id = 3, name = "entity-three"),
            Option(entityAttributeRecordProto.copy(name = "moremore", valueNumber = Option(45))),
            None
          )
        ).iterator
      )

      val actual =
        EntityStreamingUtils.gatherEntities(services.entityService.dataSource, input).runWith(Sink.seq).futureValue

      assertResult(
        Seq(
          Entity("my-name",
                 "my-type",
                 Map(AttributeName.withDefaultNS("first") -> AttributeString("foo"),
                     AttributeName.withDefaultNS("second") -> AttributeString("bar")
                 )
          ),
          Entity("entity-two", "my-type", Map.empty),
          Entity("entity-three",
                 "my-type",
                 Map(AttributeName.withDefaultNS("more") -> AttributeNumber(34),
                     AttributeName.withDefaultNS("moremore") -> AttributeNumber(45)
                 )
          )
        ),
        "actual result should have three entities"
      )(actual)
    }

  }

}
