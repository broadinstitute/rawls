package org.broadinstitute.dsde.rawls.entities

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactory,
  MockBigQueryServiceFactory,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, RemoveAttribute}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  Entity,
  RawlsRequestContext,
  RawlsUser,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, MockitoTestUtils}
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.ExecutionContext

class EntityShardingSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with TestDriverComponent
    with AttributeSupport
    with StatsDTestUtils
    with Eventually
    with MockitoTestUtils {

  import driver.api._

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit
    val executionContext: ExecutionContext
  ) extends EntityApiService
      with MockUserInfoDirectivesWithUser {
    private val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    lazy val entityService: EntityService = entityServiceConstructor(ctx1)

    def actorRefFactory = ActorSystem()
    val samDAO = new MockSamDAO(dataSource)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName = "test",
      EntityManager.defaultEntityManager(
        dataSource,
        new MockWorkspaceManagerDAO(),
        new MockDataRepoDAO("mockrepo"),
        samDAO,
        bigQueryServiceFactory,
        DataRepoEntityProviderConfig(100, 10, 0),
        testConf.getBoolean("entityStatisticsCache.enabled"),
        workbenchMetricBaseName
      ),
      1000
    ) _
  }

  val testWorkspace = new EmptyWorkspace
  val testWorkspaceName = testWorkspace.workspace.toWorkspaceName
  val testWorkspaceShardId = determineShard(testWorkspace.workspace.workspaceIdAsUUID)

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeString("c")))
  val s1 = Entity(
    "s1",
    "samples",
    Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("x"),
      AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
      AttributeName.withDefaultNS("bool") -> AttributeBoolean(false),
      AttributeName.withDefaultNS("splat") -> attributeList
    )
  )

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspace) { dataSource: SlickDataSource =>
      withServices(dataSource, testWorkspace.userOwner)(testCode)
    }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  // checks row counts for each shard
  def checkShardCounts(expected: Map[ShardId, Int] = Map()): Unit = {
    // default to 0 for all shards
    val default: Map[ShardId, Int] = allShards.map(_ -> 0).toMap
    val combined = default ++ expected
    combined.foreach { case (shardId, expectedCount) =>
      val actualCount = runAndWait(uniqueResult(sql"select count(1) from ENTITY_ATTRIBUTE_#$shardId".as[Int]))
        .getOrElse(throw new Exception("did not return a count!"))
      withClue(s"for table ENTITY_ATTRIBUTE_$shardId, actual row count of ") {
        actualCount shouldBe expectedCount
      }
    }
  }

  // tests start with an empty workspace
  "Entity attribute sharding" should "start with zero rows in each shard and the archive table" in withTestDataServices {
    _ =>
      checkShardCounts()
  }

  it should "only insert rows into the workspace's shard" in withTestDataServices { services =>
    val createdEntity = services.entityService.createEntity(testWorkspaceName, s1).futureValue
    createdEntity shouldBe s1
    checkShardCounts(Map(testWorkspaceShardId -> 6)) // 3 scalar attributes, 1 list attribute with 3 values
  }

  it should "only delete rows from the workspace's shard" in withTestDataServices { services =>
    val createdEntity = services.entityService.createEntity(testWorkspaceName, s1).futureValue
    createdEntity shouldBe s1

    checkShardCounts(Map(testWorkspaceShardId -> 6)) // 3 scalar attributes, 1 list attribute with 3 values

    services.entityService
      .updateEntity(testWorkspaceName, s1.entityType, s1.name, Seq(RemoveAttribute(AttributeName.withDefaultNS("bar"))))
      .futureValue

    withClue("after removing an attribute value") {
      checkShardCounts(Map(testWorkspaceShardId -> 5)) // we removed one attribute
    }
  }

  it should "only update rows in the workspace's shard" in withTestDataServices { services =>
    val createdEntity = services.entityService.createEntity(testWorkspaceName, s1).futureValue
    createdEntity shouldBe s1

    checkShardCounts(Map(testWorkspaceShardId -> 6)) // 3 scalar attributes, 1 list attribute with 3 values

    services.entityService
      .updateEntity(testWorkspaceName,
                    s1.entityType,
                    s1.name,
                    Seq(AddUpdateAttribute(AttributeName.withDefaultNS("bar"), AttributeNumber(99)))
      )
      .futureValue

    withClue("after updating an attribute value") {
      checkShardCounts(Map(testWorkspaceShardId -> 6)) // update should keep row counts the same
    }

    val updatedDbValue = runAndWait(
      uniqueResult(sql"select value_number from ENTITY_ATTRIBUTE_#$testWorkspaceShardId where name='bar'".as[Int])
    )

    withClue("when checking the updated attribute value") {
      updatedDbValue should contain(99)
    }
  }

  it should "not mix up workspace shards when inserting into multiple workspaces" in withTestDataServices { services =>
    // the empty workspace was created with a random uuid. find another uuid that does not have the same
    // shard identifier.
    var tempId: UUID = UUID.randomUUID()
    while (determineShard(tempId) == testWorkspaceShardId)
      tempId = UUID.randomUUID()
    val secondWorkspaceId = UUID.fromString(tempId.toString)
    val secondShardId = determineShard(secondWorkspaceId)
    secondShardId should not be testWorkspaceShardId
    val anotherWorkspace = Workspace("secondnamespace",
                                     "secondname",
                                     secondWorkspaceId.toString,
                                     "aBucket",
                                     Some("workflow-collection"),
                                     currentTime(),
                                     currentTime(),
                                     "testUser",
                                     Map.empty
    )

    val secondWorkspace = runAndWait(workspaceQuery.createOrUpdate(anotherWorkspace))

    val s2 = Entity("s2",
                    "samples",
                    Map(AttributeName.withDefaultNS("one") -> AttributeString("111"),
                        AttributeName.withDefaultNS("two") -> AttributeNumber(222)
                    )
    )

    // create entity in testWorkspace (entity s1 has 6 attribute rows)
    val created = services.entityService.createEntity(testWorkspaceName, s1).futureValue
    created shouldBe s1

    // create entity in secondWorkspace (entity s2 has 2 attribute rows)
    val created2 = services.entityService.createEntity(secondWorkspace.toWorkspaceName, s2).futureValue
    created2 shouldBe s2

    checkShardCounts(Map(testWorkspaceShardId -> 6, secondShardId -> 2))
  }

}
