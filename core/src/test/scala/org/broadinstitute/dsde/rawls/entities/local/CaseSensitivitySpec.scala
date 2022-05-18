package org.broadinstitute.dsde.rawls.entities.local

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity, EntityQuery, EntityTypeRename, RawlsUser, SortDirections, UserInfo, Workspace, WorkspaceFieldSpecs}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CaseSensitivitySpec extends AnyFreeSpec with Matchers with TestDriverComponent with ScalaFutures {

  // ===================================================================================================================
  // exemplar data used in multiple tests
  // ===================================================================================================================

  val exemplarTypes = Set("cat", "Cat", "CAT", "dog", "rat")
  val testWorkspace = new EmptyWorkspace

  // create three entities for each type in our list.
  val exemplarData = exemplarTypes.toSeq.zipWithIndex flatMap {
    case (typeName, index) =>
      Seq(
        Entity(s"001", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity(s"002", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity(s"003", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
      )
  }

  // ===================================================================================================================
  // tests
  // ===================================================================================================================

  "LocalEntityProvider case-sensitivity" - {
    "for entity types" - {
      "should return all types in uncached metadata requests" in withTestDataServices { _ =>
        // save exemplar data
        runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
        // get provider
        val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
        // get metadata
        val metadata = provider.entityTypeMetadata(false).futureValue
        metadata.keySet shouldBe exemplarTypes
      }

      "should return all types in cached metadata requests" is (pending)

      "should respect case for get-entity" in withTestDataServices { _ =>
        // save exemplar data
        runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

        // get provider
        val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
        // test gets
        exemplarData foreach { entityUnderTest =>
          val actual = provider.getEntity(entityUnderTest.entityType, entityUnderTest.name).futureValue
          actual shouldBe entityUnderTest
        }
      }

      "should respect case for update-entity" is (pending)

      "should respect case for expression evaluation" is (pending)

      exemplarTypes foreach { typeUnderTest =>
        s"should only rename target type [$typeUnderTest]" in withTestDataServices { services =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
          // rename type
          services.entityService.renameEntityType(testWorkspace.workspace.toWorkspaceName, typeUnderTest, EntityTypeRename("my-new-name")).futureValue
          // find actual type names from the db
          val actualTypes = getAllEntityTypes(testWorkspace.workspace)

          val expected = exemplarTypes - typeUnderTest + "my-new-name"
          actualTypes shouldBe expected
        }
      }

      exemplarTypes foreach { typeUnderTest =>
        s"should only delete-all target type [$typeUnderTest]" is pending
      }

      "should only delete all columns from target type" is (pending)

      "should respect case for delete specified entities" is (pending)

      "should only rename attribute within target type" is (pending)

      "creating an entity reference respects case" is (pending)

      exemplarTypes foreach { typeUnderTest =>
        s"batchUpsert respects case [$typeUnderTest]" in withTestDataServices { _ =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
          // get provider
          val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

          // create a batch upsert to change the target entity's attribute
          val op = AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("updated"))
          val upsert = EntityUpdateDefinition("001", typeUnderTest, Seq(op))
          // perform upsert
          provider.batchUpsertEntities(Seq(upsert)).futureValue

          // get actual entities, after upsert
          val entitiesAfterUpsert = getAllEntities(testWorkspace.workspace)
          // find the entity with the target name & type, after upsert
          // N.B. use this technique - get all entities, use Scala find() - to avoid any complications with the
          // provider.getEntity() method having bugs
          val foundOptAfterUpsert = entitiesAfterUpsert.find(e => e.name == "001" && e.entityType == typeUnderTest)
          foundOptAfterUpsert shouldNot be (empty)
          val foundAfterUpsert = foundOptAfterUpsert.get

          // after upsert, found entity should have an updated attribute
          val expected = Entity(foundAfterUpsert.name, typeUnderTest, Map(AttributeName.withDefaultNS("foo") -> AttributeString("updated")))
          foundAfterUpsert shouldBe expected
          // after upsert, all other entities should NOT have an updated attribute
          entitiesAfterUpsert foreach { e =>
            if (e.name != "001" && e.entityType != typeUnderTest) {
              e.attributes shouldBe Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))
            }
          }
        }
      }

      "batchUpdate respects case" is (pending)

      exemplarTypes foreach { typeUnderTest =>
        s"should only query target type [$typeUnderTest]" in withTestDataServices { _ =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
          // get provider
          val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
          // get results for one specific type
          val queryCriteria = EntityQuery(1, exemplarData.size, "name", SortDirections.Ascending, None, WorkspaceFieldSpecs(None))
          val queryResponse = provider.queryEntities(typeUnderTest, queryCriteria).futureValue
          // extract distinct entity types from results
          val typesFromResults = queryResponse.results.map(_.entityType).distinct
          typesFromResults.toSet shouldBe Set(typeUnderTest)
        }
      }

      exemplarTypes foreach { typeUnderTest =>
        s"should list all entities only for target type [$typeUnderTest]" in withTestDataServices { services =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

          val listAllResponse = services.entityService.listEntities(testWorkspace.workspace.toWorkspaceName, typeUnderTest, null).futureValue

          // extract distinct entity types from results
          val typesFromResults = listAllResponse.map(_.entityType).distinct
          typesFromResults.toSet shouldBe Set(typeUnderTest)
        }
      }

    }
  }

  // ===================================================================================================================
  // Test helpers and fixtures
  // ===================================================================================================================

  private def getAllEntities(workspace: Workspace): Seq[Entity] =
    runAndWait(entityQuery.listEntities(workspace)).iterator.toSeq

  private def getAllEntityTypes(workspace: Workspace): Set[String] = {
    val actualEntities = getAllEntities(workspace)
    actualEntities.map(_.entityType).toSet
  }

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends EntityApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val entityService: EntityService = entityServiceConstructor(userInfo1)

    def actorRefFactory = ActorSystem()
    val samDAO = new MockSamDAO(dataSource)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName = "test",
      EntityManager.defaultEntityManager(dataSource, new MockWorkspaceManagerDAO(), new MockDataRepoDAO("mockrepo"), samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10, 0), testConf.getBoolean("entityStatisticsCache.enabled"), "testMetricBaseName"),
      1000
    )_
  }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(testWorkspace) { dataSource: SlickDataSource =>
      withServices(dataSource, testWorkspace.userOwner)(testCode)
    }
  }


}


