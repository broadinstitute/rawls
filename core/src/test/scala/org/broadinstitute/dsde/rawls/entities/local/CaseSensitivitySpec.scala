package org.broadinstitute.dsde.rawls.entities.local

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeName, AttributeRename, AttributeString, Entity, EntityQuery, EntityTypeRename, FilterOperators, RawlsUser, SortDirections, UserInfo, Workspace, WorkspaceFieldSpecs}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}

import scala.concurrent.ExecutionContext

class CaseSensitivitySpec extends AnyFreeSpec with Matchers with TestDriverComponent with ScalaFutures {

  // ===================================================================================================================
  // exemplar data used in multiple tests
  // ===================================================================================================================

  val exemplarTypes = Set("cat", "Cat", "CAT", "dog", "rat")
  val testWorkspace = new EmptyWorkspace
  val fooAttribute = AttributeName.withDefaultNS("foo")

  // create three entities for each type in our list, using unique names for each entity.
  val exemplarData = exemplarTypes.toSeq.zipWithIndex flatMap {
    case (typeName, index) =>
      Seq(
        Entity(s"$typeName-$index-001", typeName, Map(fooAttribute -> AttributeString(s"$typeName-001"))),
        Entity(s"$typeName-$index-002", typeName, Map(fooAttribute -> AttributeString(s"$typeName-002"))),
        Entity(s"$typeName-$index-003", typeName, Map(fooAttribute -> AttributeString(s"$typeName-003")))
      )
  }

  // create three entities for each type in our list, reusing names across entity types.
  val exemplarDataWithCommonNames = exemplarTypes flatMap { typeName =>
    Seq(
      Entity(s"001", typeName, Map(fooAttribute -> AttributeString(s"$typeName-001"))),
      Entity(s"002", typeName, Map(fooAttribute -> AttributeString(s"$typeName-002"))),
      Entity(s"003", typeName, Map(fooAttribute -> AttributeString(s"$typeName-003")))
    )
  }

  // ===================================================================================================================
  // tests
  // ===================================================================================================================

  "LocalEntityProvider case-sensitivity" - {
    "for entity types" - {
      "features that depend on entity type case sensitivity only" - {

        "should return all types in uncached metadata requests" in withTestDataServices { _ =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
          // get provider
          val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
          // get metadata
          val metadata = provider.entityTypeMetadata(false).futureValue
          metadata.keySet shouldBe exemplarTypes
        }

        "should return all types in cached metadata requests" in withTestDataServices { services =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

          // assert cache is not yet populated
          runAndWait(entityCacheQuery.entityCacheStaleness(testWorkspace.workspace.workspaceIdAsUUID)) shouldBe empty

          // get metadata
          val metadata = services.entityService.entityTypeMetadata(testWorkspace.wsName, None, None, useCache = true).futureValue
          metadata.keySet shouldBe exemplarTypes

          // assert cache is populated and up-to-date
          runAndWait(entityCacheQuery.entityCacheStaleness(testWorkspace.workspace.workspaceIdAsUUID)) should contain (0)

          // get types from cache and verify
          val cachedTypes = runAndWait(entityTypeStatisticsQuery.getAll(testWorkspace.workspace.workspaceIdAsUUID))
          cachedTypes.keySet shouldBe exemplarTypes
          // get metadata again, should come from cache, and verify
          val cachedMetadata = services.entityService.entityTypeMetadata(testWorkspace.wsName, None, None, useCache = true).futureValue
          cachedMetadata.keySet shouldBe exemplarTypes

          runAndWait(entityCacheQuery.entityCacheStaleness(testWorkspace.workspace.workspaceIdAsUUID)) should contain (0)
        }

        exemplarTypes foreach { typeUnderTest =>
          // generate the new name for this type by flipping case of the last letter
          val lastChar = typeUnderTest.reverse.head
          val newLastChar = if (lastChar.isLower) {
            lastChar.toUpper
          } else {
            lastChar.toLower
          }
          val newName = (newLastChar + typeUnderTest.reverse.tail).reverse

          s"should only rename target type [$typeUnderTest] -> [$newName]" in withTestDataServices { services =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

            // assert we are renaming to a new name that differs only in case
            newName should not be typeUnderTest
            newName.toLowerCase shouldBe typeUnderTest.toLowerCase

            // rename type
            services.entityService.renameEntityType(testWorkspace.workspace.toWorkspaceName, typeUnderTest, EntityTypeRename(newName)).futureValue
            // find actual type names from the db
            val actualTypes = getAllEntityTypes(testWorkspace.workspace)

            val expected = exemplarTypes - typeUnderTest + newName
            actualTypes shouldBe expected
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should only delete all entities of target type [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

            // delete all entities from target type
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
            provider.deleteEntitiesOfType(typeUnderTest).futureValue

            // get actual entity types from the db
            val actualEntityTypes = getAllEntityTypes(testWorkspace.workspace)

            actualEntityTypes shouldBe exemplarTypes - typeUnderTest
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case when deleting all named attributes from target type [$typeUnderTest]" in withTestDataServices { services =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

            // delete all attributes named "foo" from the target type
            services.entityService.deleteEntityAttributes(testWorkspace.workspace.toWorkspaceName, typeUnderTest, Set(fooAttribute)).futureValue
            // get actual entities from the db
            val actualEntities = getAllEntities(testWorkspace.workspace)

            // loop through all entities. If the entity is of the target type, it should have no "foo" attribute.
            // if it is NOT of the target type, it SHOULD have a "foo" attribute.
            actualEntities foreach { e =>
              if (e.entityType == typeUnderTest) {
                e.attributes shouldBe empty
              } else {
                e.attributes.keySet shouldBe Set(fooAttribute)
              }
            }
          }
        }
        exemplarTypes foreach { typeUnderTest =>
          s"should only rename an entire column within target type [$typeUnderTest]" in withTestDataServices { services =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))

            // rename attribute from target type
            services.entityService.renameAttribute(testWorkspace.workspace.toWorkspaceName, typeUnderTest, fooAttribute,
              AttributeRename(AttributeName.withDefaultNS("new-attr-name"))).futureValue
            // get actual entities from the db
            val actualEntities = getAllEntities(testWorkspace.workspace)

            actualEntities foreach { e =>
              if (e.entityType == typeUnderTest) {
                e.attributes.keySet shouldBe Set(AttributeName.withDefaultNS("new-attr-name"))
              } else {
                e.attributes.keySet shouldBe Set(fooAttribute)
              }
            }
          }
        }
        exemplarTypes foreach { typeUnderTest =>
          s"should only query target type [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarData))
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
            // get results for one specific type
            val queryCriteria = EntityQuery(1, exemplarData.size, "name", SortDirections.Ascending, None, FilterOperators.And, WorkspaceFieldSpecs(None))
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

      "features that depend on entity type + entity name case sensitivity" - {

        "should respect case for get-entity" in withTestDataServices { _ =>
          // save exemplar data
          runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))

          // get provider
          val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")
          // test gets
          exemplarDataWithCommonNames foreach { entityUnderTest =>
            val actual = provider.getEntity(entityUnderTest.entityType, entityUnderTest.name).futureValue
            actual shouldBe entityUnderTest
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case for expression evaluation type names [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))

            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // set up arguments for expression evaluation
            val expressionEvaluationContext = ExpressionEvaluationContext(Option(typeUnderTest), Option("002"), None, Option(typeUnderTest))

            val toolInputParameter = new ToolInputParameter().name("my-input-name").valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING))
            val processableInputs = Set(MethodInput(toolInputParameter, "this.foo"))
            val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

            val submissionValidationEntityInputsList = provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()).futureValue.toList
            submissionValidationEntityInputsList.size shouldBe 1

            val entityInputs = submissionValidationEntityInputsList.head
            entityInputs.entityName shouldBe "002"
            entityInputs.inputResolutions.size shouldBe 1
            entityInputs.inputResolutions.head.error shouldBe empty
            entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"
            entityInputs.inputResolutions.head.value should contain (AttributeString(s"$typeUnderTest-002"))
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should resolve type + _id expressions correctly [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))

            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // set up arguments for expression evaluation, using "this.${typeUnderTest}_id"
            val expressionString = s"this.${typeUnderTest}_id"
            val expressionEvaluationContext = ExpressionEvaluationContext(Option(typeUnderTest), Option("002"), None, Option(typeUnderTest))
            val toolInputParameter = new ToolInputParameter().name("my-input-name").valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING))
            val processableInputs = Set(MethodInput(toolInputParameter, expressionString))
            val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

            // evaluate expression
            val submissionValidationEntityInputsList = provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()).futureValue.toList
            submissionValidationEntityInputsList.size shouldBe 1

            // verify expression resolution
            val entityInputs = submissionValidationEntityInputsList.head
            entityInputs.entityName shouldBe "002"
            entityInputs.inputResolutions.size shouldBe 1
            entityInputs.inputResolutions.head.error shouldBe empty
            entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"
            entityInputs.inputResolutions.head.value should contain (AttributeString(s"002"))
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should resolve type + _id expressions to an empty result if the type is incorrectly cased [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))

            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // set up arguments for expression evaluation, using incorrect case for "this.${typeUnderTest}_id"
            val expressionString = s"this.${typeUnderTest.head.toLower}${typeUnderTest.tail.toUpperCase}_id"
            val expressionEvaluationContext = ExpressionEvaluationContext(Option(typeUnderTest), Option("002"), None, Option(typeUnderTest))
            val toolInputParameter = new ToolInputParameter().name("my-input-name").valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING))
            val processableInputs = Set(MethodInput(toolInputParameter, expressionString))
            val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

            // evaluate expression
            val submissionValidationEntityInputsList = provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()).futureValue.toList
            submissionValidationEntityInputsList.size shouldBe 1

            // verify expression resolution
            val entityInputs = submissionValidationEntityInputsList.head
            entityInputs.entityName shouldBe "002"
            entityInputs.inputResolutions.size shouldBe 1
            entityInputs.inputResolutions.head.error should contain ("Expected single value for workflow input, but evaluated result set was empty")
            entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"
            entityInputs.inputResolutions.head.value shouldBe empty
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case for delete specified entities [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // delete two entities of target type
            val entRefs = Seq(
              AttributeEntityReference(typeUnderTest, "001"),
              AttributeEntityReference(typeUnderTest, "002"))
            provider.deleteEntities(entRefs).futureValue

            // count actual entities by type
            val actualEntities = getAllEntities(testWorkspace.workspace)
            val typesToCounts: Map[String, Int] = actualEntities.groupBy(_.entityType).map {
              case (typeName, entities) => (typeName, entities.size)
            }
            typesToCounts(typeUnderTest) shouldBe 1
            exemplarTypes-typeUnderTest foreach { otherType =>
              typesToCounts(otherType) shouldBe 3
            }
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case for rename specified entity [$typeUnderTest]" in withTestDataServices { services =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))

            // rename entity of target type
            services.entityService.renameEntity(testWorkspace.workspace.toWorkspaceName, typeUnderTest, "003", "my-new-name").futureValue

            // get actual entities
            val actualEntities = getAllEntities(testWorkspace.workspace)

            actualEntities.count(_.name == "my-new-name") shouldBe 1
            actualEntities.count(_.name == "001") shouldBe exemplarTypes.size
            actualEntities.count(_.name == "002") shouldBe exemplarTypes.size
            actualEntities.count(_.name == "003") shouldBe exemplarTypes.size - 1
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case when creating an entity reference [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // create a batch upsert to add an entity reference from 001 to 003
            val op = AddUpdateAttribute(AttributeName.withDefaultNS("my-entity-reference"), AttributeEntityReference(typeUnderTest, "003"))
            val upsert = EntityUpdateDefinition("001", typeUnderTest, Seq(op))
            // perform upsert
            provider.batchUpsertEntities(Seq(upsert)).futureValue


            // get database-level entity record for the entity containing the reference
            val entityRecordContainingReference = runAndWait(entityQuery.getEntityRecords(testWorkspace.workspace.workspaceIdAsUUID,
              Set(AttributeEntityReference(typeUnderTest, "001")))).head

            // get database-level entity record for the entity being referenced
            val entityRecordBeingReferenced = runAndWait(entityQuery.getEntityRecords(testWorkspace.workspace.workspaceIdAsUUID,
              Set(AttributeEntityReference(typeUnderTest, "003")))).head

            // get database-level attribute record for the reference and find the entity id it is referencing
            import driver.api._
            val actualReferencedIds = runAndWait(entityAttributeShardQuery(testWorkspace.workspace.workspaceIdAsUUID)
              .findByOwnerQuery(Seq(entityRecordContainingReference.id))
              .filter(_.name === "my-entity-reference")
              .map(attr => attr.valueEntityRef)
              .result)

            // we should have exactly one reference, and it should point to entityRecordBeingReferenced
            actualReferencedIds.size shouldBe 1
            actualReferencedIds.head shouldNot be (empty)
            actualReferencedIds.head.get shouldBe entityRecordBeingReferenced.id
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case during batchUpsert [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // create a batch upsert to change the target entity's attribute
            val op = AddUpdateAttribute(fooAttribute, AttributeString("updated"))
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
                val actualFoo = e.attributes.get(fooAttribute)
                actualFoo shouldNot be (empty)
                actualFoo shouldNot contain (AttributeString("updated"))
              }
            }
          }
        }

        exemplarTypes foreach { typeUnderTest =>
          s"should respect case during batchUpdate [$typeUnderTest]" in withTestDataServices { _ =>
            // save exemplar data
            runAndWait(entityQuery.save(testWorkspace.workspace, exemplarDataWithCommonNames))
            // get provider
            val provider = new LocalEntityProvider(testWorkspace.workspace, slickDataSource, false, "metricsBaseName")

            // create a batch upsert to change the target entity's attribute
            val op = AddUpdateAttribute(fooAttribute, AttributeString("updated"))
            val upsert = EntityUpdateDefinition("001", typeUnderTest, Seq(op))
            // perform upsert
            provider.batchUpdateEntities(Seq(upsert)).futureValue

            // get actual entities, after upsert
            val entitiesAfterUpdate = getAllEntities(testWorkspace.workspace)
            // find the entity with the target name & type, after upsert
            // N.B. use this technique - get all entities, use Scala find() - to avoid any complications with the
            // provider.getEntity() method having bugs
            val foundOptAfterUpdate = entitiesAfterUpdate.find(e => e.name == "001" && e.entityType == typeUnderTest)
            foundOptAfterUpdate shouldNot be (empty)
            val foundAfterUpdate = foundOptAfterUpdate.get

            // after upsert, found entity should have an updated attribute
            val expected = Entity(foundAfterUpdate.name, typeUnderTest, Map(AttributeName.withDefaultNS("foo") -> AttributeString("updated")))
            foundAfterUpdate shouldBe expected
            // after upsert, all other entities should NOT have an updated attribute
            entitiesAfterUpdate foreach { e =>
              if (e.name != "001" && e.entityType != typeUnderTest) {
                val actualFoo = e.attributes.get(fooAttribute)
                actualFoo shouldNot be (empty)
                actualFoo shouldNot contain (AttributeString("updated"))
              }
            }
          }
        }

      }
    }
  }

  // ===================================================================================================================
  // Test helpers and fixtures
  // ===================================================================================================================

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(500, Millis)), interval = scaled(Span(15, Millis)))

  private def getAllEntities(workspace: Workspace): Seq[Entity] =
    runAndWait(entityQuery.listActiveEntities(workspace)).iterator.toSeq

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


