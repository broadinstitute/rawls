package org.broadinstitute.dsde.rawls.dataaccess.slick

import _root_.slick.dbio.DBIOAction
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.rawls.model.{AttributeName, _}
import org.broadinstitute.dsde.rawls.{model, RawlsException, RawlsTestUtils}

import java.sql.SQLException
import java.util.UUID

/**
 * Created by dvoet on 2/12/16.
 */
class EntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils with RawSqlQuery {
  import driver.api._

  // entity and attribute counts, regardless of deleted status
  def countEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listEntities(workspace))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  // entity and attribute counts, non-deleted only
  def countActiveEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listActiveEntities(workspace))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  "EntityComponent" should "crud entities" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace("test_namespace",
                                         workspaceId.toString,
                                         workspaceId.toString,
                                         "bucketname",
                                         Some("workflow-collection"),
                                         currentTime(),
                                         currentTime(),
                                         "me",
                                         Map.empty,
                                         false
    )
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    val workspaceContext = workspace

    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val entity = Entity("name", "type", Map.empty)

    assertResult(entity)(runAndWait(entityQuery.save(workspaceContext, entity)))
    assertResult(Some(entity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val target1 = Entity("target1", "type", Map.empty)
    runAndWait(entityQuery.save(workspaceContext, target1))
    val target2 = Entity("target2", "type", Map.empty)
    runAndWait(entityQuery.save(workspaceContext, target2))

    val updatedEntity = entity.copy(attributes =
      Map(
        AttributeName.withDefaultNS("string") -> AttributeString("foo"),
        AttributeName.withDefaultNS("ref") -> target1.toReference,
        AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
          Seq(target1.toReference, target2.toReference)
        )
      )
    )

    assertResult(updatedEntity)(runAndWait(entityQuery.save(workspaceContext, updatedEntity)))
    assertResult(Some(updatedEntity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val updatedAgainEntity = updatedEntity.copy(attributes =
      Map(
        AttributeName.withDefaultNS("string2") -> AttributeString("foo"),
        AttributeName.withDefaultNS("ref") -> target2.toReference,
        AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
          Seq(target2.toReference, target1.toReference)
        )
      )
    )
    assertResult(updatedAgainEntity)(runAndWait(entityQuery.save(workspaceContext, updatedAgainEntity)))
    assertResult(Some(updatedAgainEntity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    assertResult(entity)(runAndWait(entityQuery.save(workspaceContext, entity)))
    assertResult(Some(entity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    // save AttributeValueEmptyList
    val emptyValListAttributeEntity =
      entity.copy(name = "emptyValListy",
                  attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeValueEmptyList)
      )
    runAndWait(entityQuery.save(workspaceContext, emptyValListAttributeEntity))
    assertResult(Some(emptyValListAttributeEntity)) {
      runAndWait(entityQuery.get(workspaceContext, "type", "emptyValListy"))
    }

    // convert AttributeValueList(Seq()) -> AttributeEmptyList
    val emptyValListEntity = entity.copy(name = "emptyValList",
                                         attributes =
                                           Map(AttributeName.withDefaultNS("emptyList") -> AttributeValueList(Seq()))
    )
    runAndWait(entityQuery.save(workspaceContext, emptyValListEntity))
    assertResult(Some(emptyValListAttributeEntity.copy(name = "emptyValList"))) {
      runAndWait(entityQuery.get(workspaceContext, "type", "emptyValList"))
    }

    // save AttributeEntityReferenceEmptyList
    val emptyRefListAttributeEntity =
      entity.copy(name = "emptyRefListy",
                  attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeEntityReferenceEmptyList)
      )
    runAndWait(entityQuery.save(workspaceContext, emptyRefListAttributeEntity))
    assertResult(Some(emptyRefListAttributeEntity)) {
      runAndWait(entityQuery.get(workspaceContext, "type", "emptyRefListy"))
    }

    // convert AttributeEntityReferenceList(Seq()) -> AttributeEntityReferenceEmptyList
    val emptyRefListEntity =
      entity.copy(name = "emptyRefList",
                  attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeEntityReferenceList(Seq()))
      )
    runAndWait(entityQuery.save(workspaceContext, emptyRefListEntity))
    assertResult(Some(emptyRefListAttributeEntity.copy(name = "emptyRefList"))) {
      runAndWait(entityQuery.get(workspaceContext, "type", "emptyRefList"))
    }

    val (entityCount1, attributeCount1) = countEntitiesAttrs(workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(workspace)

    // "hide" deletion

    assertResult(1)(runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))))
    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "name")))
    assertResult(0)(runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))))

    val (entityCount2, attributeCount2) = countEntitiesAttrs(workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(workspace)

    assertResult(entityCount1)(entityCount2)
    assertResult(attributeCount1)(attributeCount2)
    assertResult(activeEntityCount1 - 1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)

    // actual deletion

    val entityForDeletion = Entity("delete-me", "type", Map.empty)

    assertResult(entityForDeletion)(runAndWait(entityQuery.save(workspaceContext, entityForDeletion)))
    assertResult(Some(entityForDeletion))(runAndWait(entityQuery.get(workspaceContext, "type", "delete-me")))

    val (entityCount3, attributeCount3) = countEntitiesAttrs(workspace)
    val (activeEntityCount3, activeAttributeCount3) = countActiveEntitiesAttrs(workspace)

    assertResult(entityCount2 + 1)(entityCount3)
    assertResult(attributeCount2)(attributeCount3)
    assertResult(activeEntityCount2 + 1)(activeEntityCount3)
    assertResult(activeAttributeCount2)(activeAttributeCount3)

    assertResult(entityCount3)(runAndWait(entityQuery.deleteFromDb(workspaceContext)))
    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "delete-me")))
    assertResult(0)(runAndWait(entityQuery.deleteFromDb(workspaceContext)))

    val (entityCount4, attributeCount4) = countEntitiesAttrs(workspace)
    val (activeEntityCount4, activeAttributeCount4) = countActiveEntitiesAttrs(workspace)

    assertResult(0)(entityCount4)
    assertResult(0)(attributeCount4)
    assertResult(0)(activeEntityCount4)
    assertResult(0)(activeAttributeCount4)
  }

  it should "fail to saveEntityPatch for a nonexistent entity" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      val caught = intercept[RawlsException] {
        runAndWait(
          entityQuery.saveEntityPatch(
            context,
            AttributeEntityReference("Sample", "nonexistent"),
            Map(AttributeName.withDefaultNS("newAttribute") -> AttributeNumber(2)),
            Seq(AttributeName.withDefaultNS("type"))
          )
        )
      }
      // make sure we get the _right_ RawlsException:
      // "saveEntityPatch looked up $entityRef expecting 1 record, got 0 instead"
      caught.getMessage should include("expecting")
    }
  }

  it should "fail to saveEntityPatch if you try to delete and upsert the same attribute" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      val caught = intercept[RawlsException] {
        runAndWait(
          entityQuery.saveEntityPatch(
            context,
            AttributeEntityReference("Sample", "sample1"),
            Map(AttributeName.withDefaultNS("type") -> AttributeNumber(2)),
            Seq(AttributeName.withDefaultNS("type"))
          )
        )
      }
      // make sure we get the _right_ RawlsException:
      // "Can't saveEntityPatch on $entityRef because upserts and deletes share attributes <blah>"
      caught.getMessage should include("share")
    }
  }

  it should "return false if the entity type does not exist" in withMinimalTestDatabase { _ =>
    withWorkspaceContext(testData.workspace) { context =>
      val pair2EntityTypeExists = runAndWait(entityQuery.doesEntityTypeAlreadyExist(context, "Pair2")).get
      assert(!pair2EntityTypeExists)
    }
  }

  it should "change entity type name" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val rowsUpdated = runAndWait(entityQuery.changeEntityTypeName(context, "Pair", "Pair2"))
      assert(rowsUpdated == 2)
      val pair2EntityTypeExistsAfterUpdate = runAndWait(entityQuery.doesEntityTypeAlreadyExist(context, "Pair2")).get
      assert(pair2EntityTypeExistsAfterUpdate)

    }
  }

  it should "saveEntityPatch" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val inserts = Map(
        AttributeName.withDefaultNS("totallyNew") -> AttributeNumber(2),
        AttributeName.withDefaultNS("quot2") -> testData.aliquot2.toReference
      )
      val updates = Map(
        AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("c"), AttributeString("d"))),
        AttributeName.withDefaultNS("quot1") -> testData.aliquot2.toReference // jerk move
      )
      val deletes = Seq(AttributeName.withDefaultNS("whatsit"), AttributeName.withDefaultNS("nonexistent"))

      val expected = testData.sample1.attributes ++ inserts ++ updates -- deletes

      runAndWait {
        entityQuery.saveEntityPatch(context, AttributeEntityReference("Sample", "sample1"), inserts ++ updates, deletes)
      }

      assertSameElements(
        expected,
        runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes
      )

      // check that the all_attribute_values field was filled in correctly by searching on the new attributes and making sure one filtered result is found
      assertResult(1) {
        runAndWait(
          entityQuery.loadEntityPage(context,
                                     "Sample",
                                     model.EntityQuery(1,
                                                       10,
                                                       "name",
                                                       SortDirections.Ascending,
                                                       Option("sample1 2 tumor aliquot2 aliquot1 aliquot2 itsfoo")
                                     ),
                                     testContext
          )
        )._2
      }
    }
  }

  it should "update attribute list values" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      // insert new attribute 'myNewList' which is a list with 3 elements
      val inserts = Map(
        AttributeName.withDefaultNS("myNewList") -> AttributeValueList(
          Seq(
            AttributeString("abc"),
            AttributeString("def"),
            AttributeString("xyz")
          )
        )
      )

      val expectedAfterInsertion = testData.sample1.attributes ++ inserts

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    inserts,
                                    Seq.empty[AttributeName]
        )
      )

      assertSameElements(expectedAfterInsertion,
                         runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes
      )

      // update values in the list
      val updates = Map(
        AttributeName.withDefaultNS("myNewList") -> AttributeValueList(
          Seq(
            AttributeString("123"),
            AttributeString("456"),
            AttributeString("789")
          )
        )
      )

      val expectedAfterUpdate = testData.sample1.attributes ++ updates

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    updates,
                                    Seq.empty[AttributeName]
        )
      )

      // check that the 'myNewList' attribute has updated values
      assertSameElements(expectedAfterUpdate, runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes)
    }
  }

  it should "reflect changes in attribute when attribute value list size increases" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      // insert new attribute 'newEntityList' which is a list with 1 element
      val inserts =
        Map(AttributeName.withDefaultNS("newEntityList") -> AttributeValueList(Seq(AttributeString("abc1"))))

      val expectedAfterInsertion = testData.sample1.attributes ++ inserts

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    inserts,
                                    Seq.empty[AttributeName]
        )
      )

      assertSameElements(expectedAfterInsertion,
                         runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes
      )

      // update 'newEntityList' to contain 4 elements
      val updates = Map(
        AttributeName.withDefaultNS("newEntityList") -> AttributeValueList(
          Seq(
            AttributeString("abc2"),
            AttributeString("def"),
            AttributeString("abc12"),
            AttributeString("xyz")
          )
        )
      )

      val expectedAfterUpdate = testData.sample1.attributes ++ updates

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    updates,
                                    Seq.empty[AttributeName]
        )
      )

      // check that the 'newEntityList' attribute has 4 values now
      assertSameElements(expectedAfterUpdate, runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes)
    }
  }

  it should "reflect changes in attribute when attribute value list size decreases" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      // insert new attribute 'anotherList' which is a list with 3 elements
      val inserts = Map(
        AttributeName.withDefaultNS("anotherList") -> AttributeValueList(
          Seq(
            AttributeString("abc1"),
            AttributeString("abc2"),
            AttributeString("abc3")
          )
        )
      )

      val expectedAfterInsertion = testData.sample1.attributes ++ inserts

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    inserts,
                                    Seq.empty[AttributeName]
        )
      )

      assertSameElements(expectedAfterInsertion,
                         runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes
      )

      // update 'anotherList' to contain 1 element
      val updates = Map(AttributeName.withDefaultNS("anotherList") -> AttributeValueList(Seq(AttributeString("1234"))))

      val expectedAfterUpdate = testData.sample1.attributes ++ updates

      runAndWait(
        entityQuery.saveEntityPatch(context,
                                    AttributeEntityReference("Sample", "sample1"),
                                    updates,
                                    Seq.empty[AttributeName]
        )
      )

      // check that the 'anotherList' attribute has 1 value now
      assertSameElements(expectedAfterUpdate, runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes)
    }
  }

  it should "list all entities of all entity types" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      assertSameElements(constantData.allEntities, runAndWait(entityQuery.listActiveEntities(context)))
    }
  }

  it should "list all entity types with their counts" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      assertResult(
        Map("PairSet" -> 1, "Individual" -> 2, "Sample" -> 8, "Aliquot" -> 2, "SampleSet" -> 5, "Pair" -> 2)
      ) {
        runAndWait(entityQuery.getEntityTypesWithCounts(context.workspaceIdAsUUID))
      }
    }
  }

  it should "skip deleted entities when listing all entity types with their counts" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val deleteSamples = entityQuery.findActiveEntityByType(context.workspaceIdAsUUID, "Sample").result flatMap {
        entityRecs =>
          val deleteActions = entityRecs map { rec => entityQuery.hide(context, Seq(rec.toReference)) }
          DBIO.seq(deleteActions: _*)
      }
      runAndWait(deleteSamples)

      assertResult(Map("PairSet" -> 1, "Individual" -> 2, "Aliquot" -> 2, "SampleSet" -> 5, "Pair" -> 2)) {
        runAndWait(entityQuery.getEntityTypesWithCounts(context.workspaceIdAsUUID))
      }
    }
  }

  it should "list all entity types with their attribute names" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      val desiredTypesAndAttrNames = Map(
        "Sample" -> Seq("type", "whatsit", "thingies", "quot", "somefoo", "tumortype", "confused", "cycle", "foo_id"),
        // "Aliquot" -> Seq(), NOTE: this is commented out because the db query doesn't return types that have no attributes.
        "Pair" -> Seq("case", "control", "whatsit"),
        "SampleSet" -> Seq("samples", "hasSamples"),
        "PairSet" -> Seq("pairs"),
        "Individual" -> Seq("sset")
      )

      // assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
      // so we test the existence of all keys correctly here...
      val testTypesAndAttrNames = runAndWait(entityQuery.getAttrNamesAndEntityTypes(context.workspaceIdAsUUID))
      assertSameElements(testTypesAndAttrNames.keys, desiredTypesAndAttrNames.keys)

      desiredTypesAndAttrNames foreach { case (eType, attrNames) =>
        // ...and handle that the values are all correct here.
        assertSameElements(testTypesAndAttrNames(eType).map(AttributeName.toDelimitedName), attrNames)
      }
    }
  }

  it should "list all entity types with their namespaced attribute names" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace("test_namespace",
                                         workspaceId.toString,
                                         workspaceId.toString,
                                         "bucketname",
                                         Some("workflow-collection"),
                                         currentTime(),
                                         currentTime(),
                                         "me",
                                         Map.empty,
                                         false
    )
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    val workspaceContext = workspace

    // this entity also tests that namespaced and default attributes of the same name are tracked separately
    val entity1 = Entity(
      "entity1",
      "type1",
      Map(
        AttributeName.fromDelimitedName("tag:hello") -> AttributeString("world"),
        AttributeName.fromDelimitedName("tag:ihaveanamespace") -> AttributeString("foo"),
        AttributeName.fromDelimitedName("tag:ialsohaveanamespace") -> AttributeString("bar"),
        AttributeName.fromDelimitedName("iamdefault") -> AttributeString("baz"),
        AttributeName.fromDelimitedName("hello") -> AttributeString("moon")
      )
    )

    val entity2 = Entity(
      "entity2",
      "type2",
      Map(
        AttributeName.fromDelimitedName("tag:morenamespacing") -> AttributeNumber(1),
        AttributeName.fromDelimitedName("tag:evenmorenamespacing") -> AttributeNumber(2),
        AttributeName.fromDelimitedName("iamdefault") -> AttributeNumber(3),
        AttributeName.fromDelimitedName("moredefault") -> AttributeNumber(4)
      )
    )

    runAndWait(entityQuery.save(workspaceContext, entity1))
    runAndWait(entityQuery.save(workspaceContext, entity2))

    val desiredTypesAndAttrNames = Map(
      "type1" -> Seq("tag:hello", "tag:ihaveanamespace", "tag:ialsohaveanamespace", "iamdefault", "hello"),
      "type2" -> Seq("tag:morenamespacing", "tag:evenmorenamespacing", "iamdefault", "moredefault")
    )

    // assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
    // so we test the existence of all keys correctly here...
    val testTypesAndAttrNames = runAndWait(entityQuery.getAttrNamesAndEntityTypes(workspaceContext.workspaceIdAsUUID))
    assertSameElements(testTypesAndAttrNames.keys, desiredTypesAndAttrNames.keys)

    desiredTypesAndAttrNames foreach { case (eType, attrNames) =>
      // ...and handle that the values are all correct here.
      assertSameElements(testTypesAndAttrNames(eType).map(AttributeName.toDelimitedName), attrNames)
    }
  }

  // WARNING: I have fears that this unit test will be flaky, because it depends on timing.
  // if this test runs on a too-powerful computer, the SQL query that should time out
  // may run too fast, not time out, and the test will fail. If it turns out to be flaky,
  // let's just delete this test since it is 90% testing Slick's timeout feature, and 10%
  // testing that Rawls code is set up properly to pass a timeout argument to Slick.
  it should "time out when listing all entity types with their attribute names, if a timeout is specified" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      // insert a whole lot of entities with a lot of unique attribute names
      val numEntities = 1000
      val numAttrsPerEntity = 400
      (1 to numEntities) foreach { entityIdx =>
        val attrs = (1 to numAttrsPerEntity) map { _ =>
          val attrName = RandomStringUtils.randomAlphanumeric(12)
          AttributeName.withDefaultNS(attrName) -> AttributeString(attrName)
        }
        val entity = Entity(s"entity$entityIdx", "unitTestType", attrs.toMap)
        runAndWait(entityQuery.save(testData.workspace, entity))
      }

      // now attempt to calculate attr names and types, with a timeout of 1 second
      withClue("This test is potentially flaky, failures should be reviewed: ") {
        intercept[java.sql.SQLException] {
          runAndWait(entityQuery.getAttrNamesAndEntityTypes(context.workspaceIdAsUUID, 1))
        }
      }
    }
  }

  val testWorkspace = new EmptyWorkspace

  it should "trim giant all_attribute_values strings so they don't overflow" in withCustomTestDatabase(testWorkspace) {
    dataSource =>
      // it'll be longer than this (and thus will need trimming) because it'll get the entity name too
      val veryLongString = "a" * EntityComponent.allAttributeValuesColumnSize
      val sample1 = Entity("sample1",
                           "Sample",
                           Map(AttributeName.withDefaultNS("veryLongString") -> AttributeString(veryLongString))
      )
      withWorkspaceContext(testWorkspace.workspace) { context =>
        runAndWait(entityQuery.save(context, sample1))

        val entityRec = runAndWait(
          uniqueResult(
            entityQueryWithInlineAttributes
              .findEntityByName(UUID.fromString(testWorkspace.workspace.workspaceId), "Sample", "sample1")
              .result
          )
        )
        assertResult(EntityComponent.allAttributeValuesColumnSize) {
          entityRec.get.allAttributeValues.get.length
        }
      }
  }

  class BugTestData extends TestData {
    val wsName = WorkspaceName("myNamespace2", "myWorkspace2")
    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID.toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    val sample1 = new Entity("sample1",
                             "Sample",
                             Map(
                               AttributeName.withDefaultNS("aliquot") -> AttributeEntityReference("Aliquot", "aliquot1")
                             )
    )

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)

    override def save() =
      DBIOAction.seq(workspaceQuery.createOrUpdate(workspace),
                     entityQuery.save(workspace, aliquot1),
                     entityQuery.save(workspace, sample1)
      )

  }

  val bugData = new BugTestData

  it should "get an entity with attribute ref name same as an entity, but different case" in withCustomTestDatabaseInternal(
    bugData
  ) {

    withWorkspaceContext(bugData.workspace) { context =>
      assertResult(Some(bugData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
    }

  }

  it should "get an entity" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      assertResult(Some(testData.pair1)) {
        runAndWait(entityQuery.get(context, "Pair", "pair1"))
      }
      assertResult(Some(testData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(testData.sset1)) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
    }

  }

  it should "return None when an entity does not exist" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      assertResult(None) {
        runAndWait(entityQuery.get(context, "pair", "fnord"))
      }
      assertResult(None) {
        runAndWait(entityQuery.get(context, "fnord", "pair1"))
      }
    }

  }

  it should "save a new entity" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      val pair2 = Entity(
        "pair2",
        "Pair",
        Map(
          AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
          AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")
        )
      )
      runAndWait(entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(testData.workspace, "Pair", "pair2")).isDefined
      }
    }

  }

  it should "update a workspace's lastModified date when saving an entity" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      // get the workspace prior to saving the entity
      val workspaceBefore = runAndWait(workspaceQuery.findById(context.workspaceId))
        .getOrElse(fail(s"could not retrieve workspace ${context.workspaceId} before saving entity"))

      // save the entity, assert it saved correctly
      val pair2 = Entity(
        "pair2",
        "Pair",
        Map(
          AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
          AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")
        )
      )
      runAndWait(entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(testData.workspace, "Pair", "pair2")).isDefined
      }

      // get the workspace after to saving the entity
      val workspaceAfter = runAndWait(workspaceQuery.findById(context.workspaceId))
        .getOrElse(fail(s"could not retrieve workspace ${context.workspaceId} after saving entity"))

      assert(
        workspaceAfter.lastModified.isAfter(workspaceBefore.lastModified),
        s"workspace lastModified of ${workspaceAfter.lastModified} should be after lastModified of ${workspaceBefore.lastModified}, " +
          s"since we saved an entity to that workspace."
      )
    }

  }

  it should "not re-update an entity's attributes over many writes if attribute do not change" in withDefaultTestDatabase {
    val pair2 = Entity(
      "pair2",
      "Pair",
      Map(
        AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")
      )
    )

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(testData.workspace, "Pair", "pair2")).isDefined
      }
    }

    withWorkspaceContext(testData.workspace) { context =>
      val count = 20
      runMultipleAndWait(count)(_ => entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(testData.workspace, "Pair", "pair2")).isDefined
      }
      assertResult(0) { // the additional writes should not increment this entity's version
        runAndWait(
          entityQuery.findEntityByName(testData.workspace.workspaceIdAsUUID, "Pair", "pair2").map(_.version).result
        ).head
      }
    }
  }

  it should "update an entity's attributes many times concurrently if attributes change" in withDefaultTestDatabase {
    def makeEntity(idx: Int): Entity =
      Entity("some-sample",
             "Sample",
             Map(
               AttributeName.withDefaultNS("indexValue") -> AttributeString(s"index-$idx")
             )
      )

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(entityQuery.save(context, makeEntity(0)))

      // did we save the entity?
      // did we populate its all_attribute_values?
      val entityWithAllAttrs = runAndWait(
        entityQueryWithInlineAttributes
          .findEntityByName(testData.workspace.workspaceIdAsUUID, "Sample", "some-sample")
          .result
      )
      entityWithAllAttrs should have length 1
      entityWithAllAttrs.head.recordVersion shouldBe 0
      entityWithAllAttrs.head.allAttributeValues should not be empty
      entityWithAllAttrs.head.allAttributeValues.get should include("index-0")
    }

    withWorkspaceContext(testData.workspace) { context =>
      val count = 20
      (1 to count) foreach { idx =>
        runAndWait(entityQuery.save(context, makeEntity(idx)))
      }

      // did we update the record versions and populate its all_attribute_values?
      val entityWithAllAttrs = runAndWait(
        entityQueryWithInlineAttributes
          .findEntityByName(testData.workspace.workspaceIdAsUUID, "Sample", "some-sample")
          .result
      )
      entityWithAllAttrs should have length 1
      entityWithAllAttrs.head.recordVersion shouldBe count
      entityWithAllAttrs.head.allAttributeValues should not be empty
      entityWithAllAttrs.head.allAttributeValues.get should not be empty
      entityWithAllAttrs.head.allAttributeValues.get should include(
        "index-20"
      ) // we should have updated to the newest value
    }
  }

  it should "clone all entities from a workspace containing cycles" in withDefaultTestDatabase {
    val workspaceOriginal = Workspace(
      namespace = testData.wsName.namespace + "Original",
      name = testData.wsName.name + "Original",
      workspaceId = UUID.randomUUID.toString,
      bucketName = "aBucket",
      workflowCollectionName = Some("workflow-collection"),
      createdDate = currentTime(),
      lastModified = currentTime(),
      createdBy = "Joe Biden",
      Map.empty
    )

    val workspaceClone = Workspace(
      namespace = testData.wsName.namespace + "Clone",
      name = testData.wsName.name + "Clone",
      workspaceId = UUID.randomUUID.toString,
      bucketName = "anotherBucket",
      workflowCollectionName = Some("workflow-collection"),
      createdDate = currentTime(),
      lastModified = currentTime(),
      createdBy = "Joe Biden",
      Map.empty
    )

    val c1 = Entity(
      "c1",
      "samples",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("x"),
        AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
        AttributeName.withDefaultNS("cycle1") -> AttributeEntityReference("samples", "c2")
      )
    )
    val c2 = Entity(
      "c2",
      "samples",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("x"),
        AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
        AttributeName.withDefaultNS("cycle2") -> AttributeEntityReference("samples", "c3")
      )
    )
    val c3 = Entity("c3",
                    "samples",
                    Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"),
                        AttributeName.withDefaultNS("bar") -> AttributeNumber(3)
                    )
    )

    runAndWait(workspaceQuery.createOrUpdate(workspaceOriginal))
    runAndWait(workspaceQuery.createOrUpdate(workspaceClone))

    withWorkspaceContext(workspaceOriginal) { originalContext =>
      withWorkspaceContext(workspaceClone) { cloneContext =>
        runAndWait(entityQuery.save(originalContext, c3))
        runAndWait(entityQuery.save(originalContext, c2))
        runAndWait(entityQuery.save(originalContext, c1))

        val c3_updated = Entity(
          "c3",
          "samples",
          Map(
            AttributeName.withDefaultNS("foo") -> AttributeString("x"),
            AttributeName.withDefaultNS("bar") -> AttributeNumber(3),
            AttributeName.withDefaultNS("cycle3") -> AttributeEntityReference("samples", "c1")
          )
        )

        runAndWait(entityQuery.save(originalContext, c3_updated))
        runAndWait(
          entityQuery.copyEntitiesToNewWorkspace(originalContext.workspaceIdAsUUID, cloneContext.workspaceIdAsUUID)
        )

        val expectedEntities = Set(c1, c2, c3_updated)
        assertResult(expectedEntities) {
          runAndWait(entityQuery.listActiveEntities(originalContext)).toSet
        }
        assertResult(expectedEntities) {
          runAndWait(entityQuery.listActiveEntities(cloneContext)).toSet
        }
        val destShardId = determineShard(cloneContext.workspaceIdAsUUID)
        val destWsId = cloneContext.workspaceIdAsUUID
        val badEntityReferenceCount =
          sql"""select count(*) from ENTITY e join ENTITY_ATTRIBUTE_#$destShardId ea on e.id = ea.owner_id
                        join ENTITY e_ref on ea.value_entity_ref = e_ref.id
                        where ea.value_entity_ref is not null and e_ref.workspace_id != $destWsId and e.workspace_id = $destWsId"""
        assertResult(
          0,
          "cloned entity references should only point to entities within the same workspace, " +
            "we found some entity references that don't belong to the destination workspace"
        ) {
          runAndWait(badEntityReferenceCount.as[Int].head)
        }

        val expectedEntityReferenceCount =
          sql"""select count(*) from ENTITY e join ENTITY_ATTRIBUTE_#$destShardId ea on e.id = ea.owner_id
                        join ENTITY e_ref on ea.value_entity_ref = e_ref.id
                        where ea.value_entity_ref is not null and e_ref.workspace_id = $destWsId and e.workspace_id = $destWsId"""
        assertResult(3, "cloned entity references should only point to entities within the same workspace") {
          runAndWait(expectedEntityReferenceCount.as[Int].head)
        }
      }
    }

  }

  it should "throw an exception if trying to save invalid references" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      val baz =
        Entity("wig",
               "wug",
               Map(
                 AttributeName.withDefaultNS("edgeToNowhere") -> AttributeEntityReference("sample",
                                                                                          "notTheSampleYoureLookingFor"
                 )
               )
        )
      intercept[RawlsException] {
        runAndWait(entityQuery.save(context, baz))
      }
    }

  }

  it should "list entities" in withConstantTestDatabase {
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

    withWorkspaceContext(constantData.workspace) { context =>
      assertSameElements(expected, runAndWait(entityQuery.listActiveEntitiesOfType(context, "Sample")))
    }
  }

  it should "add cycles to entity graph" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      val sample1Copy = Entity(
        "sample1",
        "Sample",
        Map(
          AttributeName.withDefaultNS("type") -> AttributeString("normal"),
          AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
          AttributeName.withDefaultNS("thingies") -> AttributeValueList(
            Seq(AttributeString("a"), AttributeString("b"))
          ),
          AttributeName.withDefaultNS("aliquot") -> AttributeEntityReference("Aliquot", "aliquot1"),
          AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset1")
        )
      )
      runAndWait(entityQuery.save(context, sample1Copy))
      val sample5Copy = Entity(
        "sample5",
        "Sample",
        Map(
          AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
          AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
          AttributeName.withDefaultNS("thingies") -> AttributeValueList(
            Seq(AttributeString("a"), AttributeString("b"))
          ),
          AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset4")
        )
      )
      runAndWait(entityQuery.save(context, sample5Copy))
      val sample7Copy = Entity(
        "sample7",
        "Sample",
        Map(
          AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
          AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
          AttributeName.withDefaultNS("thingies") -> AttributeValueList(
            Seq(AttributeString("a"), AttributeString("b"))
          ),
          AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("Sample", "sample6")
        )
      )
      runAndWait(entityQuery.save(context, sample7Copy))
      val sample6Copy = Entity(
        "sample6",
        "Sample",
        Map(
          AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
          AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
          AttributeName.withDefaultNS("thingies") -> AttributeValueList(
            Seq(AttributeString("a"), AttributeString("b"))
          ),
          AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset3")
        )
      )
      runAndWait(entityQuery.save(context, sample6Copy))
      val entitiesWithCycles = List(sample1Copy, sample5Copy, sample7Copy, sample6Copy)
      entitiesWithCycles.foreach(entity =>
        assertResult(Option(entity))(runAndWait(entityQuery.get(context, entity.entityType, entity.name)))
      )
    }

  }

  it should "rename an entity" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      assertResult(Option(testData.pair1))(runAndWait(entityQuery.get(context, "Pair", "pair1")))
      assertResult(1)(runAndWait(entityQuery.rename(context, "Pair", "pair1", "amazingPair")))
      assertResult(None)(runAndWait(entityQuery.get(context, "Pair", "pair1")))
      assertResult(Option(testData.pair1.copy(name = "amazingPair"))) {
        runAndWait(entityQuery.get(context, "Pair", "amazingPair"))
      }
    }

  }

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */
  it should "get entity subtrees from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val sampleSet1Paths = Seq(
        EntityPath(Seq(testData.sset1.toReference)),
        EntityPath(Seq(testData.sset1.toReference, testData.sample1.toReference)),
        EntityPath(Seq(testData.sset1.toReference, testData.sample1.toReference, testData.aliquot1.toReference)),
        EntityPath(Seq(testData.sset1.toReference, testData.sample2.toReference)),
        EntityPath(Seq(testData.sset1.toReference, testData.sample3.toReference)),
        EntityPath(Seq(testData.sset1.toReference, testData.sample3.toReference, testData.sample1.toReference))
      )
      val sampleSet2Paths = Seq(EntityPath(Seq(testData.sset2.toReference)),
                                EntityPath(Seq(testData.sset2.toReference, testData.sample2.toReference))
      )
      val sampleSet3Paths = Seq(
        EntityPath(Seq(testData.sset3.toReference)),
        EntityPath(Seq(testData.sset3.toReference, testData.sample5.toReference)),
        EntityPath(Seq(testData.sset3.toReference, testData.sample6.toReference))
      )

      val expected = sampleSet1Paths ++ sampleSet2Paths ++ sampleSet3Paths
      assertSameElements(
        expected,
        runAndWait(
          entityQuery.getEntitySubtrees(context, "SampleSet", Set("sset1", "sset2", "sset3", "sampleSetDOESNTEXIST"))
        )
      )

      val individual2Paths = Seq(
        EntityPath(Seq(testData.indiv2.toReference)),
        EntityPath(Seq(testData.indiv2.toReference, testData.sset2.toReference)),
        EntityPath(Seq(testData.indiv2.toReference, testData.sset2.toReference, testData.sample2.toReference))
      )
      assertSameElements(individual2Paths,
                         runAndWait(entityQuery.getEntitySubtrees(context, "Individual", Set("indiv2")))
      )
    }
  }

  // the opposite of the above traversal: get all reference to these entities, traversing upward

  it should "get the full set of entity references from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val expected = Set(testData.sample1,
                         testData.sample3,
                         testData.pair1,
                         testData.pair2,
                         testData.sset1,
                         testData.ps1,
                         testData.indiv1
      ).map(_.toReference)
      assertSameElements(expected,
                         runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.sample1.toReference)))
      )

      val expected2 = Set(testData.aliquot1,
                          testData.aliquot2,
                          testData.sample1,
                          testData.sample3,
                          testData.pair1,
                          testData.pair2,
                          testData.sset1,
                          testData.ps1,
                          testData.indiv1
      ).map(_.toReference)
      assertSameElements(expected2,
                         runAndWait(
                           entityQuery.getAllReferringEntities(context,
                                                               Set(testData.aliquot1.toReference,
                                                                   testData.aliquot2.toReference
                                                               )
                           )
                         )
      )
    }
  }

  it should "not include deleted entities when getting the full set of entity references from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(entityQuery.hide(context, Seq(testData.indiv1.toReference, testData.pair2.toReference)))

      val expected =
        Set(testData.sample1, testData.sample3, testData.pair1, testData.sset1, testData.ps1).map(_.toReference)
      assertSameElements(expected,
                         runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.sample1.toReference)))
      )

      val expected2 = Set(testData.aliquot1,
                          testData.aliquot2,
                          testData.sample1,
                          testData.sample3,
                          testData.pair1,
                          testData.sset1,
                          testData.ps1
      ).map(_.toReference)
      assertSameElements(expected2,
                         runAndWait(
                           entityQuery.getAllReferringEntities(context,
                                                               Set(testData.aliquot1.toReference,
                                                                   testData.aliquot2.toReference
                                                               )
                           )
                         )
      )
    }
  }

  val x1 =
    Entity("x1", "SampleSet", Map(AttributeName.withDefaultNS("child") -> AttributeEntityReference("SampleSet", "x2")))
  val x2 = Entity("x2", "SampleSet", Map.empty)

  val workspace2 = Workspace(
    namespace = testData.wsName.namespace + "2",
    name = testData.wsName.name + "2",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty
  )

  val workspace3 = Workspace(
    namespace = testData.wsName.namespace + "3",
    name = testData.wsName.name + "3",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty
  )

  it should "copy entities without a conflict" in withDefaultTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace2))
    withWorkspaceContext(testData.workspace) { context1 =>
      withWorkspaceContext(workspace2) { context2 =>
        runAndWait(entityQuery.save(context2, x2))
        runAndWait(entityQuery.save(context2, x1))
        val x2_updated = Entity("x2",
                                "SampleSet",
                                Map(AttributeName.withDefaultNS("child") -> AttributeEntityReference("SampleSet", "x1"))
        )
        runAndWait(entityQuery.save(context2, x2_updated))

        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context2, "SampleSet")).toList.contains(x1))
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context2, "SampleSet")).toList.contains(x2_updated))

        // note: we're copying FROM workspace2 INTO workspace
        assertResult(Seq.empty) {
          runAndWait(entityQuery.getCopyConflicts(context1, Seq(x1, x2_updated).map(_.toReference)))
        }

        assertSameElements(
          Seq(x1.toReference, x2.toReference),
          runAndWait(
            entityQuery.checkAndCopyEntities(context2, context1, "SampleSet", Seq("x2"), false, testContext)
          ).entitiesCopied
        )

        // verify it was actually copied into the workspace
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context1, "SampleSet")).toList.contains(x1))
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context1, "SampleSet")).toList.contains(x2_updated))
      }
    }
  }

  it should "copy entities without a conflict with a cycle" in withDefaultTestDatabase {

    runAndWait(workspaceQuery.createOrUpdate(workspace2))
    withWorkspaceContext(testData.workspace) { context1 =>
      withWorkspaceContext(workspace2) { context2 =>
        val a = Entity("a", "test", Map.empty)
        val a6 =
          Entity("a6", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a1")))
        val a5 =
          Entity("a5", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a6")))
        val a4 =
          Entity("a4", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a5")))
        val a3 = Entity(
          "a3",
          "test",
          Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a4"),
              AttributeName.withDefaultNS("side") -> AttributeEntityReference("test", "a")
          )
        )
        val a2 =
          Entity("a2", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a3")))
        val a1 =
          Entity("a1", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a2")))
        val allEntities = Seq(a, a1, a2, a3, a4, a5, a6)
        runAndWait(entityQuery.save(context2, a))

        // save a6 first without attributes because a1 does not exist yet
        runAndWait(entityQuery.save(context2, a6.copy(attributes = Map.empty)))
        runAndWait(entityQuery.save(context2, a5))
        runAndWait(entityQuery.save(context2, a4))
        runAndWait(entityQuery.save(context2, a3))
        runAndWait(entityQuery.save(context2, a2))
        runAndWait(entityQuery.save(context2, a1))

        // the cycle
        runAndWait(entityQuery.save(context2, a6))

        // note: we're copying FROM workspace2 INTO workspace
        assertResult(Seq.empty) {
          runAndWait(entityQuery.getCopyConflicts(context1, allEntities.map(_.toReference)))
        }

        assertSameElements(allEntities.map(_.toReference),
                           runAndWait(
                             entityQuery.checkAndCopyEntities(context2, context1, "test", Seq("a1"), false, testContext)
                           ).entitiesCopied
        )

        // verify it was actually copied into the workspace
        assertSameElements(allEntities, runAndWait(entityQuery.listActiveEntitiesOfType(context1, "test")).toSet)
      }
    }

  }

  it should "copy entities with a conflict" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>
      assertResult(Set(testData.sample1.toReference)) {
        runAndWait(entityQuery.getCopyConflicts(context, Seq(testData.sample1).map(_.toReference)))
          .map(_.toReference)
          .toSet
      }

      assertResult(Set(EntityHardConflict(testData.sample1.entityType, testData.sample1.name))) {
        runAndWait(
          entityQuery.checkAndCopyEntities(context, context, "Sample", Seq("sample1"), false, testContext)
        ).hardConflicts.toSet
      }

      // verify that it wasn't copied into the workspace again
      assert(
        runAndWait(entityQuery.listActiveEntitiesOfType(context, "Sample")).toList
          .filter(entity => entity == testData.sample1)
          .size == 1
      )
    }

  }

  it should "copy entities with a conflict in the entity subtrees and properly link already existing entities" in withDefaultTestDatabase {

    runAndWait(workspaceQuery.createOrUpdate(workspace2))
    runAndWait(workspaceQuery.createOrUpdate(workspace3))
    withWorkspaceContext(workspace2) { context2 =>
      withWorkspaceContext(workspace3) { context3 =>
        val participant1 = Entity("participant1", "participant", Map.empty)
        val sample1 = Entity(
          "sample1",
          "sample",
          Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "participant1"))
        )

        runAndWait(entityQuery.save(context2, participant1))
        runAndWait(entityQuery.save(context3, participant1))
        runAndWait(entityQuery.save(context3, sample1))

        assertResult(List()) {
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "sample")).toList
        }

        assertResult(List(participant1)) {
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "participant")).toList
        }

        runAndWait(entityQuery.checkAndCopyEntities(context3, context2, "sample", Seq("sample1"), true, testContext))

        assertResult(List(sample1)) {
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "sample")).toList
        }

        assertResult(List(participant1)) {
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "participant")).toList
        }

      }
    }
  }

  it should "fail when putting dots in user-specified strings" in withDefaultTestDatabase {
    // NB: entity names allow dots, so entity names are not included in this test
    val dottyType = Entity("dottyType", "Sam.ple", Map.empty)
    val dottyAttr = Entity("dottyAttr", "Sample", Map(AttributeName.withDefaultNS("foo.bar") -> AttributeBoolean(true)))
    val dottyAttr2 = Entity("dottyAttr", "Sample", Map(AttributeName("library", "foo.bar") -> AttributeBoolean(true)))

    withWorkspaceContext(testData.workspace) { context =>
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyType)))
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyAttr)))
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyAttr2)))
    }

  }

  Attributable.reservedAttributeNames.foreach { reserved =>
    it should s"fail using reserved attribute name ${reserved.name} in default namespace" in withDefaultTestDatabase {
      val e = Entity("test_sample", "Sample", Map(reserved -> AttributeString("foo")))

      withWorkspaceContext(testData.workspace) { context =>
        intercept[RawlsException] {
          runAndWait(entityQuery.save(context, e))
        }
      }
    }

    AttributeName.validNamespaces.-(AttributeName.defaultNamespace).foreach { namespace =>
      it should s"succeed using reserved attribute name ${reserved.name} in namespace $namespace" in withDefaultTestDatabase {
        val e = Entity("test_sample", "Sample", Map(AttributeName(namespace, reserved.name) -> AttributeString("foo")))

        withWorkspaceContext(testData.workspace) { context =>
          runAndWait(entityQuery.save(context, e))
        }
        assert {
          runAndWait(entityQuery.get(testData.workspace, "Sample", "test_sample")).isDefined
        }
      }
    }
  }

  it should s"fail using reserved attribute name sample_id in namespace default for sample entity type" in withDefaultTestDatabase {
    val e = Entity("test_sample", "Sample", Map(AttributeName.withDefaultNS("sample_id") -> AttributeString("foo")))

    withWorkspaceContext(testData.workspace) { context =>
      intercept[RawlsException] {
        runAndWait(entityQuery.save(context, e))
      }
    }
  }

  it should "save a new entity with the same name as a deleted entity" in withDefaultTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace("test_namespace",
                                         workspaceId.toString,
                                         workspaceId.toString,
                                         "bucketname",
                                         Some("workflow-collection"),
                                         currentTime(),
                                         currentTime(),
                                         "me",
                                         Map.empty,
                                         false
    )
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    val workspaceContext = workspace

    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val entity = Entity("name", "type", Map.empty)

    assertResult(entity)(runAndWait(entityQuery.save(workspaceContext, entity)))
    assertResult(Some(entity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val oldId = runAndWait(entityQuery.findEntityByName(workspaceId, "type", "name").result).head.id

    assertResult(1)(runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))))
    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    assertResult(entity)(runAndWait(entityQuery.save(workspaceContext, entity)))
    assertResult(Some(entity))(runAndWait(entityQuery.get(workspaceContext, "type", "name")))

    val newId = runAndWait(entityQuery.findEntityByName(workspaceId, "type", "name").result).head.id

    assert(oldId != newId)
  }

  it should "delete an entity type and return the total number of rows deleted (hidden)" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val sampleCount = runAndWait(entityQuery.listActiveEntitiesOfType(context, "sample")).iterator.size

      assertResult(sampleCount) {
        runAndWait(entityQuery.hideType(context, "sample"))
      }
    }
  }

  it should "delete a set without affecting its component entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      assertResult(Some(testData.sset1)) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
      assertResult(Some(testData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(testData.sample2)) {
        runAndWait(entityQuery.get(context, "Sample", "sample2"))
      }
      assertResult(Some(testData.sample3)) {
        runAndWait(entityQuery.get(context, "Sample", "sample3"))
      }

      assertResult(1) {
        runAndWait(entityQuery.hide(context, Seq(testData.sset1.toReference)))
      }

      assertResult(None) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
      assertResult(Some(testData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(testData.sample2)) {
        runAndWait(entityQuery.get(context, "Sample", "sample2"))
      }
      assertResult(Some(testData.sample3)) {
        runAndWait(entityQuery.get(context, "Sample", "sample3"))
      }
    }
  }

  it should "delete a sample without affecting its individual or any other entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val (entityCount1, attributeCount1) = countEntitiesAttrs(testData.workspace)
      val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(testData.workspace)

      val bob = Entity(
        "Bob",
        "Individual",
        Map(AttributeName.withDefaultNS("alive") -> AttributeBoolean(false),
            AttributeName.withDefaultNS("sampleSite") -> AttributeString("head")
        )
      )
      val blood = Entity("Bob-Blood",
                         "Sample",
                         Map(AttributeName.withDefaultNS("indiv") -> bob.toReference,
                             AttributeName.withDefaultNS("color") -> AttributeString("red")
                         )
      )
      val bone = Entity("Bob-Bone",
                        "Sample",
                        Map(AttributeName.withDefaultNS("indiv") -> bob.toReference,
                            AttributeName.withDefaultNS("color") -> AttributeString("white")
                        )
      )

      runAndWait(entityQuery.save(context, bob))
      runAndWait(entityQuery.save(context, blood))
      runAndWait(entityQuery.save(context, bone))

      assertResult(Some(bob)) {
        runAndWait(entityQuery.get(context, "Individual", "Bob"))
      }
      assertResult(Some(blood)) {
        runAndWait(entityQuery.get(context, "Sample", blood.name))
      }
      assertResult(Some(bone)) {
        runAndWait(entityQuery.get(context, "Sample", bone.name))
      }

      val (entityCount2, attributeCount2) = countEntitiesAttrs(testData.workspace)
      val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(testData.workspace)

      assertResult(entityCount1 + 3)(entityCount2)
      assertResult(attributeCount1 + 6)(attributeCount2)
      assertResult(activeEntityCount1 + 3)(activeEntityCount2)
      assertResult(activeAttributeCount1 + 6)(activeAttributeCount2)

      assertResult(1) {
        runAndWait(entityQuery.hide(context, Seq(bone.toReference)))
      }

      assertResult(Some(bob)) {
        runAndWait(entityQuery.get(context, "Individual", "Bob"))
      }
      assertResult(Some(blood)) {
        runAndWait(entityQuery.get(context, "Sample", blood.name))
      }
      assertResult(None) {
        runAndWait(entityQuery.get(context, "Sample", bone.name))
      }

      val (entityCount3, attributeCount3) = countEntitiesAttrs(testData.workspace)
      val (activeEntityCount3, activeAttributeCount3) = countActiveEntitiesAttrs(testData.workspace)

      assertResult(entityCount2)(entityCount3)
      assertResult(attributeCount2)(attributeCount3)
      assertResult(activeEntityCount2 - 1)(activeEntityCount3)
      assertResult(activeAttributeCount2 - 2)(activeAttributeCount3)

    }
  }

  it should "return only the selected attributes" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val testAttribute1 = AttributeName.withDefaultNS("attr1") -> AttributeString("val1")
      val testAttribute2 = AttributeName.withDefaultNS("attr2") -> AttributeString("val2")
      val testAttribute3 = AttributeName.withDefaultNS("attr3") -> AttributeString("val3")
      val testAttribute4 = AttributeName.withDefaultNS("attr4") -> AttributeString("val4")

      val entityToSave = Entity(
        "testName",
        "testType",
        Map(testAttribute1, testAttribute2, testAttribute3, testAttribute4)
      )

      runAndWait(entityQuery.save(context, entityToSave))

      val result = runAndWait(entityQuery.get(
        context,
        entityToSave.entityType,
        entityToSave.name,
        Set(testAttribute1._1, testAttribute3._1)
      ))

      assert(result.isDefined)
      val resAttributeNames = result.get.attributes.keys

      assert(resAttributeNames.exists(_.equalsIgnoreCase(testAttribute1._1)),
        "Attribute 1 should be returned by the filter query")
      assert(resAttributeNames.exists(_.equalsIgnoreCase(testAttribute3._1)),
        "Attribute 3 should be returned by the filter query")
      resAttributeNames.size shouldBe 2
    }
  }

  it should "select the all_attribute_values column when using entityQueryWithInlineAttributes and not otherwise" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val hasAttrs = Entity(
        "entityWithAttrs",
        "Pair",
        Map(
          AttributeName.withDefaultNS("attrOne") -> AttributeString("one"),
          AttributeName.withDefaultNS("attrTwo") -> AttributeString("two"),
          AttributeName.withDefaultNS("attrThree") -> AttributeString("three"),
          AttributeName.withDefaultNS("attrFour") -> AttributeString("four")
        )
      )

      runAndWait(entityQuery.save(context, hasAttrs))

      val recWithAttrs = runAndWait(
        entityQueryWithInlineAttributes
          .filter(e => e.name === "entityWithAttrs" && e.entityType === "Pair")
          .result
          .headOption
      )

      assert(recWithAttrs.isDefined, "entityQuery should find the record")
      withClue(
        "entityQueryWithInlineAttributes should return an EntityRecordWithInlineAttributes, which has allAttributeValues"
      ) {
        recWithAttrs shouldBe an[Option[EntityRecordWithInlineAttributes]]
      }
      assertResult(Some(Some("entitywithattrs one two three four")), "entityQuery should return allAttributeValues") {
        recWithAttrs.map(_.allAttributeValues)
      }

      val recWithoutAttrs = runAndWait(
        entityQuery
          .filter(e => e.name === "entityWithAttrs" && e.entityType === "Pair")
          .result
          .headOption
      )

      assert(recWithoutAttrs.isDefined, "entityQuery should find the record")
      withClue("entityQuery should return an EntityRecord, which does not have allAttributeValues") {
        recWithoutAttrs shouldBe an[Option[EntityRecord]]
      }

      assertResult(recWithoutAttrs,
                   "entityQuery and entityQueryWithInlineAttributes should return the same record otherwise"
      ) {
        recWithAttrs.map(_.withoutAllAttributeValues)
      }
    }
  }

  it should "delete all values in an attribute list" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { wsctx =>
      val entityToSave = Entity(
        "testName",
        "testType",
        Map(
          AttributeName.withDefaultNS("attributeListToDelete") -> AttributeValueList(
            List(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3), AttributeNumber(4), AttributeNumber(5))
          )
        )
      )

      runAndWait(entityQuery.save(wsctx, entityToSave))

      val initialResult = runAndWait(entityQuery.get(wsctx, entityToSave.entityType, entityToSave.name))
      assert(initialResult.get.attributes.nonEmpty)

      runAndWait(
        entityQuery.saveEntityPatch(wsctx,
                                    entityToSave.toReference,
                                    Map.empty,
                                    List(AttributeName.withDefaultNS("attributeListToDelete"))
        )
      )

      val updatedResult = runAndWait(entityQuery.get(wsctx, entityToSave.entityType, entityToSave.name))
      assert(updatedResult.get.attributes.isEmpty)
    }
  }

  private def caseSensitivityFixtures(context: Workspace) = {
    val entitiesToSave = Seq(
      Entity("name-1",
             "mytype",
             Map(AttributeName.withDefaultNS("case") -> AttributeString("value1"),
                 AttributeName.withDefaultNS("foo") -> AttributeString("bar")
             )
      ),
      Entity("name-2",
             "mytype",
             Map(AttributeName.withDefaultNS("CASE") -> AttributeString("value2"),
                 AttributeName.withDefaultNS("foo") -> AttributeString("bar")
             )
      ),
      Entity("name-3",
             "mytype",
             Map(AttributeName.withDefaultNS("case") -> AttributeString("value3"),
                 AttributeName.withDefaultNS("CASE") -> AttributeString("value4")
             )
      ),
      Entity("name-4", "anothertype", Map(AttributeName.withDefaultNS("case") -> AttributeString("value5"))),
      Entity("name-5", "anothertype", Map(AttributeName.withDefaultNS("CASE") -> AttributeString("value6")))
    )
    runAndWait(entityQuery.save(context, entitiesToSave))

    assume(runAndWait(entityQuery.listEntities(context)).size == 5,
           "filteredCount tests did not set up fixtures correctly"
    )

  }

  // following set of filteredCount tests all use the same entity fixtures on top of an empty workspace
  val emptyWorkspace = new EmptyWorkspace

  List("mytype", "anothertype") foreach { typeName =>
    List("name", "case", "CASE") foreach { sortKey =>
      List(SortDirections.Ascending, SortDirections.Descending) foreach { sortDir =>
        it should s"return filteredCount == unfilteredCount if no filter terms, type=[$typeName], sortKey=[$sortKey], sortDir=[$sortDir]" in withCustomTestDatabase(
          emptyWorkspace
        ) { _ =>
          withWorkspaceContext(emptyWorkspace.workspace) { context =>
            caseSensitivityFixtures(context)

            assume(runAndWait(entityQuery.listEntities(context)).size == 5,
                   "filteredCount tests did not set up fixtures correctly, within first test"
            )
            val unfilteredQuery = EntityQuery(1, 1, sortKey, sortDir, None)
            val pageResult = runAndWait(entityQuery.loadEntityPage(context, typeName, unfilteredQuery, testContext))
            pageResult._2 should be > 0
            pageResult._2 shouldBe pageResult._1
          }
        }
      }
    }
  }

  val filterFixtures = Map(
    "mytype" -> Map("bar" -> 2, "value1" -> 1, "value" -> 3, "nonexistent" -> 0),
    "anothertype" -> Map("value5" -> 1, "value" -> 2, "alsononexistent" -> 0)
  )

  filterFixtures foreach { typeFixtures =>
    val typeName = typeFixtures._1
    val typeTests = typeFixtures._2
    typeTests foreach { test =>
      val filterTerm = test._1
      val expectedCount = test._2
      List("name", "case", "CASE") foreach { sortKey =>
        List(SortDirections.Ascending, SortDirections.Descending) foreach { sortDir =>
          it should s"return expected count ($expectedCount) for type=[$typeName], sortKey=[$sortKey], sortDir=[$sortDir], term [$filterTerm]" in withCustomTestDatabase(
            emptyWorkspace
          ) { _ =>
            withWorkspaceContext(emptyWorkspace.workspace) { context =>
              caseSensitivityFixtures(context)
              val filterQuery = EntityQuery(1, 1, sortKey, sortDir, Some(filterTerm))
              val pageResult = runAndWait(entityQuery.loadEntityPage(context, typeName, filterQuery, testContext))
              pageResult._2 shouldBe expectedCount
            }
          }
        }
      }
    }
  }

}
