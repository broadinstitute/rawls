
package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import _root_.slick.dbio.DBIOAction
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils, model}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by dvoet on 2/12/16.
 */
class EntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {
  import driver.api._

  // entity and attribute counts, regardless of deleted status
  def countEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listEntities(SlickWorkspaceContext(workspace)))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  // entity and attribute counts, non-deleted only
  def countActiveEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listActiveEntities(SlickWorkspaceContext(workspace)))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  "EntityComponent" should "crud entities" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace("test_namespace", workspaceId.toString, Set.empty, workspaceId.toString, "bucketname", currentTime(), currentTime(), "me", Map.empty, Map.empty, Map.empty, false)
    runAndWait(workspaceQuery.save(workspace))
    val workspaceContext = SlickWorkspaceContext(workspace)

    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val entity = Entity("name", "type", Map.empty)

    assertResult(entity) { runAndWait(entityQuery.save(workspaceContext, entity)) }
    assertResult(Some(entity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val target1 = Entity("target1", "type", Map.empty)
    runAndWait(entityQuery.save(workspaceContext, target1))
    val target2 = Entity("target2", "type", Map.empty)
    runAndWait(entityQuery.save(workspaceContext, target2))

    val updatedEntity = entity.copy(attributes = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("foo"),
      AttributeName.withDefaultNS("ref") -> target1.toReference,
      AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(Seq(target1.toReference, target2.toReference))))

    assertResult(updatedEntity) { runAndWait(entityQuery.save(workspaceContext, updatedEntity)) }
    assertResult(Some(updatedEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val updatedAgainEntity = updatedEntity.copy(attributes = Map(
      AttributeName.withDefaultNS("string2") -> AttributeString("foo"),
      AttributeName.withDefaultNS("ref") -> target2.toReference,
      AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(Seq(target2.toReference, target1.toReference))))
    assertResult(updatedAgainEntity) { runAndWait(entityQuery.save(workspaceContext, updatedAgainEntity)) }
    assertResult(Some(updatedAgainEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    assertResult(entity) { runAndWait(entityQuery.save(workspaceContext, entity)) }
    assertResult(Some(entity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    //save AttributeValueEmptyList
    val emptyValListAttributeEntity = entity.copy(name = "emptyValListy", attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeValueEmptyList))
    runAndWait(entityQuery.save(workspaceContext, emptyValListAttributeEntity))
    assertResult(Some(emptyValListAttributeEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "emptyValListy")) }

    //convert AttributeValueList(Seq()) -> AttributeEmptyList
    val emptyValListEntity = entity.copy(name = "emptyValList", attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeValueList(Seq())))
    runAndWait(entityQuery.save(workspaceContext, emptyValListEntity))
    assertResult(Some(emptyValListAttributeEntity.copy(name="emptyValList"))) { runAndWait(entityQuery.get(workspaceContext, "type", "emptyValList")) }

    //save AttributeEntityReferenceEmptyList
    val emptyRefListAttributeEntity = entity.copy(name = "emptyRefListy", attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeEntityReferenceEmptyList))
    runAndWait(entityQuery.save(workspaceContext, emptyRefListAttributeEntity))
    assertResult(Some(emptyRefListAttributeEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "emptyRefListy")) }

    //convert AttributeEntityReferenceList(Seq()) -> AttributeEntityReferenceEmptyList
    val emptyRefListEntity = entity.copy(name = "emptyRefList", attributes = Map(AttributeName.withDefaultNS("emptyList") -> AttributeEntityReferenceList(Seq())))
    runAndWait(entityQuery.save(workspaceContext, emptyRefListEntity))
    assertResult(Some(emptyRefListAttributeEntity.copy(name="emptyRefList"))) { runAndWait(entityQuery.get(workspaceContext, "type", "emptyRefList")) }

    val (entityCount1, attributeCount1) = countEntitiesAttrs(workspace)
    val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(workspace)

    // "hide" deletion

    assertResult(1) { runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))) }
    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }
    assertResult(0) { runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))) }

    val (entityCount2, attributeCount2) = countEntitiesAttrs(workspace)
    val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(workspace)

    assertResult(entityCount1)(entityCount2)
    assertResult(attributeCount1)(attributeCount2)
    assertResult(activeEntityCount1 - 1)(activeEntityCount2)
    assertResult(activeAttributeCount1)(activeAttributeCount2)

    // actual deletion

    val entityForDeletion = Entity("delete-me", "type", Map.empty)

    assertResult(entityForDeletion) { runAndWait(entityQuery.save(workspaceContext, entityForDeletion)) }
    assertResult(Some(entityForDeletion)) { runAndWait(entityQuery.get(workspaceContext, "type", "delete-me")) }

    val (entityCount3, attributeCount3) = countEntitiesAttrs(workspace)
    val (activeEntityCount3, activeAttributeCount3) = countActiveEntitiesAttrs(workspace)

    assertResult(entityCount2 + 1)(entityCount3)
    assertResult(attributeCount2)(attributeCount3)
    assertResult(activeEntityCount2 + 1)(activeEntityCount3)
    assertResult(activeAttributeCount2)(activeAttributeCount3)

    assertResult(entityCount3) { runAndWait(entityQuery.deleteFromDb(workspaceContext.workspaceId)) }
    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "delete-me")) }
    assertResult(0) { runAndWait(entityQuery.deleteFromDb(workspaceContext.workspaceId)) }

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
          entityQuery.saveEntityPatch(context, AttributeEntityReference("Sample", "nonexistent"),
            Map(AttributeName.withDefaultNS("newAttribute") -> AttributeNumber(2)),
            Seq(AttributeName.withDefaultNS("type"))
          ))
      }
      //make sure we get the _right_ RawlsException:
      //"saveEntityPatch looked up $entityRef expecting 1 record, got 0 instead"
      caught.getMessage should include("expecting")
    }
  }

  it should "fail to saveEntityPatch if you try to delete and upsert the same attribute" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      val caught = intercept[RawlsException] {
        runAndWait(
          entityQuery.saveEntityPatch(context, AttributeEntityReference("Sample", "sample1"),
            Map(AttributeName.withDefaultNS("type") -> AttributeNumber(2)),
            Seq(AttributeName.withDefaultNS("type"))
          ))
      }
      //make sure we get the _right_ RawlsException:
      //"Can't saveEntityPatch on $entityRef because upserts and deletes share attributes <blah>"
      caught.getMessage should include("share")
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
        AttributeName.withDefaultNS("quot1") -> testData.aliquot2.toReference //jerk move
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
        runAndWait(entityQuery.loadEntityPage(context, "Sample", model.EntityQuery(1, 10, "name", SortDirections.Ascending, Option("sample1 2 tumor aliquot2 aliquot1 aliquot2 itsfoo"))))._2
      }
    }
  }

  it should "list all entities of all entity types" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      assertSameElements(constantData.allEntities, runAndWait(entityQuery.listActiveEntities(context)))
    }
  }

  it should "list all entity types with their counts" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      assertResult(Map("PairSet" -> 1, "Individual" -> 2, "Sample" -> 8, "Aliquot" -> 2, "SampleSet" -> 5, "Pair" -> 2)) {
        runAndWait(entityQuery.getEntityTypesWithCounts(context))
      }
    }
  }

  it should "skip deleted entities when listing all entity types with their counts" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val deleteSamples = entityQuery.findActiveEntityByType(context.workspaceId, "Sample").result flatMap { entityRecs =>
        val deleteActions = entityRecs map { rec => entityQuery.hide(context, Seq(rec.toReference)) }
        DBIO.seq(deleteActions:_*)
      }
      runAndWait(deleteSamples)

      assertResult(Map("PairSet" -> 1, "Individual" -> 2, "Aliquot" -> 2, "SampleSet" -> 5, "Pair" -> 2)) {
        runAndWait(entityQuery.getEntityTypesWithCounts(context))
      }
    }
  }

  it should "list all entity types with their attribute names" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>

      val desiredTypesAndAttrNames = Map(
        "Sample" -> Seq("type", "whatsit", "thingies", "quot", "somefoo", "tumortype", "confused", "cycle", "foo_id"),
        //"Aliquot" -> Seq(), NOTE: this is commented out because the db query doesn't return types that have no attributes.
        "Pair" -> Seq("case", "control", "whatsit"),
        "SampleSet" -> Seq("samples", "hasSamples"),
        "PairSet" -> Seq("pairs"),
        "Individual" -> Seq("sset")
      )

      //assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
      //so we test the existence of all keys correctly here...
      val testTypesAndAttrNames = runAndWait(entityQuery.getAttrNamesAndEntityTypes(context))
      assertSameElements(testTypesAndAttrNames.keys, desiredTypesAndAttrNames.keys)

      desiredTypesAndAttrNames foreach { case (eType, attrNames) =>
        //...and handle that the values are all correct here.
        assertSameElements(testTypesAndAttrNames(eType), attrNames)
      }
    }
  }

  it should "list all entity type metadata" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>

      val desiredTypeMetadata = Map[String, EntityTypeMetadata](
        "Sample" -> EntityTypeMetadata(8, "sample_id", Seq("type", "whatsit", "thingies", "quot", "somefoo", "tumortype", "confused", "cycle", "foo_id")),
        "Aliquot" -> EntityTypeMetadata(2, "aliquot_id", Seq()),
        "Pair" -> EntityTypeMetadata(2, "pair_id", Seq("case", "control", "whatsit")),
        "SampleSet" -> EntityTypeMetadata(5, "sampleset_id", Seq("samples", "hasSamples")),
        "PairSet" -> EntityTypeMetadata(1, "pairset_id", Seq("pairs")),
        "Individual" -> EntityTypeMetadata(2, "individual_id", Seq("sset"))
      )

      //assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
      //so we test the existence of all keys correctly here...
      val testTypeMetadata = runAndWait(entityQuery.getEntityTypeMetadata(context))
      assertSameElements(testTypeMetadata.keys, desiredTypeMetadata.keys)

      testTypeMetadata foreach { case (eType, testMetadata) =>
        val desiredMetadata = desiredTypeMetadata(eType)

        //...and test that count and the list of attribute names are correct here.
        assert(testMetadata.count == desiredMetadata.count)
        assertSameElements(testMetadata.attributeNames, desiredMetadata.attributeNames)
      }
    }
  }

  // GAWB-870
  val testWorkspace = new EmptyWorkspace
  it should "list all entity type metadata when all_attribute_values is null" in withCustomTestDatabase(testWorkspace) { dataSource =>
    withWorkspaceContext(testWorkspace.workspace) { context =>

      val id1 = 1
      val id2 = 2   // arbitrary

      // count distinct misses rows with null columns, like this one
      runAndWait(entityQuery += EntityRecord(id1, "test1", "null_attrs_type", context.workspaceId, 0, None, deleted = false, None))

      runAndWait(entityQuery += EntityRecord(id2, "test2", "blank_attrs_type", context.workspaceId, 0, Some(""), deleted = false, None))

      val desiredTypeMetadata = Map[String, EntityTypeMetadata](
        "null_attrs_type" -> EntityTypeMetadata(1, "null_attrs_type_id", Seq()),
        "blank_attrs_type" -> EntityTypeMetadata(1, "blank_attrs_type", Seq())
      )

      //assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
      //so we test the existence of all keys correctly here...
      val testTypeMetadata = runAndWait(entityQuery.getEntityTypeMetadata(context))
      assertSameElements(testTypeMetadata.keys, desiredTypeMetadata.keys)

      testTypeMetadata foreach { case (eType, testMetadata) =>
        val desiredMetadata = desiredTypeMetadata(eType)

        //...and test that count and the list of attribute names are correct here.
        assert(testMetadata.count == desiredMetadata.count)
        assertSameElements(testMetadata.attributeNames, desiredMetadata.attributeNames)
      }
    }
  }


  it should "trim giant all_attribute_values strings so they don't overflow" in withCustomTestDatabase(testWorkspace) { dataSource =>
    //it'll be longer than this (and thus will need trimming) because it'll get the entity name too
    val veryLongString = "a" * EntityComponent.allAttributeValuesColumnSize
    val sample1 = Entity("sample1", "Sample",
      Map(AttributeName.withDefaultNS("veryLongString") -> AttributeString(veryLongString)))
    withWorkspaceContext(testWorkspace.workspace) { context =>
      runAndWait(entityQuery.save(context, sample1))

      val entityRec = runAndWait(uniqueResult(entityQuery.findEntityByName(UUID.fromString(testWorkspace.workspace.workspaceId), "Sample", "sample1").result))
      assertResult(EntityComponent.allAttributeValuesColumnSize) {
        entityRec.get.allAttributeValues.get.length
      }
    }
  }

  class BugTestData extends TestData {
    val wsName = WorkspaceName("myNamespace2", "myWorkspace2")
    val workspace = new Workspace(wsName.namespace, wsName.name, Set.empty, UUID.randomUUID.toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty, Map.empty, Map.empty)

    val sample1 = new Entity("sample1", "Sample",
      Map(AttributeName.withDefaultNS("aliquot") -> AttributeEntityReference("Aliquot", "aliquot1")))

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)

    override def save() = {
      DBIOAction.seq(
        workspaceQuery.save(workspace),
        entityQuery.save(SlickWorkspaceContext(workspace), aliquot1),
        entityQuery.save(SlickWorkspaceContext(workspace), sample1))
    }

  }

  val bugData = new BugTestData

  it should "get an entity with attribute ref name same as an entity, but different case" in withCustomTestDatabaseInternal(bugData) {

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
        val pair2 = Entity("pair2", "Pair",
          Map(
            AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
            AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")))
        runAndWait(entityQuery.save(context, pair2))
        assert {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "Pair", "pair2")).isDefined
        }
      }

  }

  it should "update an entity's attributes many times concurrently" in withDefaultTestDatabase {
    val pair2 = Entity("pair2", "Pair",
      Map(
        AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")))

    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "Pair", "pair2")).isDefined
      }
    }

    withWorkspaceContext(testData.workspace) { context =>
      val count = 20
      runMultipleAndWait(count)(_ => entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "Pair", "pair2")).isDefined
      }
      assertResult(count+1) {
        runAndWait(entityQuery.findEntityByName(SlickWorkspaceContext(testData.workspace).workspaceId, "Pair", "pair2").map(_.version).result).head
      }
    }
  }

  it should "clone all entities from a workspace containing cycles" in withDefaultTestDatabase {
    val workspaceOriginal = Workspace(
      namespace = testData.wsName.namespace + "Original",
      name = testData.wsName.name + "Original",
      Set.empty,
      workspaceId = UUID.randomUUID.toString,
      bucketName = "aBucket",
      createdDate = currentTime(),
      lastModified = currentTime(),
      createdBy = "Joe Biden",
      Map.empty,
      Map.empty,
      Map.empty
    )

    val workspaceClone = Workspace(
      namespace = testData.wsName.namespace + "Clone",
      name = testData.wsName.name + "Clone",
      authorizationDomain = Set.empty,
      workspaceId = UUID.randomUUID.toString,
      bucketName = "anotherBucket",
      createdDate = currentTime(),
      lastModified = currentTime(),
      createdBy = "Joe Biden",
      Map.empty,
      Map.empty,
      Map.empty
    )


      val c1 = Entity("c1", "samples", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("cycle1") -> AttributeEntityReference("samples", "c2")))
      val c2 = Entity("c2", "samples", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("cycle2") -> AttributeEntityReference("samples", "c3")))
      val c3 = Entity("c3", "samples", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3)))

      runAndWait(workspaceQuery.save(workspaceOriginal))
      runAndWait(workspaceQuery.save(workspaceClone))

      withWorkspaceContext(workspaceOriginal) { originalContext =>
        withWorkspaceContext(workspaceClone) { cloneContext =>
          runAndWait(entityQuery.save(originalContext, c3))
          runAndWait(entityQuery.save(originalContext, c2))
          runAndWait(entityQuery.save(originalContext, c1))

          val c3_updated = Entity("c3", "samples", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("cycle3") -> AttributeEntityReference("samples", "c1")))

          runAndWait(entityQuery.save(originalContext, c3_updated))
          runAndWait(entityQuery.copyAllEntities(originalContext, cloneContext))

          val expectedEntities = Set(c1, c2, c3_updated)
          assertResult(expectedEntities) {
            runAndWait(entityQuery.listActiveEntities(originalContext)).toSet
          }
          assertResult(expectedEntities) {
            runAndWait(entityQuery.listActiveEntities(cloneContext)).toSet
          }
        }
      }

  }

  it should "throw an exception if trying to save invalid references" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        val baz = Entity("wig", "wug", Map(AttributeName.withDefaultNS("edgeToNowhere") -> AttributeEntityReference("sample", "notTheSampleYoureLookingFor")))
        intercept[RawlsException] {
          runAndWait(entityQuery.save(context, baz))
        }
      }

  }

  it should "list entities" in withConstantTestDatabase {
    val expected = Seq(constantData.sample1,
      constantData.sample2,
      constantData.sample3,
      constantData.sample4,
      constantData.sample5,
      constantData.sample6,
      constantData.sample7,
      constantData.sample8)

    withWorkspaceContext(constantData.workspace) { context =>
      assertSameElements(expected, runAndWait(entityQuery.listActiveEntitiesOfType(context, "Sample")))
    }
  }

  it should "add cycles to entity graph" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        val sample1Copy = Entity("sample1", "Sample",
          Map(
            AttributeName.withDefaultNS("type") -> AttributeString("normal"),
            AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
            AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            AttributeName.withDefaultNS("aliquot") -> AttributeEntityReference("Aliquot", "aliquot1"),
            AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset1")))
        runAndWait(entityQuery.save(context, sample1Copy))
        val sample5Copy = Entity("sample5", "Sample",
          Map(
            AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
            AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
            AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset4")))
        runAndWait(entityQuery.save(context, sample5Copy))
        val sample7Copy = Entity("sample7", "Sample",
          Map(
            AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
            AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
            AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("Sample", "sample6")))
        runAndWait(entityQuery.save(context, sample7Copy))
        val sample6Copy = Entity("sample6", "Sample",
          Map(
            AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
            AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
            AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("SampleSet", "sset3")))
        runAndWait(entityQuery.save(context, sample6Copy))
        val entitiesWithCycles = List(sample1Copy, sample5Copy, sample7Copy, sample6Copy)
        entitiesWithCycles.foreach( entity =>
          assertResult(Option(entity)) { runAndWait(entityQuery.get(context, entity.entityType, entity.name)) }
        )
      }

  }

  it should "rename an entity" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Option(testData.pair1)) { runAndWait(entityQuery.get(context, "Pair", "pair1")) }
        assertResult(1) { runAndWait(entityQuery.rename(context, "Pair", "pair1", "amazingPair")) }
        assertResult(None) { runAndWait(entityQuery.get(context, "Pair", "pair1")) }
        assertResult(Option(testData.pair1.copy(name = "amazingPair"))) { runAndWait(entityQuery.get(context, "Pair", "amazingPair")) }
      }

  }

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */
  it should "get entity subtrees from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val sampleSet1Paths = Seq(EntityPath(Seq(testData.sset1.toReference)),
                                EntityPath(Seq(testData.sset1.toReference, testData.sample1.toReference)),
                                EntityPath(Seq(testData.sset1.toReference, testData.sample1.toReference, testData.aliquot1.toReference)),
                                EntityPath(Seq(testData.sset1.toReference, testData.sample2.toReference)),
                                EntityPath(Seq(testData.sset1.toReference, testData.sample3.toReference)),
                                EntityPath(Seq(testData.sset1.toReference, testData.sample3.toReference, testData.sample1.toReference)))
      val sampleSet2Paths = Seq(EntityPath(Seq(testData.sset2.toReference)),
                                EntityPath(Seq(testData.sset2.toReference, testData.sample2.toReference)))
      val sampleSet3Paths = Seq(EntityPath(Seq(testData.sset3.toReference)),
                                EntityPath(Seq(testData.sset3.toReference, testData.sample5.toReference)),
                                EntityPath(Seq(testData.sset3.toReference, testData.sample6.toReference)))

      val expected = sampleSet1Paths ++ sampleSet2Paths ++ sampleSet3Paths
      assertSameElements(expected, runAndWait(entityQuery.getEntitySubtrees(context, "SampleSet", Set("sset1", "sset2", "sset3", "sampleSetDOESNTEXIST"))))

      val individual2Paths = Seq(EntityPath(Seq(testData.indiv2.toReference)),
                                 EntityPath(Seq(testData.indiv2.toReference, testData.sset2.toReference)),
                                 EntityPath(Seq(testData.indiv2.toReference, testData.sset2.toReference, testData.sample2.toReference)))
      assertSameElements(individual2Paths, runAndWait(entityQuery.getEntitySubtrees(context, "Individual", Set("indiv2"))))
    }
  }

  // the opposite of the above traversal: get all reference to these entities, traversing upward

  it should "get the full set of entity references from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val expected = Set(testData.sample1, testData.sample3, testData.pair1, testData.pair2, testData.sset1, testData.ps1, testData.indiv1).map(_.toReference)
      assertSameElements(expected, runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.sample1.toReference))))

      val expected2 = Set(testData.aliquot1, testData.aliquot2, testData.sample1, testData.sample3, testData.pair1, testData.pair2, testData.sset1, testData.ps1, testData.indiv1).map(_.toReference)
      assertSameElements(expected2, runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.aliquot1.toReference, testData.aliquot2.toReference))))
    }
  }

  it should "not include deleted entities when getting the full set of entity references from a list of entities" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      runAndWait(entityQuery.hide(context, Seq(testData.indiv1.toReference, testData.pair2.toReference)))

      val expected = Set(testData.sample1, testData.sample3, testData.pair1, testData.sset1, testData.ps1).map(_.toReference)
      assertSameElements(expected, runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.sample1.toReference))))

      val expected2 = Set(testData.aliquot1, testData.aliquot2, testData.sample1, testData.sample3, testData.pair1, testData.sset1, testData.ps1).map(_.toReference)
      assertSameElements(expected2, runAndWait(entityQuery.getAllReferringEntities(context, Set(testData.aliquot1.toReference, testData.aliquot2.toReference))))
    }
  }

  val x1 = Entity("x1", "SampleSet", Map(AttributeName.withDefaultNS("child") -> AttributeEntityReference("SampleSet", "x2")))
  val x2 = Entity("x2", "SampleSet", Map.empty)

  val workspace2 = Workspace(
    namespace = testData.wsName.namespace + "2",
    name = testData.wsName.name + "2",
    Set.empty,
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty,
    Map.empty,
    Map.empty
  )

  val workspace3 = Workspace(
    namespace = testData.wsName.namespace + "3",
    name = testData.wsName.name + "3",
    Set.empty,
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty,
    Map.empty,
    Map.empty
  )

  it should "copy entities without a conflict" in withDefaultTestDatabase {
    runAndWait(workspaceQuery.save(workspace2))
    withWorkspaceContext(testData.workspace) { context1 =>
      withWorkspaceContext(workspace2) { context2 =>
        runAndWait(entityQuery.save(context2, x2))
        runAndWait(entityQuery.save(context2, x1))
        val x2_updated = Entity("x2", "SampleSet", Map(AttributeName.withDefaultNS("child") -> AttributeEntityReference("SampleSet", "x1")))
        runAndWait(entityQuery.save(context2, x2_updated))

        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context2, "SampleSet")).toList.contains(x1))
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context2, "SampleSet")).toList.contains(x2_updated))

        // note: we're copying FROM workspace2 INTO workspace
        assertResult(Seq.empty) {
          runAndWait(entityQuery.getCopyConflicts(context1, Seq(x1, x2_updated).map(_.toReference)))
        }

        assertSameElements(Seq(x1.toReference, x2.toReference), runAndWait(entityQuery.checkAndCopyEntities(context2, context1, "SampleSet", Seq("x2"), false)).entitiesCopied)

        //verify it was actually copied into the workspace
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context1, "SampleSet")).toList.contains(x1))
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context1, "SampleSet")).toList.contains(x2_updated))
      }
    }
  }

  it should "copy entities without a conflict with a cycle" in withDefaultTestDatabase {

    runAndWait(workspaceQuery.save(workspace2))
    withWorkspaceContext(testData.workspace) { context1 =>
      withWorkspaceContext(workspace2) { context2 =>
        val a = Entity("a", "test", Map.empty)
        val a6 = Entity("a6", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a1")))
        val a5 = Entity("a5", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a6")))
        val a4 = Entity("a4", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a5")))
        val a3 = Entity("a3", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a4"), AttributeName.withDefaultNS("side") -> AttributeEntityReference("test", "a")))
        val a2 = Entity("a2", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a3")))
        val a1 = Entity("a1", "test", Map(AttributeName.withDefaultNS("next") -> AttributeEntityReference("test", "a2")))
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

        assertSameElements(allEntities.map(_.toReference), runAndWait(entityQuery.checkAndCopyEntities(context2, context1, "test", Seq("a1"), false)).entitiesCopied)

        //verify it was actually copied into the workspace
        assertSameElements(allEntities, runAndWait(entityQuery.listActiveEntitiesOfType(context1, "test")).toSet)
      }
    }

  }

  it should "copy entities with a conflict" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Set(testData.sample1.toReference)) {
          runAndWait(entityQuery.getCopyConflicts(context, Seq(testData.sample1).map(_.toReference))).map(_.toReference).toSet
        }

        assertResult(Set(EntityHardConflict(testData.sample1.entityType, testData.sample1.name))) {
          runAndWait(entityQuery.checkAndCopyEntities(context, context, "Sample", Seq("sample1"), false)).hardConflicts.toSet
        }

        //verify that it wasn't copied into the workspace again
        assert(runAndWait(entityQuery.listActiveEntitiesOfType(context, "Sample")).toList.filter(entity => entity == testData.sample1).size == 1)
      }

  }

  it should "copy entities with a conflict in the entity subtrees and properly link already existing entities" in withDefaultTestDatabase {

    runAndWait(workspaceQuery.save(workspace2))
    runAndWait(workspaceQuery.save(workspace3))
    withWorkspaceContext(workspace2) { context2 =>
      withWorkspaceContext(workspace3) { context3 =>
        val participant1 = Entity("participant1", "participant", Map.empty)
        val sample1 = Entity("sample1", "sample", Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "participant1")))

        runAndWait(entityQuery.save(context2, participant1))
        runAndWait(entityQuery.save(context3, participant1))
        runAndWait(entityQuery.save(context3, sample1))

        assertResult(List()){
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "sample")).toList
        }

        assertResult(List(participant1)){
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "participant")).toList
        }

        runAndWait(entityQuery.checkAndCopyEntities(context3, context2, "sample", Seq("sample1"), true))

        assertResult(List(sample1)){
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "sample")).toList
        }

        assertResult(List(participant1)){
          runAndWait(entityQuery.listActiveEntitiesOfType(context2, "participant")).toList
        }

      }
    }
  }

  it should "fail when putting dots in user-specified strings" in withDefaultTestDatabase {
    val dottyName = Entity("dotty.name", "Sample", Map.empty)
    val dottyType = Entity("dottyType", "Sam.ple", Map.empty)
    val dottyAttr = Entity("dottyAttr", "Sample", Map(AttributeName.withDefaultNS("foo.bar") -> AttributeBoolean(true)))
    val dottyAttr2 = Entity("dottyAttr", "Sample", Map(AttributeName("library", "foo.bar") -> AttributeBoolean(true)))

      withWorkspaceContext(testData.workspace) { context =>
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyName)) }
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyType)) }
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyAttr)) }
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyAttr2)) }
      }

  }

  Attributable.reservedAttributeNames.foreach { reserved =>
    AttributeName.validNamespaces.foreach { namespace =>
      it should s"fail using reserved attribute name $reserved in namespace $namespace" in withDefaultTestDatabase {
        val e = Entity("test_sample", "Sample", Map(AttributeName(namespace, reserved) -> AttributeString("foo")))

        withWorkspaceContext(testData.workspace) { context =>
          intercept[RawlsException] {
            runAndWait(entityQuery.save(context, e))
          }
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
    val workspace: Workspace = Workspace("test_namespace", workspaceId.toString, Set.empty, workspaceId.toString, "bucketname", currentTime(), currentTime(), "me", Map.empty, Map.empty, Map.empty, false)
    runAndWait(workspaceQuery.save(workspace))
    val workspaceContext = SlickWorkspaceContext(workspace)

    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val entity = Entity("name", "type", Map.empty)

    assertResult(entity) { runAndWait(entityQuery.save(workspaceContext, entity)) }
    assertResult(Some(entity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val oldId = runAndWait(entityQuery.findEntityByName(workspaceId, "type", "name").result).head.id

    assertResult(1) { runAndWait(entityQuery.hide(workspaceContext, Seq(entity.toReference))) }
    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    assertResult(entity) { runAndWait(entityQuery.save(workspaceContext, entity)) }
    assertResult(Some(entity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val newId = runAndWait(entityQuery.findEntityByName(workspaceId, "type", "name").result).head.id

    assert(oldId != newId)
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

      val bob = Entity("Bob", "Individual", Map(AttributeName.withDefaultNS("alive") -> AttributeBoolean(false), AttributeName.withDefaultNS("sampleSite") -> AttributeString("head")))
      val blood = Entity("Bob-Blood", "Sample", Map(AttributeName.withDefaultNS("indiv") -> bob.toReference, AttributeName.withDefaultNS("color") -> AttributeString("red")))
      val bone = Entity("Bob-Bone", "Sample", Map(AttributeName.withDefaultNS("indiv") -> bob.toReference, AttributeName.withDefaultNS("color") -> AttributeString("white")))

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

  it should "not select the all_attribute_values column when using entityQuery.withoutAllAttributes" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>

      val hasAttrs = Entity("entityWithAttrs", "Pair", Map(
        AttributeName.withDefaultNS("attrOne") -> AttributeString("one"),
        AttributeName.withDefaultNS("attrTwo") -> AttributeString("two"),
        AttributeName.withDefaultNS("attrThree") -> AttributeString("three"),
        AttributeName.withDefaultNS("attrFour") -> AttributeString("four")
      ))

      runAndWait(entityQuery.save(context, hasAttrs))

      val recWithAttrs = runAndWait(entityQuery
        .filter(e => e.name === "entityWithAttrs" && e.entityType === "Pair")
          .result.headOption)

      assert(recWithAttrs.isDefined, "entityQuery should find the record")
      assertResult(Some(Some("entitywithattrs one two three four")), "entityQuery should return allAttributeValues") {
        recWithAttrs.map(_.allAttributeValues)
      }
      import entityQuery.EntityRecordLightShape

      val recWithoutAttrs = runAndWait(entityQuery.withoutAllAttributeValues
        .filter(e => e.name === "entityWithAttrs" && e.entityType === "Pair")
        .result.headOption)

      assert(recWithoutAttrs.isDefined, "entityQuery.withoutAllAttributeValues should find the record")
      assertResult(Some(None), "entityQueryLight should not return allAttributeValues") {
        recWithoutAttrs.map(_.allAttributeValues)
      }

      assertResult(recWithoutAttrs, "entityQuery and entityQuery.withoutAllAttributeValues should return the same record otherwise") {
        recWithAttrs.map(_.copy(allAttributeValues = None))
      }
    }
  }

  it should "gather entity statistics for single namespace/name" in withDefaultTestDatabase {
    val entityStats = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(testData.wsName.namespace), Some(testData.wsName.name)))
    val expected = Set(
      NamedStatistic("Aliquot", 2),
      NamedStatistic("Sample", 8),
      NamedStatistic("Pair", 2),
      NamedStatistic("PairSet", 1),
      NamedStatistic("SampleSet", 5),
      NamedStatistic("Individual", 2)
    )
    assertResult(expected)(entityStats.toSet)
  }

  it should "gather entity statistics for an entire namespace, without a name" in withDefaultTestDatabase {
    val expected = Set(
      NamedStatistic("Aliquot", 16),
      NamedStatistic("Sample", 66),
      NamedStatistic("Pair", 16),
      NamedStatistic("PairSet", 8),
      NamedStatistic("SampleSet", 40),
      NamedStatistic("Individual", 16)
    )
    val entityStats1 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(testData.billingProject.projectName.value), None))
    assertResult(expected)(entityStats1.toSet)
    // add two sample sets to a different namespace
    runAndWait(workspaceQuery.save(workspace2))
    withWorkspaceContext(workspace2) { wsctx =>
      runAndWait(entityQuery.save(wsctx, x2))
      runAndWait(entityQuery.save(wsctx, x1))
    }
    // not found in original namespace
    val entityStats2 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(testData.billingProject.projectName.value), None))
    assertResult(expected)(entityStats2.toSet)
    // yes found in new namespace
    val entityStats3 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(workspace2.namespace), None))
    assertResult(Set(NamedStatistic("SampleSet", 2)))(entityStats3.toSet)
  }

  it should "gather entity statistics for a name, across all namespaces" in withDefaultTestDatabase {
    val entityStats1 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(None, Some(testData.wsName.name)))
    val expected1 = Set(
      NamedStatistic("Aliquot", 2),
      NamedStatistic("Sample", 8),
      NamedStatistic("Pair", 2),
      NamedStatistic("PairSet", 1),
      NamedStatistic("SampleSet", 5),
      NamedStatistic("Individual", 2)
    )
    assertResult(expected1)(entityStats1.toSet)
    // add two sample sets in a different namespace
    val wsOtherNamespace = testData.workspace.copy(namespace="somethingelse", workspaceId = UUID.randomUUID.toString)
    runAndWait(workspaceQuery.save(wsOtherNamespace))
    withWorkspaceContext(wsOtherNamespace) { wsctx =>
      runAndWait(entityQuery.save(wsctx, x2))
      runAndWait(entityQuery.save(wsctx, x1))
    }
    val entityStats2 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(None, Some(testData.wsName.name)))
    val expected2 = Set(
      NamedStatistic("Aliquot", 2),
      NamedStatistic("Sample", 8),
      NamedStatistic("Pair", 2),
      NamedStatistic("PairSet", 1),
      NamedStatistic("SampleSet", 7),
      NamedStatistic("Individual", 2)
    )
    assertResult(expected2)(entityStats2.toSet)
  }

  it should "gather entity statistics for the entire corpus, across all namespace/names" in withDefaultTestDatabase {
    // add two sample sets in a different namespace
    runAndWait(workspaceQuery.save(workspace2))
    withWorkspaceContext(workspace2) { wsctx =>
      runAndWait(entityQuery.save(wsctx, x2))
      runAndWait(entityQuery.save(wsctx, x1))
    }
    val entityStats = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(None, None))
    val expected = Set(
      NamedStatistic("Aliquot", 16),
      NamedStatistic("Sample", 66),
      NamedStatistic("Pair", 16),
      NamedStatistic("PairSet", 8),
      NamedStatistic("SampleSet", 42),
      NamedStatistic("Individual", 16)
    )
    assertResult(expected)(entityStats.toSet)
  }

  it should "not include deleted entities in entity statistics " in withDefaultTestDatabase {
    // save two sample sets
    withWorkspaceContext(testData.workspace) { wsctx =>
      runAndWait(entityQuery.save(wsctx, x2))
      runAndWait(entityQuery.save(wsctx, x1))
    }
    val entityStats1 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(testData.wsName.namespace), Some(testData.wsName.name)))
    val expected1 = Set(
      NamedStatistic("Aliquot", 2),
      NamedStatistic("Sample", 8),
      NamedStatistic("Pair", 2),
      NamedStatistic("PairSet", 1),
      NamedStatistic("SampleSet", 7),
      NamedStatistic("Individual", 2)
    )
    assertResult(expected1)(entityStats1.toSet)

    // now delete those two sample sets
    withWorkspaceContext(testData.workspace) { wsctx =>
      runAndWait(entityQuery.hide(wsctx, Seq(x1, x2).map(x => AttributeEntityReference(x.entityType, x.name))))
    }
    val entityStats2 = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some(testData.wsName.namespace), Some(testData.wsName.name)))
    val expected2 = Set(
      NamedStatistic("Aliquot", 2),
      NamedStatistic("Sample", 8),
      NamedStatistic("Pair", 2),
      NamedStatistic("PairSet", 1),
      NamedStatistic("SampleSet", 5),
      NamedStatistic("Individual", 2)
    )
    assertResult(expected2)(entityStats2.toSet)
  }

  it should "return empty list for entity statistics when namespace/name matches nothing" in withDefaultTestDatabase {
    val entityStats = runAndWait(entityQuery.EntityStatisticsQueries.countEntitiesofTypeInNamespace(Some("thisNamespaceDoesntExist"), None))
    val expected = Set.empty[NamedStatistic]
    assertResult(expected)(entityStats.toSet)
  }

}
