package org.broadinstitute.dsde.rawls.dataaccess.slick

import _root_.slick.dbio.DBIOAction
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.rawls.model.{AttributeName, _}
import org.broadinstitute.dsde.rawls.{model, RawlsException, RawlsTestUtils}

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util.UUID

/**
 * Created by dvoet on 2/12/16.
 */
class EntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils with RawSqlQuery {
  import driver.api._

  // entity and attribute counts, regardless of deleted status
  def countEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.UnitTestHelpers.listEntities(workspace))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  // entity and attribute counts, non-deleted only
  def countActiveEntitiesAttrs(workspace: Workspace): (Int, Int) = {
    val ents = runAndWait(entityQuery.listActiveEntities(workspace))
    (ents.size, ents.map(_.attributes.size).sum)
  }

  "EntityComponent" should "crud entities" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace(
      "test_namespace",
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

    assertResult(entityCount3)(runAndWait(entityQuery.deleteEntitiesAndAttributesFromDb(workspaceContext)))
    assertResult(None)(runAndWait(entityQuery.get(workspaceContext, "type", "delete-me")))
    assertResult(0)(runAndWait(entityQuery.deleteEntitiesAndAttributesFromDb(workspaceContext)))

    val (entityCount4, attributeCount4) = countEntitiesAttrs(workspace)
    val (activeEntityCount4, activeAttributeCount4) = countActiveEntitiesAttrs(workspace)

    assertResult(0)(entityCount4)
    assertResult(0)(attributeCount4)
    assertResult(0)(activeEntityCount4)
    assertResult(0)(activeAttributeCount4)
  }

  it should "return false if the entity type does not exist" in withMinimalTestDatabase { _ =>
    withWorkspaceContext(legacyTestData.workspace) { context =>
      val pair2EntityTypeExists = runAndWait(entityQuery.doesEntityTypeAlreadyExist(context, "Pair2")).get
      assert(!pair2EntityTypeExists)
    }
  }

  it should "list all entities of all entity types" in withConstantTestDatabase {
    withWorkspaceContext(constantData.workspace) { context =>
      assertSameElements(constantData.allEntities, runAndWait(entityQuery.listActiveEntities(context)))
    }
  }

  val testWorkspace = new EmptyWorkspace

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

  it should "get an entity" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
      assertResult(Some(legacyTestData.pair1)) {
        runAndWait(entityQuery.get(context, "Pair", "pair1"))
      }
      assertResult(Some(legacyTestData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(legacyTestData.sset1)) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
    }

  }

  it should "return None when an entity does not exist" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
      assertResult(None) {
        runAndWait(entityQuery.get(context, "pair", "fnord"))
      }
      assertResult(None) {
        runAndWait(entityQuery.get(context, "fnord", "pair1"))
      }
    }

  }

  it should "save a new entity" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
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
        runAndWait(entityQuery.get(legacyTestData.workspace, "Pair", "pair2")).isDefined
      }
    }

  }

  it should "update a workspace's lastModified date when saving an entity" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
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
        runAndWait(entityQuery.get(legacyTestData.workspace, "Pair", "pair2")).isDefined
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

  it should "not re-update an entity's attributes over many writes if attribute do not change" in withLegacyDefaultTestDatabase {
    val pair2 = Entity(
      "pair2",
      "Pair",
      Map(
        AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1")
      )
    )

    withWorkspaceContext(legacyTestData.workspace) { context =>
      runAndWait(entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(legacyTestData.workspace, "Pair", "pair2")).isDefined
      }
    }

    withWorkspaceContext(legacyTestData.workspace) { context =>
      val count = 20
      runMultipleAndWait(count)(_ => entityQuery.save(context, pair2))
      assert {
        runAndWait(entityQuery.get(legacyTestData.workspace, "Pair", "pair2")).isDefined
      }
      assertResult(0) { // the additional writes should not increment this entity's version
        runAndWait(
          entityQuery
            .findEntityByName(legacyTestData.workspace.workspaceIdAsUUID, "Pair", "pair2")
            .map(_.version)
            .result
        ).head
      }
    }
  }

  it should "update an entity's attributes many times concurrently if attributes change" in withLegacyDefaultTestDatabase {
    def makeEntity(idx: Int): Entity =
      Entity("some-sample",
             "Sample",
             Map(
               AttributeName.withDefaultNS("indexValue") -> AttributeString(s"index-$idx")
             )
      )

    withWorkspaceContext(legacyTestData.workspace) { context =>
      runAndWait(entityQuery.save(context, makeEntity(0)))

      // did we save the entity?
      val entityWithAllAttrs = runAndWait(
        entityQuery
          .findEntityByName(legacyTestData.workspace.workspaceIdAsUUID, "Sample", "some-sample")
          .result
      )
      entityWithAllAttrs should have length 1
      entityWithAllAttrs.head.recordVersion shouldBe 0
    }

    withWorkspaceContext(legacyTestData.workspace) { context =>
      val count = 20
      (1 to count) foreach { idx =>
        runAndWait(entityQuery.save(context, makeEntity(idx)))
      }

      // did we update the record versions?
      val entityWithAllAttrs = runAndWait(
        entityQuery
          .findEntityByName(legacyTestData.workspace.workspaceIdAsUUID, "Sample", "some-sample")
          .result
      )
      entityWithAllAttrs should have length 1
      entityWithAllAttrs.head.recordVersion shouldBe count
    }
  }

  it should "throw an exception if trying to save invalid references" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
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
      assertSameElements(expected, runAndWait(entityQuery.UnitTestHelpers.listActiveEntitiesOfType(context, "Sample")))
    }
  }

  it should "add cycles to entity graph" in withLegacyDefaultTestDatabase {

    withWorkspaceContext(legacyTestData.workspace) { context =>
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

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */

  val x1 =
    Entity("x1", "SampleSet", Map(AttributeName.withDefaultNS("child") -> AttributeEntityReference("SampleSet", "x2")))
  val x2 = Entity("x2", "SampleSet", Map.empty)

  val workspace2 = Workspace(
    namespace = legacyTestData.wsName.namespace + "2",
    name = legacyTestData.wsName.name + "2",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty
  )

  val workspace3 = Workspace(
    namespace = legacyTestData.wsName.namespace + "3",
    name = legacyTestData.wsName.name + "3",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = currentTime(),
    lastModified = currentTime(),
    createdBy = "Joe Biden",
    Map.empty
  )

  it should "fail when putting dots in user-specified strings" in withLegacyDefaultTestDatabase {
    // NB: entity names allow dots, so entity names are not included in this test
    val dottyType = Entity("dottyType", "Sam.ple", Map.empty)
    val dottyAttr = Entity("dottyAttr", "Sample", Map(AttributeName.withDefaultNS("foo.bar") -> AttributeBoolean(true)))
    val dottyAttr2 = Entity("dottyAttr", "Sample", Map(AttributeName("library", "foo.bar") -> AttributeBoolean(true)))

    withWorkspaceContext(legacyTestData.workspace) { context =>
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyType)))
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyAttr)))
      intercept[RawlsException](runAndWait(entityQuery.save(context, dottyAttr2)))
    }

  }

  Attributable.reservedAttributeNames.foreach { reserved =>
    it should s"fail using reserved attribute name ${reserved.name} in default namespace" in withLegacyDefaultTestDatabase {
      val e = Entity("test_sample", "Sample", Map(reserved -> AttributeString("foo")))

      withWorkspaceContext(legacyTestData.workspace) { context =>
        intercept[RawlsException] {
          runAndWait(entityQuery.save(context, e))
        }
      }
    }

    AttributeName.validNamespaces.-(AttributeName.defaultNamespace).foreach { namespace =>
      it should s"succeed using reserved attribute name ${reserved.name} in namespace $namespace" in withLegacyDefaultTestDatabase {
        val e = Entity("test_sample", "Sample", Map(AttributeName(namespace, reserved.name) -> AttributeString("foo")))

        withWorkspaceContext(legacyTestData.workspace) { context =>
          runAndWait(entityQuery.save(context, e))
        }
        assert {
          runAndWait(entityQuery.get(legacyTestData.workspace, "Sample", "test_sample")).isDefined
        }
      }
    }
  }

  it should s"fail using reserved attribute name sample_id in namespace default for sample entity type" in withLegacyDefaultTestDatabase {
    val e = Entity("test_sample", "Sample", Map(AttributeName.withDefaultNS("sample_id") -> AttributeString("foo")))

    withWorkspaceContext(legacyTestData.workspace) { context =>
      intercept[RawlsException] {
        runAndWait(entityQuery.save(context, e))
      }
    }
  }

  it should "save a new entity with the same name as a deleted entity" in withLegacyDefaultTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace(
      "test_namespace",
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

  it should "delete an entity type and return the total number of rows deleted (hidden)" in withLegacyDefaultTestDatabase {
    withWorkspaceContext(legacyTestData.workspace) { context =>
      val sampleCount =
        runAndWait(entityQuery.UnitTestHelpers.listActiveEntitiesOfType(context, "sample")).iterator.size

      assertResult(sampleCount) {
        runAndWait(entityQuery.hideType(context, "sample"))
      }
    }
  }

  it should "delete a set without affecting its component entities" in withLegacyDefaultTestDatabase {
    withWorkspaceContext(legacyTestData.workspace) { context =>
      assertResult(Some(legacyTestData.sset1)) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
      assertResult(Some(legacyTestData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(legacyTestData.sample2)) {
        runAndWait(entityQuery.get(context, "Sample", "sample2"))
      }
      assertResult(Some(legacyTestData.sample3)) {
        runAndWait(entityQuery.get(context, "Sample", "sample3"))
      }

      assertResult(1) {
        runAndWait(entityQuery.hide(context, Seq(legacyTestData.sset1.toReference)))
      }

      assertResult(None) {
        runAndWait(entityQuery.get(context, "SampleSet", "sset1"))
      }
      assertResult(Some(legacyTestData.sample1)) {
        runAndWait(entityQuery.get(context, "Sample", "sample1"))
      }
      assertResult(Some(legacyTestData.sample2)) {
        runAndWait(entityQuery.get(context, "Sample", "sample2"))
      }
      assertResult(Some(legacyTestData.sample3)) {
        runAndWait(entityQuery.get(context, "Sample", "sample3"))
      }
    }
  }

  it should "delete a sample without affecting its individual or any other entities" in withLegacyDefaultTestDatabase {
    withWorkspaceContext(legacyTestData.workspace) { context =>
      val (entityCount1, attributeCount1) = countEntitiesAttrs(legacyTestData.workspace)
      val (activeEntityCount1, activeAttributeCount1) = countActiveEntitiesAttrs(legacyTestData.workspace)

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

      val (entityCount2, attributeCount2) = countEntitiesAttrs(legacyTestData.workspace)
      val (activeEntityCount2, activeAttributeCount2) = countActiveEntitiesAttrs(legacyTestData.workspace)

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

      val (entityCount3, attributeCount3) = countEntitiesAttrs(legacyTestData.workspace)
      val (activeEntityCount3, activeAttributeCount3) = countActiveEntitiesAttrs(legacyTestData.workspace)

      assertResult(entityCount2)(entityCount3)
      assertResult(attributeCount2)(attributeCount3)
      assertResult(activeEntityCount2 - 1)(activeEntityCount3)
      assertResult(activeAttributeCount2 - 2)(activeAttributeCount3)

    }
  }

  it should "return only the selected attributes" in withLegacyDefaultTestDatabase {
    withWorkspaceContext(legacyTestData.workspace) { context =>
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

      val result = runAndWait(
        entityQuery.get(
          context,
          entityToSave.entityType,
          entityToSave.name,
          Set(testAttribute1._1, testAttribute3._1)
        )
      )

      assert(result.isDefined)
      val resAttributeNames = result.get.attributes.keys

      assert(resAttributeNames.exists(_.equalsIgnoreCase(testAttribute1._1)),
             "Attribute 1 should be returned by the filter query"
      )
      assert(resAttributeNames.exists(_.equalsIgnoreCase(testAttribute3._1)),
             "Attribute 3 should be returned by the filter query"
      )
      resAttributeNames.size shouldBe 2
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
      Entity(
        "name-3",
        "mytype",
        Map(AttributeName.withDefaultNS("case") -> AttributeString("value3"),
            AttributeName.withDefaultNS("CASE") -> AttributeString("value4")
        )
      ),
      Entity("name-4", "anothertype", Map(AttributeName.withDefaultNS("case") -> AttributeString("value5"))),
      Entity("name-5", "anothertype", Map(AttributeName.withDefaultNS("CASE") -> AttributeString("value6")))
    )
    runAndWait(entityQuery.save(context, entitiesToSave))

    assume(runAndWait(entityQuery.UnitTestHelpers.listEntities(context)).size == 5,
           "filteredCount tests did not set up fixtures correctly"
    )

  }

}
