package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import _root_.slick.dbio.DBIOAction
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by dvoet on 2/12/16.
 */
class EntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "EntityComponent" should "crud entities" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace: Workspace = Workspace("test_namespace", workspaceId.toString, workspaceId.toString, "bucketname", DateTime.now(), DateTime.now(), "me", Map.empty, Map.empty, false)
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

    val updatedEntity = entity.copy(attributes = Map("string" -> AttributeString("foo"),
      "ref" -> AttributeEntityReference(target1.entityType, target1.name),
      "refList" -> AttributeEntityReferenceList(Seq(AttributeEntityReference(target1.entityType, target1.name), AttributeEntityReference(target2.entityType, target2.name)))))

    assertResult(updatedEntity) { runAndWait(entityQuery.save(workspaceContext, updatedEntity)) }
    assertResult(Some(updatedEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    val updatedAgainEntity = updatedEntity.copy(attributes = Map("string2" -> AttributeString("foo"),
      "ref" -> AttributeEntityReference(target2.entityType, target2.name),
      "refList" -> AttributeEntityReferenceList(Seq(AttributeEntityReference(target2.entityType, target2.name), AttributeEntityReference(target1.entityType, target1.name)))))
    assertResult(updatedAgainEntity) { runAndWait(entityQuery.save(workspaceContext, updatedAgainEntity)) }
    assertResult(Some(updatedAgainEntity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    assertResult(entity) { runAndWait(entityQuery.save(workspaceContext, entity)) }
    assertResult(Some(entity)) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }

    assertResult(true) { runAndWait(entityQuery.delete(workspaceContext, "type", "name")) }
    assertResult(None) { runAndWait(entityQuery.get(workspaceContext, "type", "name")) }
    assertResult(false) { runAndWait(entityQuery.delete(workspaceContext, "type", "name")) }
  }



  it should "get entity types" in withDefaultTestDatabase { 
    
      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Set("PairSet", "Individual", "Sample", "Aliquot", "SampleSet", "Pair")) {
          runAndWait(entityQuery.getEntityTypes(context)).toSet
        }
      }

  }

  it should "correctly count entities of a certain type" in withDefaultTestDatabase {

    withWorkspaceContext(testData.workspace) { context =>

      val bernie = Entity("Bernie", "Politician", Map.empty)
      val obama = Entity("Obama", "Politician", Map.empty)
      val biden = Entity("Biden", "Politician", Map.empty)
      val trump = Entity("Trump", "Politician", Map.empty)

      runAndWait(entityQuery.save(workspaceContext, bernie))
      runAndWait(entityQuery.save(workspaceContext, obama))
      runAndWait(entityQuery.save(workspaceContext, biden))
      runAndWait(entityQuery.save(workspaceContext, trump))

      assertResult(4) {
        runAndWait(entityQuery.getEntityTypes(context)).toSet
      }
    }

  }

  it should "list all entities of all entity types" in withDefaultTestDatabase { 
    
      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty, testData.aliquot1, testData.aliquot2, testData.pair1, testData.pair2, testData.ps1, testData.indiv1, testData.aliquot1, testData.aliquot2)) {
          runAndWait(entityQuery.listEntitiesAllTypes(context)).toSet
        }
      }

  }

  class BugTestData extends TestData {
    val wsName = WorkspaceName("myNamespace2", "myWorkspace2")
    val workspace = new Workspace(wsName.namespace, wsName.name, UUID.randomUUID.toString, "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map.empty)

    val sample1 = new Entity("sample1", "Sample",
      Map("aliquot" -> AttributeEntityReference("Aliquot", "aliquot1")))

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)

    override def save() = {
      DBIOAction.seq(
        workspaceQuery.save(workspace),
        entityQuery.save(SlickWorkspaceContext(workspace), aliquot1),
        entityQuery.save(SlickWorkspaceContext(workspace), sample1))
    }

  }

  val bugData = new BugTestData

  it should "get an entity with attribute ref name same as an entity, but different case" in withCustomTestDatabase(bugData) {

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
            "case" -> AttributeEntityReference("Sample", "sample3"),
            "control" -> AttributeEntityReference("Sample", "sample1")))
        runAndWait(entityQuery.save(context, pair2))
        assert {
          runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "Pair", "pair2")).isDefined
        }
      }

  }

  it should "clone all entities from a workspace containing cycles" in withDefaultTestDatabase { 
    val workspaceOriginal = Workspace(
      namespace = testData.wsName.namespace + "Original",
      name = testData.wsName.name + "Original",
      workspaceId = UUID.randomUUID.toString,
      bucketName = "aBucket",
      createdDate = DateTime.now,
      lastModified = DateTime.now,
      createdBy = "Joe Biden",
      Map.empty,
      Map.empty
    )

    val workspaceClone = Workspace(
      namespace = testData.wsName.namespace + "Clone",
      name = testData.wsName.name + "Clone",
      workspaceId = UUID.randomUUID.toString,
      bucketName = "anotherBucket",
      createdDate = DateTime.now,
      lastModified = DateTime.now,
      createdBy = "Joe Biden",
      Map.empty,
      Map.empty
    )


      val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle1" -> AttributeEntityReference("samples", "c2")))
      val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle2" -> AttributeEntityReference("samples", "c3")))
      val c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)))

      runAndWait(workspaceQuery.save(workspaceOriginal))
      runAndWait(workspaceQuery.save(workspaceClone))

      withWorkspaceContext(workspaceOriginal) { originalContext =>
        withWorkspaceContext(workspaceClone) { cloneContext =>
          runAndWait(entityQuery.save(originalContext, c3))
          runAndWait(entityQuery.save(originalContext, c2))
          runAndWait(entityQuery.save(originalContext, c1))

          val c3_updated = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle3" -> AttributeEntityReference("samples", "c1")))

          runAndWait(entityQuery.save(originalContext, c3_updated))
          runAndWait(entityQuery.cloneAllEntities(originalContext, cloneContext))

          val expectedEntities = Set(c1, c2, c3_updated)
          assertResult(expectedEntities) {
            runAndWait(entityQuery.listEntitiesAllTypes(originalContext)).toSet
          }
          assertResult(expectedEntities) {
            runAndWait(entityQuery.listEntitiesAllTypes(cloneContext)).toSet
          }
        }
      }

  }

  it should "throw an exception if trying to save invalid references" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        val baz = Entity("wig", "wug", Map("edgeToNowhere" -> AttributeEntityReference("sample", "notTheSampleYoureLookingFor")))
        intercept[RawlsException] {
          runAndWait(entityQuery.save(context, baz))
        }
      }

  }

  it should "list entities" in withDefaultTestDatabase {
    
      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7)) {
          runAndWait(entityQuery.list(context, "Sample")).toSet
        }
      }

  }

  it should "add cycles to entity graph" in withDefaultTestDatabase { 

      withWorkspaceContext(testData.workspace) { context =>
        val sample1Copy = Entity("sample1", "Sample",
          Map(
            "type" -> AttributeString("normal"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            "aliquot" -> AttributeEntityReference("Aliquot", "aliquot1"),
            "cycle" -> AttributeEntityReference("SampleSet", "sset1")))
        runAndWait(entityQuery.save(context, sample1Copy))
        val sample5Copy = Entity("sample5", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            "cycle" -> AttributeEntityReference("SampleSet", "sset4")))
        runAndWait(entityQuery.save(context, sample5Copy))
        val sample7Copy = Entity("sample7", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            "cycle" -> AttributeEntityReference("Sample", "sample6")))
        runAndWait(entityQuery.save(context, sample7Copy))
        val sample6Copy = Entity("sample6", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
            "cycle" -> AttributeEntityReference("SampleSet", "sset3")))
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
        assertResult(Set(testData.sset3, testData.sample1, testData.sset2, testData.aliquot1, testData.sample6, testData.sset1, testData.sample2, testData.sample3, testData.sample5)) {
          runAndWait(entityQuery.getEntitySubtrees(context, "SampleSet", List("sset1", "sset2", "sset3", "sampleSetDOESNTEXIST"))).toSet
        }
      }

  }

  val x1 = Entity("x1", "SampleSet", Map("child" -> AttributeEntityReference("SampleSet", "x2")))
  val x2 = Entity("x2", "SampleSet", Map.empty)

  val workspace2 = Workspace(
    namespace = testData.wsName.namespace + "2",
    name = testData.wsName.name + "2",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    createdDate = DateTime.now,
    lastModified = DateTime.now,
    createdBy = "Joe Biden",
    Map.empty,
    Map.empty
  )

  it should "copy entities without a conflict" in withDefaultTestDatabase { 

      runAndWait(workspaceQuery.save(workspace2))
      withWorkspaceContext(testData.workspace) { context1 =>
        withWorkspaceContext(workspace2) { context2 =>
          runAndWait(entityQuery.save(context2, x2))
          runAndWait(entityQuery.save(context2, x1))
          val x2_updated = Entity("x2", "SampleSet", Map("child" -> AttributeEntityReference("SampleSet", "x1")))
          runAndWait(entityQuery.save(context2, x2_updated))

          assert(runAndWait(entityQuery.list(context2, "SampleSet")).toList.contains(x1))
          assert(runAndWait(entityQuery.list(context2, "SampleSet")).toList.contains(x2_updated))

          // note: we're copying FROM workspace2 INTO workspace
          assertResult(Seq.empty) {
            runAndWait(entityQuery.getCopyConflicts(context1, Seq(x1, x2_updated)))
          }

          assertResult(Seq.empty) {
            runAndWait(entityQuery.copyEntities(context2, context1, "SampleSet", Seq("x2")))
          }

          //verify it was actually copied into the workspace
          assert(runAndWait(entityQuery.list(context1, "SampleSet")).toList.contains(x1))
          assert(runAndWait(entityQuery.list(context1, "SampleSet")).toList.contains(x2_updated))
        }
      }

  }

  it should "copy entities without a conflict with a cycle" in withDefaultTestDatabase {

    runAndWait(workspaceQuery.save(workspace2))
    withWorkspaceContext(testData.workspace) { context1 =>
      withWorkspaceContext(workspace2) { context2 =>
        val a = Entity("a", "test", Map.empty)
        val a6 = Entity("a6", "test", Map("next" -> AttributeEntityReference("test", "a1")))
        val a5 = Entity("a5", "test", Map("next" -> AttributeEntityReference("test", "a6")))
        val a4 = Entity("a4", "test", Map("next" -> AttributeEntityReference("test", "a5")))
        val a3 = Entity("a3", "test", Map("next" -> AttributeEntityReference("test", "a4"), "side" -> AttributeEntityReference("test", "a")))
        val a2 = Entity("a2", "test", Map("next" -> AttributeEntityReference("test", "a3")))
        val a1 = Entity("a1", "test", Map("next" -> AttributeEntityReference("test", "a2")))
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
          runAndWait(entityQuery.getCopyConflicts(context1, Seq(a1, a2, a3, a4, a5, a6, a)))
        }

        assertResult(Seq.empty) {
          runAndWait(entityQuery.copyEntities(context2, context1, "test", Seq("a1")))
        }

        //verify it was actually copied into the workspace
        assertResult(Set(a1, a2, a3, a4, a5, a6, a)) {
          runAndWait(entityQuery.list(context1, "test")).toSet
        }
      }
    }

  }

  it should "copy entities with a conflict" in withDefaultTestDatabase {

      withWorkspaceContext(testData.workspace) { context =>
        assertResult(Set(testData.sample1)) {
          runAndWait(entityQuery.getCopyConflicts(context, Seq(testData.sample1))).toSet
        }

        assertResult(Set(testData.sample1, testData.aliquot1)) {
          runAndWait(entityQuery.copyEntities(context, context, "Sample", Seq("sample1"))).toSet
        }

        //verify that it wasn't copied into the workspace again
        assert(runAndWait(entityQuery.list(context, "Sample")).toList.filter(entity => entity == testData.sample1).size == 1)
      }

  }

  it should "fail when putting dots in user-specified strings" in withDefaultTestDatabase { 
    val dottyName = Entity("dotty.name", "Sample", Map.empty)
    val dottyType = Entity("dottyType", "Sam.ple", Map.empty)
    val dottyAttr = Entity("dottyAttr", "Sample", Map("foo.bar" -> AttributeBoolean(true)))

      withWorkspaceContext(testData.workspace) { context =>
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyName)) }
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyType)) }
        intercept[RawlsException] { runAndWait(entityQuery.save(context, dottyAttr)) }
      }

  }

  Attributable.reservedAttributeNames.foreach { reserved =>
    it should "fail using reserved attribute name " + reserved in withDefaultTestDatabase { 
      val e = Entity("test_sample", "Sample", Map(reserved -> AttributeString("foo")))

        withWorkspaceContext(testData.workspace) { context =>
          intercept[RawlsException] { runAndWait(entityQuery.save(context, e)) }
        }

    }
  }
}
