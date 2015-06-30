package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Direction
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._

class GraphEntityDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  override val testDbName = "GraphEntityDAOSpec"
  lazy val dao: GraphEntityDAO = new GraphEntityDAO()

  // setup workspace objects
  val wsName = WorkspaceName("myNamespace", "myWorkspace")

  val aliquot1 = Entity("aliquot1", "aliquot", Map("sampleSet" -> AttributeReferenceSingle("sampleSet", "sampleSet3")), wsName)
  val aliquot2 = Entity("aliquot2", "aliquot", Map.empty, wsName)

  var sample1 = Entity("sample1", "sample",
    Map(
      "type" -> AttributeString("normal"),
      "whatsit" -> AttributeNumber(100),
      "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
      "aliquot" -> AttributeReferenceSingle("aliquot", "aliquot1")),
    wsName)

  val sample2 = Entity("sample2", "sample", Map("type" -> AttributeString("tumor"), "aliquot2" -> AttributeReferenceSingle("aliquot", "aliquot2")), wsName)
  val sample3 = Entity("sample3", "sample", Map("type" -> AttributeString("tumor")), wsName)
  val sample4 = Entity("sample4", "sample", Map("type" -> AttributeString("tumor")), wsName)
  var sample5 = Entity("sample5", "sample", Map("type" -> AttributeString("tumor")), wsName)
  var sample6 = Entity("sample6", "sample", Map("type" -> AttributeString("tumor")), wsName)
  var sample7 = Entity("sample7", "sample", Map("type" -> AttributeString("tumor"), "cycle" -> AttributeReferenceSingle("sample", "sample6")), wsName)

  val pair1 = Entity("pair1", "pair",
    Map(
      "case" -> AttributeReferenceSingle("sample", "sample1"),
      "control" -> AttributeReferenceSingle("sample", "sample2")),
    wsName)

  val sampleSet1 = Entity("sampleSet1", "sampleSet",
    Map("hasSamples" -> AttributeReferenceList(Seq(
      AttributeReferenceSingle("sample", "sample1"),
      AttributeReferenceSingle("sample", "sample2")))),
    wsName)

  val sampleSet2 = Entity("sampleSet2", "sampleSet",
    Map("hasSamples" -> AttributeReferenceList(Seq(
      AttributeReferenceSingle("sample", "sample4")))),
    wsName)

  val sampleSet3 = Entity("sampleSet3", "sampleSet",
    Map("hasSamples" -> AttributeReferenceList(Seq(
      AttributeReferenceSingle("sample", "sample5"),
      AttributeReferenceSingle("sample", "sample6")))),
    wsName)

  val sampleSet4 = Entity("sampleSet4", "sampleSet",
    Map("hasSamples" -> AttributeReferenceList(Seq(
      AttributeReferenceSingle("sample", "sample7")))),
    wsName)

  val workspace = Workspace(
    namespace = wsName.namespace,
    name = wsName.name,
    createdDate = DateTime.now(),
    createdBy = "Joe Biden",
    Map.empty
  )

  // for the initial upload, we need the workspace DAO
  // unfortunately because of how BeforeAndAfterAll works, this has to be inside a test...
  "GraphEntityDAO" should "setup a workspace" in {
    new GraphWorkspaceDAO().save(workspace, txn)
    dao.save(workspace.namespace, workspace.name, sample5, txn)
    dao.save(workspace.namespace, workspace.name, sample6, txn)
    dao.save(workspace.namespace, workspace.name, sampleSet3, txn)
    dao.save(workspace.namespace, workspace.name, aliquot1, txn)
    dao.save(workspace.namespace, workspace.name, aliquot2, txn)
    dao.save(workspace.namespace, workspace.name, sample1, txn)
    dao.save(workspace.namespace, workspace.name, sample2, txn)
    dao.save(workspace.namespace, workspace.name, sample3, txn)
    dao.save(workspace.namespace, workspace.name, sample4, txn)
    dao.save(workspace.namespace, workspace.name, sample7, txn)
    dao.save(workspace.namespace, workspace.name, pair1, txn)
    dao.save(workspace.namespace, workspace.name, sampleSet1, txn)
    dao.save(workspace.namespace, workspace.name, sampleSet2, txn)
    dao.save(workspace.namespace, workspace.name, sampleSet4, txn)
  }

  it should "get entity types" in {
    assertResult(Set("sample", "sampleSet", "aliquot", "pair")) { dao.getEntityTypes(workspace.namespace, workspace.name, txn).toSet }
  }

  it should "list all entities of all entity types" in {
    assertResult(Set(sample5, sample6, sample1, sample2, sample3, sample4, sample7, sampleSet3, sampleSet1, sampleSet2, sampleSet4, aliquot1, aliquot2, pair1)) {
      dao.listEntitiesAllTypes(workspace.namespace, workspace.name, txn).toSet
    }
  }

  it should "list all entities of all entity types" in {
    assertResult(Set(sample5, sample6, sample1, sample2, sample3, sample4, sample7, sampleSet3, sampleSet1, sampleSet2, sampleSet4, aliquot1, aliquot2, pair1)) { dao.listEntitiesAllTypes(workspace.namespace, workspace.name, txn).toSet }
  }

  it should "get an entity" in {
    assertResult(Some(pair1)) { dao.get(workspace.namespace, workspace.name, "pair", "pair1", txn) }
    assertResult(Some(sample1)) { dao.get(workspace.namespace, workspace.name, "sample", "sample1", txn) }
    assertResult(Some(sampleSet1)) { dao.get(workspace.namespace, workspace.name, "sampleSet", "sampleSet1", txn) }
  }

  it should "return None when an entity does not exist" in {
    assertResult(None) { dao.get(workspace.namespace, workspace.name, "pair", "fnord", txn) }
    assertResult(None) { dao.get(workspace.namespace, workspace.name, "fnord", "pair1", txn) }
  }

  it should "return None when a parent workspace does not exist" in {
    assertResult(None) { dao.get(workspace.namespace, "fnord", "pair", "pair1", txn) }
  }

  it should "save a new entity" in {
    val pair2 = Entity("pair2", "pair",
      Map(
        "case" -> AttributeReferenceSingle("sample", "sample3"),
        "control" -> AttributeReferenceSingle("sample", "sample1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, pair2, txn)
    assert {
      txn.withGraph { graph =>
        graph.getVertices("_entityType", "pair")
          .filter(v => v.getProperty[String]("_name") == "pair2")
          .headOption.isDefined
      }
    }
  }

  it should "clone all entities from a workspace containing cycles" in {
    lazy val daoCycles: GraphEntityDAO = new GraphEntityDAO()
    lazy val workspaceDaoOriginal: GraphWorkspaceDAO = new GraphWorkspaceDAO()
    lazy val workspaceDaoClone: GraphWorkspaceDAO = new GraphWorkspaceDAO()

    val workspaceOriginal = Workspace(
      namespace = wsName.namespace + "Original",
      name = wsName.name + "Original",
      createdDate = DateTime.now(),
      createdBy = "Joe Biden",
      Map.empty
    )

    val workspaceClone = Workspace(
      namespace = wsName.namespace + "Clone",
      name = wsName.name + "Clone",
      createdDate = DateTime.now(),
      createdBy = "Joe Biden",
      Map.empty
    )

    val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle1" -> AttributeReferenceSingle("samples", "c2")), WorkspaceName(workspace.namespace, workspace.name))
    val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle2" -> AttributeReferenceSingle("samples", "c3")), WorkspaceName(workspace.namespace, workspace.name))
    var c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), WorkspaceName(workspace.namespace, workspace.name))

    workspaceDaoOriginal.save(workspaceOriginal, txn)
    workspaceDaoClone.save(workspaceClone, txn)

    daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c3, txn)
    daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c2, txn)
    daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c1, txn)

    c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle3" -> AttributeReferenceSingle("samples", "c1")), WorkspaceName(workspace.namespace, workspace.name))

    daoCycles.save(workspaceOriginal.namespace, workspaceOriginal.name, c3, txn)
    daoCycles.cloneAllEntities(workspaceOriginal.namespace, workspaceClone.namespace, workspaceOriginal.name, workspaceClone.name, txn)

    assertResult(dao.listEntitiesAllTypes(workspaceOriginal.namespace, workspaceOriginal.name, txn).map(_.copy(workspaceName = WorkspaceName(workspaceClone.namespace, workspaceClone.name))).toSet) {
      dao.listEntitiesAllTypes(workspaceClone.namespace, workspaceClone.name, txn).toSet
    }
  }

  it should "save updates to an existing entity" in {
    // flip case / control and add property, remove a property
    val pair1Updated = Entity("pair1", "pair",
      Map(
        "isItAPair" -> AttributeBoolean(true),
        "case" -> AttributeReferenceSingle("sample", "sample2"),
        "control" -> AttributeReferenceSingle("sample", "sample1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, pair1Updated, txn)
    txn.withGraph { graph =>
      val fetched = graph.getVertices("_entityType", "pair").filter(v => v.getProperty[String]("_name") == "pair1").head
      assert { fetched.getPropertyKeys.contains("isItAPair") }
      // TODO check edges?
    }

    val pair1UpdatedAgain = Entity("pair1", "pair",
      Map(
        "case" -> AttributeReferenceSingle("sample", "sample2"),
        "control" -> AttributeReferenceSingle("sample", "sample1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, pair1UpdatedAgain, txn)
    txn.withGraph { graph =>
      val fetched = graph.getVertices("_entityType", "pair").filter(v => v.getProperty[String]("_name") == "pair1").head
      assert { !fetched.getPropertyKeys.contains("isItAPair") }
    }
  }

  it should "throw an exception when trying to save an entity to a nonexistent workspace" in {
    val foo = Entity("foo", "bar", Map.empty, wsName)
    intercept[IllegalArgumentException] { dao.save("fnord", "dronf", foo, txn) }
  }

  it should "throw an exception if trying to save reserved-keyword attributes" in {
    val bar = Entity("boo", "far", Map("_entityType" -> AttributeString("myAmazingClazzzz")), wsName)
    intercept[IllegalArgumentException] { dao.save(workspace.namespace, workspace.name, bar, txn) }
  }

  it should "throw an exception if trying to save invalid references" in {
    val baz = Entity("wig", "wug", Map("edgeToNowhere" -> AttributeReferenceSingle("sample", "notTheSampleYoureLookingFor")), wsName)
    intercept[IllegalArgumentException] { dao.save(workspace.namespace, workspace.name, baz, txn) }
  }

  it should "delete an entity" in {
    dao.delete(workspace.namespace, workspace.name, "pair", "pair2", txn)
    assert {
      txn.withGraph { graph =>
        graph.getVertices("_entityType", "pair")
          .filter(v => v.getProperty[String]("_name") == "pair2")
          .headOption.isEmpty
      }
    }
  }

  it should "list entities" in {
    assert {
      val samples = dao.list(workspace.namespace, workspace.name, "sample", txn).toList
      List(sample1, sample2, sample3).map(samples.contains(_)).reduce(_&&_)
    }
  }

  it should "add cycles to entity graph" in {
    sample1 = Entity("sample1", "sample",
      Map(
        "type" -> AttributeString("normal"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "aliquot" -> AttributeReferenceSingle("aliquot", "aliquot1"),
        "cycle" -> AttributeReferenceSingle("sampleSet", "sampleSet1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, sample1, txn)
    sample5 = Entity("sample5", "sample",
      Map(
        "type" -> AttributeString("tumor"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "cycle" -> AttributeReferenceSingle("sampleSet", "sampleSet4")),
      wsName)
    dao.save(workspace.namespace, workspace.name, sample5, txn)
    sample7 = Entity("sample7", "sample",
      Map(
        "type" -> AttributeString("tumor"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "cycle" -> AttributeReferenceSingle("sample", "sample6")),
      wsName)
    dao.save(workspace.namespace, workspace.name, sample7, txn)
    sample6 = Entity("sample6", "sample",
      Map(
        "type" -> AttributeString("tumor"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "cycle" -> AttributeReferenceSingle("sampleSet", "sampleSet3")),
      wsName)
    dao.save(workspace.namespace, workspace.name, sample6, txn)
    val entitiesWithCycles = List("sample1", "sample5", "sample7", "sample6")
    txn.withGraph { graph =>
      val fetched = graph.getVertices().filter(v => entitiesWithCycles.contains(v.getProperty[String]("_name"))).head.getEdges(Direction.OUT, "cycle")
      assert { fetched.size == 1 }
    }
  }

  it should "rename an entity" in {
    dao.rename(workspace.namespace, workspace.name, "pair", "pair1", "amazingPair", txn)
    txn.withGraph { graph =>
      val pairNames = graph.getVertices("_entityType", "pair").map(_.getProperty[String]("_name")).toList
      assert {
        pairNames.contains("amazingPair")
      }
      assert {
        !pairNames.contains("pair1")
      }
    }
  }

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */
  it should "get entity subtrees from a list of entities" in {
    assertResult(Set(sample6, sample5, sampleSet2, sample1, sampleSet3, sampleSet4, sample7, sample4, aliquot2, sample2, aliquot1, sampleSet1)){
      dao.getEntitySubtrees(workspace.namespace, workspace.name, "sampleSet", List("sampleSet1", "sampleSet2", "sampleSet4", "sampleSetDOESNTEXIST"), txn).toSet
    }
  }

  val x1 = Entity("x1", "sampleSet", Map.empty, wsName)

  val workspace2 = Workspace(
    namespace = wsName.namespace + "2",
    name = wsName.name + "2",
    createdDate = DateTime.now(),
    createdBy = "Joe Biden",
    Map.empty
  )

  it should "copy entities without a conflict" in {
    new GraphWorkspaceDAO().save(workspace2, txn)
    dao.save(workspace2.namespace, workspace2.name, x1, txn)

    assertResult(Seq.empty){
      dao.getCopyConflicts(workspace.namespace, workspace.name, Seq(x1), txn)
    }

    assertResult(Seq.empty){
      dao.copyEntities(workspace.namespace, workspace.name, workspace2.namespace, workspace2.name, "sampleSet", Seq("x1"), txn)
    }

    //verify it was actually copied into the workspace
    assert(dao.list(workspace.namespace, workspace.name, "sampleSet", txn).toList.contains(x1))
  }

  it should "copy entities with a conflict" in {
    assertResult(Seq(x1)){
      dao.getCopyConflicts(workspace.namespace, workspace.name, Seq(x1), txn)
    }

    assertResult(Seq(x1)){
      dao.copyEntities(workspace.namespace, workspace.name, workspace2.namespace, workspace2.name, "sampleSet", Seq("x1"), txn)
    }

    //verify that it wasn't copied into the workspace again
    assert(dao.list(workspace.namespace, workspace.name, "sampleSet", txn).toList.filter(entity => entity == x1).size == 1)
  }
}
