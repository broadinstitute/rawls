package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import com.tinkerpop.blueprints.Direction
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._

class GraphEntityDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  override val testDbName = "GraphEntityDAOSpec"
  lazy val dao: GraphEntityDAO = new GraphEntityDAO()
  lazy val daoCycles: GraphEntityDAO = new GraphEntityDAO()
  lazy val daoCyclesClone: GraphEntityDAO = new GraphEntityDAO()

  // setup workspace objects
  val wsName = WorkspaceName("myNamespace", "myWorkspace")

  val sample1 = Entity("sample1", "sample",
    Map(
      "type" -> AttributeString("normal"),
      "whatsit" -> AttributeNumber(100),
      "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true)))),
    wsName)

  val sample2 = Entity("sample2", "sample", Map("type" -> AttributeString("tumor")), wsName)
  val sample3 = Entity("sample3", "sample", Map("type" -> AttributeString("tumor")), wsName)

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

  val workspace = Workspace(
    namespace = wsName.namespace,
    name = wsName.name,
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

  // for the initial upload, we need the workspace DAO
  // unfortunately because of how BeforeAndAfterAll works, this has to be inside a test...
  "GraphEntityDAO" should "setup a workspace" in {
    new GraphWorkspaceDAO().save(workspace, txn)
    dao.save(workspace.namespace, workspace.name, sample1, txn)
    dao.save(workspace.namespace, workspace.name, sample2, txn)
    dao.save(workspace.namespace, workspace.name, sample3, txn)
    dao.save(workspace.namespace, workspace.name, pair1, txn)
    dao.save(workspace.namespace, workspace.name, sampleSet1, txn)
  }

  it should "get entity types" in {
    assertResult(Seq("sample", "pair", "sampleSet")) { dao.getEntityTypes(workspace.namespace, workspace.name, txn) }
  }

  it should "list all entities of all entity types" in {
    assertResult(Seq(sample1, sample2, sample3, pair1, sampleSet1)) { dao.listEntitiesAllTypes(workspace.namespace, workspace.name, txn).toSeq }
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

  it should "copy a workspace with cycles in it" in {
    val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))

    val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle1" -> AttributeReferenceSingle("samples", "c2")), WorkspaceName(workspace.namespace, workspace.name))
    val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle2" -> AttributeReferenceSingle("samples", "c3")), WorkspaceName(workspace.namespace, workspace.name))
    var c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList), WorkspaceName(workspace.namespace, workspace.name))

    daoCycles.save(workspace.namespace, workspace.name, c3, txn)
    daoCycles.save(workspace.namespace, workspace.name, c2, txn)
    daoCycles.save(workspace.namespace, workspace.name, c1, txn)

    c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList, "cycle3" -> AttributeReferenceSingle("samples", "c1")), WorkspaceName(workspace.namespace, workspace.name))
    daoCycles.save(workspace.namespace, workspace.name, c3, txn)


    daoCycles.cloneAllEntities(workspace.namespace, workspaceClone.namespace, workspace.name, workspaceClone.namespace, txn)
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
}
