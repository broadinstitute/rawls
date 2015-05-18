package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._

class GraphEntityDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  override val testDbName = "GraphEntityDAOSpec"
  lazy val dao: GraphEntityDAO = new GraphEntityDAO(graph)

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
    Map(
      "sample" -> Map(
        sample1.name -> sample1,
        sample2.name -> sample2,
        sample3.name -> sample3),
      "pair" -> Map(
        pair1.name -> pair1),
      "sampleSet" -> Map(
        sampleSet1.name -> sampleSet1)
    )
  )

  // for the initial upload, we need the workspace DAO
  // unfortunately because of how BeforeAndAfterAll works, this has to be inside a test...
  "GraphEntityDAO" should "setup a workspace" in {
    new GraphWorkspaceDAO(graph).save(workspace)
  }

  it should "get an entity" in {
    assertResult(Some(pair1)) { dao.get(workspace.namespace, workspace.name, "pair", "pair1") }
    assertResult(Some(sample1)) { dao.get(workspace.namespace, workspace.name, "sample", "sample1") }
    assertResult(Some(sampleSet1)) { dao.get(workspace.namespace, workspace.name, "sampleSet", "sampleSet1") }
  }

  it should "return None when an entity does not exist" in {
    assertResult(None) { dao.get(workspace.namespace, workspace.name, "pair", "fnord") }
    assertResult(None) { dao.get(workspace.namespace, workspace.name, "fnord", "pair1") }
  }

  it should "return None when a parent workspace does not exist" in {
    assertResult(None) { dao.get(workspace.namespace, "fnord", "pair", "pair1") }
  }

  it should "save a new entity" in {
    val pair2 = Entity("pair2", "pair",
      Map(
        "case" -> AttributeReferenceSingle("sample", "sample3"),
        "control" -> AttributeReferenceSingle("sample", "sample1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, pair2)
    assert {
      graph.getVertices("_clazz", "pair")
        .filter(v => v.getProperty("_name") == "pair2")
        .headOption.isDefined
    }
  }

  it should "save updates to an existing entity" in {
    // flip case / control and add property
    val pair1Updated = Entity("pair1", "pair",
      Map(
        "isItAPair" -> AttributeBoolean(true),
        "case" -> AttributeReferenceSingle("sample", "sample2"),
        "control" -> AttributeReferenceSingle("sample", "sample1")),
      wsName)
    dao.save(workspace.namespace, workspace.name, pair1Updated)
    val fetched = graph.getVertices("_clazz", "pair").filter(v => v.getProperty("_name") == "pair1").head
    assert { fetched.getPropertyKeys.contains("isItAPair") }
    // TODO check edges?
  }

  it should "throw an exception when trying to save an entity to a nonexistent workspace" in {
    val foo = Entity("foo", "bar", Map.empty, wsName)
    intercept[IllegalArgumentException] { dao.save("fnord", "dronf", foo) }
  }

  it should "throw an exception if trying to save reserved-keyword attributes" in {
    val bar = Entity("boo", "far", Map("_clazz" -> AttributeString("myAmazingClazzzz")), wsName)
    intercept[IllegalArgumentException] { dao.save(workspace.namespace, workspace.name, bar) }
  }

  it should "throw an exception if trying to save invalid references" in {
    val baz = Entity("wig", "wug", Map("edgeToNowhere" -> AttributeReferenceSingle("sample", "notTheSampleYoureLookingFor")), wsName)
    intercept[IllegalArgumentException] { dao.save(workspace.namespace, workspace.name, baz) }
  }

  it should "delete an entity" in {
    dao.delete(workspace.namespace, workspace.name, "pair", "pair2")
    assert {
      graph.getVertices("_clazz", "pair")
        .filter(v => v.getProperty("_name") == "pair2")
        .headOption.isEmpty
    }
  }

  it should "list entities" in {
    assert {
      val samples = dao.list(workspace.namespace, workspace.name, "sample").toList
      List(sample1, sample2, sample3).map(samples.contains(_)).reduce(_&&_)
    }
  }

  it should "rename an entity" in {
    dao.rename(workspace.namespace, workspace.name, "pair", "pair1", "amazingPair")
    val pairNames = graph.getVertices("_clazz", "pair").map(_.getProperty[String]("_name")).toList
    assert { pairNames.contains("amazingPair") }
    assert { !pairNames.contains("pair1") }
  }
}
