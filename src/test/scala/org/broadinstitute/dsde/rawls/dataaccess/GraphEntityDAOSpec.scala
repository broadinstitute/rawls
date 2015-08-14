package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Direction
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

class GraphEntityDAOSpec extends FlatSpec with Matchers with OrientDbTestFixture {
  lazy val dao: GraphEntityDAO = new GraphEntityDAO()

  "GraphEntityDAO" should "get entity types" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Set("PairSet", "Individual", "Sample", "Aliquot", "SampleSet", "Pair")) {
          dao.getEntityTypes(context, txn).toSet
        }
      }
    }
  }

  it should "list all entities of all entity types" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7, testData.sset1, testData.sset2, testData.sset3, testData.sset4, testData.sset_empty, testData.aliquot1, testData.aliquot2, testData.pair1, testData.pair2, testData.ps1, testData.indiv1, testData.aliquot1, testData.aliquot2)) {
          dao.listEntitiesAllTypes(context, txn).toSet
        }
      }
    }
  }

  class BugTestData() extends TestData {
    val wsName = WorkspaceName("myNamespace2", "myWorkspace2")
    val workspace = new Workspace(wsName.namespace, wsName.name, "aBucket", DateTime.now, "testUser", Map.empty)

    val sample1 = new Entity("sample1", "Sample",
      Map(
        "aliquot" -> AttributeEntityReference("Aliquot", "aliquot1")),
      wsName )

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty, wsName)

    override def save(txn: RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
      withWorkspaceContext(workspace, txn) { context =>
        entityDAO.save(context, aliquot1, txn)
        entityDAO.save(context, sample1, txn)
      }
    }
  }
  val bugData = new BugTestData

  it should "get an entity with attribute ref name same as an entity, but different case" in withCustomTestDatabase(new BugTestData) { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(bugData.workspace, txn) { context =>
        assertResult(Some(bugData.sample1)) {
          dao.get(context, "Sample", "sample1", txn)
        }
      }
    }
  }

  it should "get an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Some(testData.pair1)) {
          dao.get(context, "Pair", "pair1", txn)
        }
        assertResult(Some(testData.sample1)) {
          dao.get(context, "Sample", "sample1", txn)
        }
        assertResult(Some(testData.sset1)) {
          dao.get(context, "SampleSet", "sset1", txn)
        }
      }
    }
  }

  it should "return None when an entity does not exist" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(None) {
          dao.get(context, "pair", "fnord", txn)
        }
        assertResult(None) {
          dao.get(context, "fnord", "pair1", txn)
        }
      }
    }
  }

  it should "save a new entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val pair2 = Entity("pair2", "Pair",
          Map(
            "case" -> AttributeEntityReference("Sample", "sample3"),
            "control" -> AttributeEntityReference("Sample", "sample1")),
          testData.wsName)
        dao.save(context, pair2, txn)
        assert {
          txn.withGraph { graph =>
            graph.getVertices("entityType", "Pair").exists(v => v.getProperty[String]("name") == "pair2")
          }
        }
      }
    }
  }

  it should "clone all entities from a workspace containing cycles" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      lazy val daoCycles: GraphEntityDAO = new GraphEntityDAO()
      lazy val workspaceDaoOriginal: GraphWorkspaceDAO = new GraphWorkspaceDAO()
      lazy val workspaceDaoClone: GraphWorkspaceDAO = new GraphWorkspaceDAO()

      val workspaceOriginal = Workspace(
        namespace = testData.wsName.namespace + "Original",
        name = testData.wsName.name + "Original",
        bucketName = "aBucket",
        createdDate = DateTime.now(),
        createdBy = "Joe Biden",
        Map.empty
      )

      val workspaceClone = Workspace(
        namespace = testData.wsName.namespace + "Clone",
        name = testData.wsName.name + "Clone",
        bucketName = "anotherBucket",
        createdDate = DateTime.now(),
        createdBy = "Joe Biden",
        Map.empty
      )

      val c1 = Entity("c1", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle1" -> AttributeEntityReference("samples", "c2")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))
      val c2 = Entity("c2", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle2" -> AttributeEntityReference("samples", "c3")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))
      var c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3)), WorkspaceName(testData.workspace.namespace, testData.workspace.name))

      workspaceDaoOriginal.save(workspaceOriginal, txn)
      workspaceDaoClone.save(workspaceClone, txn)

      withWorkspaceContext(workspaceOriginal, txn) { originalContext =>
        withWorkspaceContext(workspaceClone, txn) { cloneContext =>
          daoCycles.save(originalContext, c3, txn)
          daoCycles.save(originalContext, c2, txn)
          daoCycles.save(originalContext, c1, txn)

          c3 = Entity("c3", "samples", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "cycle3" -> AttributeEntityReference("samples", "c1")), WorkspaceName(testData.workspace.namespace, testData.workspace.name))

          daoCycles.save(originalContext, c3, txn)
          daoCycles.cloneAllEntities(originalContext, cloneContext, txn)

          assertResult(dao.listEntitiesAllTypes(originalContext, txn).map(_.copy(workspaceName = WorkspaceName(workspaceClone.namespace, workspaceClone.name))).toSet) {
            dao.listEntitiesAllTypes(cloneContext, txn).toSet
          }
        }
      }
    }
  }

  it should "save updates to an existing entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        // flip case / control and add property, remove a property
        val pair1Updated = Entity("pair1", "Pair",
          Map(
            "isItAPair" -> AttributeBoolean(true),
            "case" -> AttributeEntityReference("Sample", "sample2"),
            "control" -> AttributeEntityReference("Sample", "sample1")),
          testData.wsName)
        dao.save(context, pair1Updated, txn)
        txn.withGraph { graph =>
          val fetched = graph.getVertices("entityType", "Pair").filter(v => v.getProperty[String]("name") == "pair1").head
          assert {
            fetched.getVertices(Direction.OUT).head.getPropertyKeys.contains("isItAPair")
          }
          // TODO check edges?
        }

        val pair1UpdatedAgain = Entity("pair1", "Pair",
          Map(
            "case" -> AttributeEntityReference("Sample", "sample2"),
            "control" -> AttributeEntityReference("Sample", "sample1")),
          testData.wsName)
        dao.save(context, pair1UpdatedAgain, txn)
        txn.withGraph { graph =>
          val fetched = graph.getVertices("entityType", "Pair").filter(v => v.getProperty[String]("name") == "pair1").head
          assert {
            !fetched.getPropertyKeys.contains("isItAPair")
          }
        }
      }
    }
  }

  it should "throw an exception if trying to save invalid references" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val baz = Entity("wig", "wug", Map("edgeToNowhere" -> AttributeEntityReference("sample", "notTheSampleYoureLookingFor")), testData.wsName)
        intercept[RawlsException] {
          dao.save(context, baz, txn)
        }
      }
    }
  }

  it should "delete an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        dao.delete(context, "Pair", "pair2", txn)
        assert {
          txn.withGraph { graph =>
            !graph.getVertices("entityType", "Pair").exists(v => v.getProperty[String]("name") == "pair2")
          }
        }
      }
    }
  }

  it should "list entities" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Set(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6, testData.sample7)) {
          dao.list(context, "Sample", txn).toSet
        }
      }
    }
  }

  it should "add cycles to entity graph" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        val sample1Copy = Entity("sample1", "Sample",
          Map(
            "type" -> AttributeString("normal"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
            "aliquot" -> AttributeEntityReference("Aliquot", "aliquot1"),
            "cycle" -> AttributeEntityReference("SampleSet", "sset1")),
          testData.wsName)
        dao.save(context, sample1Copy, txn)
        val sample5Copy = Entity("sample5", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
            "cycle" -> AttributeEntityReference("SampleSet", "sset4")),
          testData.wsName)
        dao.save(context, sample5Copy, txn)
        val sample7Copy = Entity("sample7", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
            "cycle" -> AttributeEntityReference("Sample", "sample6")),
          testData.wsName)
        dao.save(context, sample7Copy, txn)
        val sample6Copy = Entity("sample6", "Sample",
          Map(
            "type" -> AttributeString("tumor"),
            "whatsit" -> AttributeNumber(100),
            "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
            "cycle" -> AttributeEntityReference("SampleSet", "sset3")),
          testData.wsName)
        dao.save(context, sample6Copy, txn)
        val entitiesWithCycles = List("sample1", "sample5", "sample7", "sample6")
        txn.withGraph { graph =>
          val fetched = graph.getVertices().filter(v => entitiesWithCycles.contains(v.getProperty[String]("name"))).head.getVertices(Direction.OUT).head.getEdges(Direction.OUT, "cycle")
          assert {
            fetched.size == 1
          }
        }
      }
    }
  }

  it should "rename an entity" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        dao.rename(context, "Pair", "pair1", "amazingPair", txn)
        txn.withGraph { graph =>
          val pairNames = graph.getVertices("entityType", "Pair").map(_.getProperty[String]("name")).toList
          assert {
            pairNames.contains("amazingPair")
          }
          assert {
            !pairNames.contains("pair1")
          }
        }
      }
    }
  }

  /* Test case tests for cycles, cycles contained within cycles, cycles existing below other cycles, invalid
   * entity names being supplied, and multiple disjoint subtrees
   */
  it should "get entity subtrees from a list of entities" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Set(testData.sset3, testData.sample1, testData.sset2, testData.aliquot1, testData.sample6, testData.sset1, testData.sample2, testData.sample3, testData.sample5)) {
          dao.getEntitySubtrees(context, "SampleSet", List("sset1", "sset2", "sset3", "sampleSetDOESNTEXIST"), txn).toSet
        }
      }
    }
  }

  val x1 = Entity("x1", "SampleSet", Map.empty, testData.wsName)

  val workspace2 = Workspace(
    namespace = testData.wsName.namespace + "2",
    name = testData.wsName.name + "2",
    bucketName = "aBucket",
    createdDate = DateTime.now(),
    createdBy = "Joe Biden",
    Map.empty
  )

  it should "copy entities without a conflict" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      workspaceDAO.save(workspace2, txn)
      withWorkspaceContext(testData.workspace, txn) { context1 =>
        withWorkspaceContext(workspace2, txn) { context2 =>
          dao.save(context2, x1, txn)

          // note: we're copying FROM workspace2 INTO workspace
          assertResult(Seq.empty) {
            dao.getCopyConflicts(context1, Seq(x1), txn)
          }

          assertResult(Seq.empty) {
            dao.copyEntities(context2, context1, "SampleSet", Seq("x1"), txn)
          }

          //verify it was actually copied into the workspace
          assert(dao.list(context1, "SampleSet", txn).toList.contains(x1))
        }
      }
    }
  }


  it should "copy entities with a conflict" in withDefaultTestDatabase { dataSource =>
    dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        assertResult(Set(testData.sample1)) {
          dao.getCopyConflicts(context, Seq(testData.sample1), txn).toSet
        }

        assertResult(Set(testData.sample1, testData.aliquot1)) {
          dao.copyEntities(context, context, "Sample", Seq("sample1"), txn).toSet
        }

        //verify that it wasn't copied into the workspace again
        assert(dao.list(context, "Sample", txn).toList.filter(entity => entity == testData.sample1).size == 1)
      }
    }
  }

  it should "fail when putting dots in user-specified strings" in withDefaultTestDatabase { dataSource =>
    val dottyName = Entity("dotty.name", "Sample", Map.empty, testData.wsName)
    val dottyType = Entity("dottyType", "Sam.ple", Map.empty, testData.wsName)
    val dottyAttr = Entity("dottyAttr", "Sample", Map("foo.bar" -> AttributeBoolean(true)), testData.wsName)
    dataSource inTransaction { txn =>
      withWorkspaceContext(testData.workspace, txn) { context =>
        intercept[RawlsException] { dao.save(context, dottyName, txn) }
        intercept[RawlsException] { dao.save(context, dottyType, txn) }
        intercept[RawlsException] { dao.save(context, dottyAttr, txn) }
      }
    }
  }
}