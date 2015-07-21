package org.broadinstitute.dsde.rawls.graph

import java.util.logging.{LogManager, Logger}

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.joda.time.DateTime
import scala.collection.immutable.HashMap
import org.scalatest.BeforeAndAfterAll
import java.util.UUID
import spray.json._
import spray.httpx.SprayJsonSupport
import SprayJsonSupport._
import WorkspaceJsonSupport._


trait OrientDbTestFixture extends BeforeAndAfterAll {
  this : org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  val testDate = new DateTime()

  override def beforeAll: Unit = {
    // TODO find a better way to set the log level. Nothing else seems to work.
    LogManager.getLogManager().reset()
    Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)
  }
  override def afterAll: Unit = {
  }

  lazy val entityDAO: GraphEntityDAO = new GraphEntityDAO()
  lazy val workspaceDAO = new GraphWorkspaceDAO()
  lazy val methodConfigDAO = new GraphMethodConfigurationDAO()
  lazy val submissionDAO: SubmissionDAO = new GraphSubmissionDAO(new GraphWorkflowDAO)

  abstract class TestData {
    def save(txn:RawlsTransaction)
  }

  class DefaultTestData() extends TestData {
    // setup workspace objects
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val workspace = Workspace(wsName.namespace, wsName.name, "aBucket", DateTime.now, "testUser", new HashMap[String, Attribute]() )

    val sample1 = Entity("sample1", "Sample",
      Map(
        "type" -> AttributeString("normal"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "quot" -> AttributeReferenceSingle("Aliquot", "aliquot1"),
        "somefoo" -> AttributeString("itsfoo")),
      WorkspaceName(wsName.namespace, wsName.name) )
    val sample2 = Entity("sample2", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeString("huh?") ), WorkspaceName(wsName.namespace, wsName.name) )
    val sample3 = Entity("sample3", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeReferenceSingle("Sample", "sample1") ), WorkspaceName(wsName.namespace, wsName.name) )
    val sample4 = Entity("sample4", "Sample", Map("type" -> AttributeString("tumor")), wsName)
    var sample5 = Entity("sample5", "Sample", Map("type" -> AttributeString("tumor")), wsName)
    var sample6 = Entity("sample6", "Sample", Map("type" -> AttributeString("tumor")), wsName)
    var sample7 = Entity("sample7", "Sample", Map("type" -> AttributeString("tumor"), "cycle" -> AttributeReferenceSingle("Sample", "sample6")), wsName)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty, wsName)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty, wsName)

    val pair1 = Entity("pair1", "Pair",
      Map( "case" -> AttributeReferenceSingle("Sample", "sample2"),
        "control" -> AttributeReferenceSingle("Sample", "sample1") ),
      WorkspaceName(wsName.namespace, wsName.name) )
    val pair2 = Entity("pair2", "Pair",
      Map( "case" -> AttributeReferenceSingle("Sample", "sample3"),
        "control" -> AttributeReferenceSingle("Sample", "sample1") ),
      WorkspaceName(wsName.namespace, wsName.name) )

    val sset1 = Entity("sset1", "SampleSet",
      Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample1"),
        AttributeReferenceSingle("Sample", "sample2"),
        AttributeReferenceSingle("Sample", "sample3"))) ),
      WorkspaceName(wsName.namespace, wsName.name) )
    val sset2 = new Entity("sset2", "SampleSet",
      Map( "samples" -> AttributeReferenceList( List(AttributeReferenceSingle("Sample", "sample2"))) ),
      WorkspaceName(wsName.namespace, wsName.name) )

    val sset3 = Entity("sset3", "SampleSet",
      Map("hasSamples" -> AttributeReferenceList(Seq(
        AttributeReferenceSingle("Sample", "sample5"),
        AttributeReferenceSingle("Sample", "sample6")))),
      wsName)

    val sset4 = Entity("sset4", "SampleSet",
      Map("hasSamples" -> AttributeReferenceList(Seq(
        AttributeReferenceSingle("Sample", "sample7")))),
      wsName)

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map( "samples" -> AttributeReferenceList(List())),
      WorkspaceName("workspaces", "test_workspace") )

    val ps1 = Entity("ps1", "PairSet",
      Map( "pairs" -> AttributeReferenceList( List(AttributeReferenceSingle("Pair", "pair1"),
        AttributeReferenceSingle("Pair", "pair2"))) ),
      WorkspaceName(wsName.namespace, wsName.name) )

    val indiv1 = Entity("indiv1", "Individual",
      Map( "sset" -> AttributeReferenceSingle("SampleSet", "sset1") ),
      WorkspaceName(wsName.namespace, wsName.name) )

    val methodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      "Sample",
      Map("i1" -> "input expr"),
      Map("o1" -> "output expr"),
      Map("p1" -> "prereq expr"),
      wsName,
      MethodStoreConfiguration("ns", "meth1", "1"),
      MethodStoreMethod("ns-config", "meth1", "1")
    )

    val methodConfig2 = MethodConfiguration("dsde", "testConfig2", "Sample", Map("ready"-> "true"), Map("param1"-> "foo"), Map("out" -> "bar"), wsName, MethodStoreConfiguration(wsName.namespace+"-config", "method-a-config", "1"), MethodStoreMethod(wsName.namespace, "method-a", "1"))
    val methodConfig3 = MethodConfiguration("dsde", "testConfig", "Sample", Map("ready"-> "true"), Map("param1"-> "foo", "param2"-> "foo2"), Map("out" -> "bar"), wsName, MethodStoreConfiguration("ns", "meth1", "1"), MethodStoreMethod("ns-config", "meth1", "1"))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> "this.type"), outputs=Map.empty, wsName, MethodStoreConfiguration("dsde-config", "three_step", "1"), MethodStoreMethod("dsde", "three_step", "1"))
    val methodConfigUnparseable = MethodConfiguration("dsde", "UnparseableMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> "this..wont.parse"), outputs=Map.empty, wsName, MethodStoreConfiguration("dsde-config", "three_step", "1"), MethodStoreMethod("dsde", "three_step", "1"))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> "this.tumortype"), outputs=Map.empty, wsName, MethodStoreConfiguration("dsde-config", "three_step", "1"), MethodStoreMethod("dsde", "three_step", "1"))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> "this.confused"), outputs=Map.empty, wsName, MethodStoreConfiguration("dsde-config", "three_step", "1"), MethodStoreMethod("dsde", "three_step", "1"))

    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, methodConfig.workspaceName)
    val methodConfigName2 = methodConfigName.copy(name="novelName")
    val methodConfigName3 = methodConfigName.copy(name="noSuchName")
    val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName,methodConfigName2)
    val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName,methodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, methodConfig.workspaceName)
    val methodRepoGood = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", newMethodConfigName)
    val methodRepoMissing = MethodRepoConfigurationQuery("workspace_test", "rawls_test_missing", "1", methodConfigName)
    val methodRepoEmptyPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_empty_payload", "1", methodConfigName)
    val methodRepoBadPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_bad_payload", "1", methodConfigName)

    val submission1 = Submission("submission1",testDate,workspace.namespace,workspace.name,"std","someMethod",indiv1.entityType, indiv1.name,
      Seq(Workflow(workspace.namespace,workspace.name,"workflow1",WorkflowStatuses.Submitted,testDate,sample1.entityType, sample1.name),
        Workflow(workspace.namespace,workspace.name,"workflow2",WorkflowStatuses.Submitted,testDate,sample2.entityType, sample2.name),
        Workflow(workspace.namespace,workspace.name,"workflow3",WorkflowStatuses.Submitted,testDate,sample3.entityType, sample3.name)), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)
    val submission2 = Submission("submission2",testDate,workspace.namespace,workspace.name,"std","someMethod",indiv1.entityType, indiv1.name,
      Seq(Workflow(workspace.namespace,workspace.name,"workflow4",WorkflowStatuses.Submitted,testDate,sample1.entityType, sample1.name),
        Workflow(workspace.namespace,workspace.name,"workflow5",WorkflowStatuses.Submitted,testDate,sample2.entityType, sample2.name),
        Workflow(workspace.namespace,workspace.name,"workflow6",WorkflowStatuses.Submitted,testDate,sample3.entityType, sample3.name)), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    override def save(txn:RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)

      entityDAO.save(workspace.namespace, workspace.name, aliquot1, txn)
      entityDAO.save(workspace.namespace, workspace.name, aliquot2, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample1, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample2, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample3, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample4, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample5, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample6, txn)
      entityDAO.save(workspace.namespace, workspace.name, sample7, txn)
      entityDAO.save(workspace.namespace, workspace.name, pair1, txn)
      entityDAO.save(workspace.namespace, workspace.name, pair2, txn)
      entityDAO.save(workspace.namespace, workspace.name, ps1, txn)
      entityDAO.save(workspace.namespace, workspace.name, sset1, txn)
      entityDAO.save(workspace.namespace, workspace.name, sset2, txn)
      entityDAO.save(workspace.namespace, workspace.name, sset3, txn)
      entityDAO.save(workspace.namespace, workspace.name, sset4, txn)
      //entityDAO.save(workspace.namespace, workspace.name, sset_empty, txn)
      entityDAO.save(workspace.namespace, workspace.name, indiv1, txn)

      methodConfigDAO.save(workspace.namespace, workspace.name, methodConfig, txn)
      methodConfigDAO.save(workspace.namespace, workspace.name, methodConfigValid, txn)
      methodConfigDAO.save(workspace.namespace, workspace.name, methodConfigUnparseable, txn)
      methodConfigDAO.save(workspace.namespace, workspace.name, methodConfigNotAllSamples, txn)
      methodConfigDAO.save(workspace.namespace, workspace.name, methodConfigAttrTypeMixup, txn)

      submissionDAO.save(workspace.namespace, workspace.name, submission1, txn)
      submissionDAO.save(workspace.namespace, workspace.name, submission2, txn)
    }
  }
  val testData = new DefaultTestData()

  def withEmptyTestDatabase(testCode:DataSource => Any):Unit = {
    val emptyData = new TestData() {
      override def save(txn: RawlsTransaction): Unit = {
        // no op
      }
    }

    withCustomTestDatabase(emptyData)(testCode)
  }
  def withDefaultTestDatabase(testCode:DataSource => Any):Unit = {
    withCustomTestDatabase(testData)(testCode)
  }
  def withCustomTestDatabase(data:TestData)(testCode:DataSource => Any):Unit = {
    val dbName = UUID.randomUUID.toString
    val dataSource = DataSource("memory:"+dbName, "admin", "admin")
    val graph = new OrientGraph("memory:"+dbName)
    // save the data inside a transaction to cause data to be committed
    dataSource inTransaction { txn =>
      data.save(txn)
    }

    testCode(dataSource)
    graph.rollback()
    graph.drop()
    graph.shutdown()
  }
}
