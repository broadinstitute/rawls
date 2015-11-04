package org.broadinstitute.dsde.rawls.graph

import java.util.logging.{LogManager, Logger}

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.joda.time.DateTime
import spray.http.OAuth2BearerToken
import org.scalatest.BeforeAndAfterAll
import java.util.UUID

import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext

trait OrientDbTestFixture extends BeforeAndAfterAll {
  this : org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  val testDate = new DateTime()
  val userInfo = UserInfo("test_token", OAuth2BearerToken("token"), 123, "123456789876543212345")

  override def beforeAll: Unit = {
    // TODO find a better way to set the log level. Nothing else seems to work.
    LogManager.getLogManager().reset()
    Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)
  }

  override def afterAll: Unit = {}

  lazy val entityDAO: GraphEntityDAO = new GraphEntityDAO()
  lazy val workspaceDAO = new GraphWorkspaceDAO()
  lazy val methodConfigDAO = new GraphMethodConfigurationDAO()
  lazy val authDAO = new GraphAuthDAO()
  lazy val submissionDAO = new GraphSubmissionDAO()

  val containerDAO = GraphContainerDAO(
    new GraphWorkflowDAO(submissionDAO),
    workspaceDAO,
    entityDAO,
    methodConfigDAO,
    authDAO,
    submissionDAO
  )

  abstract class TestData {
    def save(txn:RawlsTransaction)
  }

  def createTestSubmission(workspace: Workspace, methodConfig: MethodConfiguration, submissionEntity: Entity, workflowEntities: Seq[Entity]) = {
    val workflows = (workflowEntities collect {
      case ref: Entity => Workflow("workflow_" + UUID.randomUUID.toString, WorkflowStatuses.Submitted, testDate, AttributeEntityReference(ref.entityType, ref.name))
    })

    Submission("submission_" + UUID.randomUUID.toString, testDate, "testUser", methodConfig.namespace, methodConfig.name, AttributeEntityReference(submissionEntity.entityType, submissionEntity.name),
      workflows,
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)
  }

  def makeRawlsGroup(name: String, users: Set[RawlsUserRef], groups: Set[RawlsGroupRef]) =
    RawlsGroup(RawlsGroupName(name), RawlsGroupEmail("dummy@example.com"), users, groups)

  class EmptyWorkspace() extends TestData {
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val workspace = Workspace(wsName.namespace, wsName.name, "aWorkspaceId", "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map.empty)

    override def save(txn:RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
    }
  }

  class LockedWorkspace() extends TestData {
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val workspace = Workspace(wsName.namespace, wsName.name, "aWorkspaceId", "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map.empty, isLocked = true )

    override def save(txn:RawlsTransaction): Unit = {
      workspaceDAO.save(workspace, txn)
    }
  }

  class DefaultTestData() extends TestData {
    // setup workspace objects
    val owner = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("testwsOwners", Set(owner), Set.empty)

    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsAttrs = Map(
      "string" -> AttributeString("yep, it's a string"),
      "number" -> AttributeNumber(10),
      "empty" -> AttributeEmptyList,
      "values" -> AttributeValueList(Seq(AttributeString("another string"), AttributeBoolean(true)))
    )
    val workspace = Workspace(wsName.namespace, wsName.name, "aWorkspaceId", "aBucket", DateTime.now, DateTime.now, "testUser", wsAttrs, Map(WorkspaceAccessLevels.Owner -> ownerGroup))

    val sample1 = Entity("sample1", "Sample",
      Map(
        "type" -> AttributeString("normal"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeBoolean(true))),
        "quot" -> AttributeEntityReference("Aliquot", "aliquot1"),
        "somefoo" -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeEntityReference("Sample", "sample1") ) )
    val sample4 = Entity("sample4", "Sample", Map("type" -> AttributeString("tumor")))
    var sample5 = Entity("sample5", "Sample", Map("type" -> AttributeString("tumor")))
    var sample6 = Entity("sample6", "Sample", Map("type" -> AttributeString("tumor")))
    var sample7 = Entity("sample7", "Sample", Map("type" -> AttributeString("tumor"), "cycle" -> AttributeEntityReference("Sample", "sample6")))

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map( "case" -> AttributeEntityReference("Sample", "sample2"),
        "control" -> AttributeEntityReference("Sample", "sample1") ) )
    val pair2 = Entity("pair2", "Pair",
      Map( "case" -> AttributeEntityReference("Sample", "sample3"),
        "control" -> AttributeEntityReference("Sample", "sample1") ) )

    val sset1 = Entity("sset1", "SampleSet",
      Map( "samples" -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample1"),
        AttributeEntityReference("Sample", "sample2"),
        AttributeEntityReference("Sample", "sample3"))) ) )
    val sset2 = new Entity("sset2", "SampleSet",
      Map( "samples" -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample2"))) ) )

    val sset3 = Entity("sset3", "SampleSet",
      Map("hasSamples" -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample5"),
        AttributeEntityReference("Sample", "sample6")))))

    val sset4 = Entity("sset4", "SampleSet",
      Map("hasSamples" -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample7")))))

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map( "samples" -> AttributeEmptyList ))

    val ps1 = Entity("ps1", "PairSet",
      Map( "pairs" -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Pair", "pair1"),
        AttributeEntityReference("Pair", "pair2"))) ) )

    val indiv1 = Entity("indiv1", "Individual",
      Map( "sset" -> AttributeEntityReference("SampleSet", "sset1") ) )

    val methodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      "Sample",
      Map("p1" -> AttributeString("prereq")),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      MethodRepoMethod("ns-config", "meth1", 1)
    )

    val methodConfig2 = MethodConfiguration("dsde", "testConfig2", "Sample", Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), MethodRepoMethod(wsName.namespace, "method-a", 1))
    val methodConfig3 = MethodConfiguration("dsde", "testConfig", "Sample", Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo"), "param2"-> AttributeString("foo2")), Map("out" -> AttributeString("bar")), MethodRepoMethod("ns-config", "meth1", 1))

    val methodConfigEntityUpdate = MethodConfiguration("ns", "testConfig1", "Sample", Map(), Map(), Map("o1" -> AttributeString("this.foo")), MethodRepoMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns", "testConfig1", "Sample", Map(), Map(), Map("o1" -> AttributeString("workspace.foo")), MethodRepoMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.type")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigUnparseable = MethodConfiguration("dsde", "UnparseableMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.confused")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigValidExprs = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty,
      inputs=Map("foo" -> AttributeString("this.thing.foo"), "bar" -> AttributeString("workspace.bar")),
      outputs=Map("foo" -> AttributeString("this.foo"), "bar" -> AttributeString("workspace.bar")),
      MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigInvalidExprs = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty,
      inputs=Map("foo" -> AttributeString("bonk.thing.foo"), "bar" -> AttributeString("workspace.bar")),
      outputs=Map("foo" -> AttributeString("this.bonk.foo"), "bar" -> AttributeString("foo.bar")),
      MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, wsName)
    val methodConfigName2 = methodConfigName.copy(name="novelName")
    val methodConfigName3 = methodConfigName.copy(name="noSuchName")
    val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName,methodConfigName2)
    val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName,methodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, wsName)
    val methodRepoGood = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, newMethodConfigName)
    val methodRepoMissing = MethodRepoConfigurationImport("workspace_test", "rawls_test_missing", 1, methodConfigName)
    val methodRepoEmptyPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_empty_payload", 1, methodConfigName)
    val methodRepoBadPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_bad_payload", 1, methodConfigName)

    val submission1 = createTestSubmission(workspace, methodConfig, indiv1, Seq(sample1, sample2, sample3))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, Seq(sample1, sample2, sample3))

    val submissionUpdateEntity = createTestSubmission(workspace, methodConfigEntityUpdate, indiv1, Seq(indiv1))
    val submissionUpdateWorkspace = createTestSubmission(workspace, methodConfigWorkspaceUpdate, indiv1, Seq(indiv1))

    val submissionTerminateTest = Submission("submissionTerminate",testDate, "testUser",methodConfig.namespace,methodConfig.name,AttributeEntityReference(indiv1.entityType, indiv1.name),
      Seq(Workflow("workflowA",WorkflowStatuses.Submitted,testDate,AttributeEntityReference(sample1.entityType, sample1.name)),
        Workflow("workflowB",WorkflowStatuses.Submitted,testDate,AttributeEntityReference(sample2.entityType, sample2.name)),
        Workflow("workflowC",WorkflowStatuses.Submitted,testDate,AttributeEntityReference(sample3.entityType, sample3.name)),
        Workflow("workflowD",WorkflowStatuses.Submitted,testDate,AttributeEntityReference(sample4.entityType, sample4.name))), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    override def save(txn:RawlsTransaction): Unit = {
      authDAO.saveUser(owner, txn)
      authDAO.saveGroup(ownerGroup, txn)
      workspaceDAO.save(workspace, txn)
      withWorkspaceContext(workspace, txn, bSkipLockCheck=true) { context =>
        entityDAO.save(context, aliquot1, txn)
        entityDAO.save(context, aliquot2, txn)
        entityDAO.save(context, sample1, txn)
        entityDAO.save(context, sample2, txn)
        entityDAO.save(context, sample3, txn)
        entityDAO.save(context, sample4, txn)
        entityDAO.save(context, sample5, txn)
        entityDAO.save(context, sample6, txn)
        entityDAO.save(context, sample7, txn)
        entityDAO.save(context, pair1, txn)
        entityDAO.save(context, pair2, txn)
        entityDAO.save(context, ps1, txn)
        entityDAO.save(context, sset1, txn)
        entityDAO.save(context, sset2, txn)
        entityDAO.save(context, sset3, txn)
        entityDAO.save(context, sset4, txn)
        entityDAO.save(context, sset_empty, txn)
        entityDAO.save(context, indiv1, txn)

        methodConfigDAO.save(context, methodConfig, txn)
        methodConfigDAO.save(context, methodConfig2, txn)
        methodConfigDAO.save(context, methodConfigValid, txn)
        methodConfigDAO.save(context, methodConfigUnparseable, txn)
        methodConfigDAO.save(context, methodConfigNotAllSamples, txn)
        methodConfigDAO.save(context, methodConfigAttrTypeMixup, txn)

        submissionDAO.save(context, submissionTerminateTest, txn)
        submissionDAO.save(context, submission1, txn)
        submissionDAO.save(context, submission2, txn)
        submissionDAO.save(context, submissionUpdateEntity, txn)
        submissionDAO.save(context, submissionUpdateWorkspace, txn)
      }
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
    try {
      val graph = new OrientGraph("memory:"+dbName)

      // do this twice to make sure it is idempotent
      VertexSchema.createVertexClasses(graph)
      VertexSchema.createVertexClasses(graph)

      // save the data inside a transaction to cause data to be committed
      dataSource.inTransaction() { txn =>
        data.save(txn)
      }

      testCode(dataSource)
      graph.rollback()
      graph.drop()
      graph.shutdown()
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    }
  }

  /**
   * Only set bSkipLockCheck = true from fixture save calls.
   */
  def withWorkspaceContext[T](workspace: Workspace, txn: RawlsTransaction, bSkipLockCheck:Boolean=false)(testCode: WorkspaceContext => T) = {
    if( !bSkipLockCheck ) {
      assert( txn.readLocks.contains(workspace.toWorkspaceName) || txn.writeLocks.contains(workspace.toWorkspaceName),
        s"Attempting to use context on workspace ${workspace.toWorkspaceName} but it's not read or write locked! Add it to inTransaction or inFutureTransaction")
    }
    val workspaceContext = workspaceDAO.loadContext(workspace.toWorkspaceName, txn).getOrElse(throw new RawlsException(s"Unable to load workspaceContext for ${workspace.toWorkspaceName}"))
    testCode(workspaceContext)
  }
}
