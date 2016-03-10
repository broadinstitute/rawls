package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, TestExecutionContext}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, BeforeAndAfterAll, Matchers}
import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcProfile
import _root_.slick.driver.H2Driver.api._
import spray.http.OAuth2BearerToken

import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Created by dvoet on 2/3/16.
 */
trait TestDriverComponent extends DriverComponent with DataAccess {

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  val databaseConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig[JdbcProfile]("h2mem1")
  override val driver: JdbcProfile = databaseConfig.driver
  val database = databaseConfig.db
  val slickDataSource = new SlickDataSource(databaseConfig)

  val testDate = new DateTime()
  val userInfo = UserInfo("test_token", OAuth2BearerToken("token"), 123, "123456789876543212345")

  protected def runAndWait[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect], duration: Duration = 1 minutes): R = {
    Await.result(database.run(action.transactionally), duration)
  }

  import driver.api._

  def createTestSubmission(workspace: Workspace, methodConfig: MethodConfiguration, submissionEntity: Entity, rawlsUserRef: RawlsUserRef, workflowEntities: Seq[Entity], inputResolutions: Map[Entity, Seq[SubmissionValidationValue]]) = {
    val workflows = (workflowEntities collect {
      case ref: Entity => Workflow(UUID.randomUUID.toString, WorkflowStatuses.Submitted, testDate, Option(AttributeEntityReference(ref.entityType, ref.name)), inputResolutions(ref))
    })

    Submission(UUID.randomUUID.toString, testDate, rawlsUserRef, methodConfig.namespace, methodConfig.name, Option(AttributeEntityReference(submissionEntity.entityType, submissionEntity.name)),
      workflows,
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)
  }

  def makeRawlsGroup(name: String, users: Set[RawlsUserRef]) =
    RawlsGroup(RawlsGroupName(name), RawlsGroupEmail(s"$name@example.com"), users, Set.empty)

  class EmptyWorkspace() extends TestData {
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map(
      WorkspaceAccessLevels.Owner -> ownerGroup,
      WorkspaceAccessLevels.Write -> writerGroup,
      WorkspaceAccessLevels.Read -> readerGroup))

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        workspaceQuery.save(workspace)
      )
    }
  }

  class LockedWorkspace() extends TestData {
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", DateTime.now, DateTime.now, "testUser", Map.empty, Map(
      WorkspaceAccessLevels.Owner -> ownerGroup,
      WorkspaceAccessLevels.Write -> writerGroup,
      WorkspaceAccessLevels.Read -> readerGroup), isLocked = true )

    override def save() = {
      DBIO.seq (
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        workspaceQuery.save(workspace)
      )
    }
  }

  class DefaultTestData() extends TestData {
    // setup workspace objects
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), Set(RawlsUser(userInfo)), "testBucketUrl")

    val wsAttrs = Map(
      "string" -> AttributeString("yep, it's a string"),
      "number" -> AttributeNumber(10),
      "empty" -> AttributeEmptyList,
      "values" -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspaceNoGroups = Workspace(wsName.namespace, wsName.name + "3", UUID.randomUUID().toString, "aBucket2", DateTime.now, DateTime.now, "testUser", wsAttrs, Map.empty)

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", DateTime.now, DateTime.now, "testUser", wsAttrs, Map(
      WorkspaceAccessLevels.Owner -> ownerGroup,
      WorkspaceAccessLevels.Write -> writerGroup,
      WorkspaceAccessLevels.Read -> readerGroup))

    val sample1 = Entity("sample1", "Sample",
      Map(
        "type" -> AttributeString("normal"),
        "whatsit" -> AttributeNumber(100),
        "thingies" -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        "quot" -> AttributeEntityReference("Aliquot", "aliquot1"),
        "somefoo" -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map( "type" -> AttributeString("tumor"), "tumortype" -> AttributeString("LUSC"), "confused" -> AttributeEntityReference("Sample", "sample1") ) )
    val sample4 = Entity("sample4", "Sample", Map("type" -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map("type" -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map("type" -> AttributeString("tumor")))
    val sample7 = Entity("sample7", "Sample", Map("type" -> AttributeString("tumor"), "cycle" -> AttributeEntityReference("Sample", "sample6")))

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

    val inputResolutions = Seq(SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name"))

    val submission1 = createTestSubmission(workspace, methodConfig, indiv1, userOwner, Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, userOwner, Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions))

    val submissionUpdateEntity = createTestSubmission(workspace, methodConfigEntityUpdate, indiv1, userOwner, Seq(indiv1), Map(indiv1 -> inputResolutions))
    val submissionUpdateWorkspace = createTestSubmission(workspace, methodConfigWorkspaceUpdate, indiv1, userOwner, Seq(indiv1), Map(indiv1 -> inputResolutions))

    val submissionTerminateTest = Submission("submissionTerminate",testDate, userOwner,methodConfig.namespace,methodConfig.name,Option(AttributeEntityReference(indiv1.entityType, indiv1.name)),
      Seq(Workflow("workflowA",WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), inputResolutions),
        Workflow("workflowB",WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample2.entityType, sample2.name)), inputResolutions),
        Workflow("workflowC",WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample3.entityType, sample3.name)), inputResolutions),
        Workflow("workflowD",WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample4.entityType, sample4.name)), inputResolutions)), Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(RawlsUser(userInfo)),
        rawlsBillingProjectQuery.save(billingProject),
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        workspaceQuery.save(workspace),
        workspaceQuery.save(workspaceNoGroups),
        withWorkspaceContext(workspace)({ context =>
          DBIO.seq(
                entityQuery.save(context, aliquot1),
                entityQuery.save(context, aliquot2),
                entityQuery.save(context, sample1),
                entityQuery.save(context, sample2),
                entityQuery.save(context, sample3),
                entityQuery.save(context, sample4),
                entityQuery.save(context, sample5),
                entityQuery.save(context, sample6),
                entityQuery.save(context, sample7),
                entityQuery.save(context, pair1),
                entityQuery.save(context, pair2),
                entityQuery.save(context, ps1),
                entityQuery.save(context, sset1),
                entityQuery.save(context, sset2),
                entityQuery.save(context, sset3),
                entityQuery.save(context, sset4),
                entityQuery.save(context, sset_empty),
                entityQuery.save(context, indiv1),

                methodConfigurationQuery.save(context, methodConfig),
                methodConfigurationQuery.save(context, methodConfig2),
                methodConfigurationQuery.save(context, methodConfigValid),
                methodConfigurationQuery.save(context, methodConfigUnparseable),
                methodConfigurationQuery.save(context, methodConfigNotAllSamples),
                methodConfigurationQuery.save(context, methodConfigAttrTypeMixup)
  //
  //              submissionDAO.save(context, submissionTerminateTest, txn)
  //              submissionDAO.save(context, submission1, txn)
  //              submissionDAO.save(context, submission2, txn)
  //              submissionDAO.save(context, submissionUpdateEntity, txn)
  //              submissionDAO.save(context, submissionUpdateWorkspace, txn)
          )
        })
      )
    }
  }

  val testData = new DefaultTestData()

  def withEmptyTestDatabase(testCode: => Any):Unit = {
    val emptyData = new TestData() {
      override def save() = {
        DBIO.successful(Unit)
      }
    }

    withCustomTestDatabase(emptyData)(testCode)
  }

  def withDefaultTestDatabase(testCode: => Any):Unit = {
    withCustomTestDatabase(testData)(testCode)
  }

  def withDefaultTestDatabase(testCode:SlickDataSource => Any):Unit = {
    withCustomTestDatabase(testData)(testCode(slickDataSource))
  }

  def withCustomTestDatabase(data:TestData)(testCode: => Any):Unit = {
    try {
      runAndWait(allSchemas.create)
      runAndWait(data.save())
      testCode
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    } finally {
      runAndWait(allSchemas.drop)
    }
  }

  def withWorkspaceContext[T](workspace: Workspace)(testCode: (SlickWorkspaceContext) => T): T = {
    testCode(SlickWorkspaceContext(workspace))
  }
}

trait TestData {
  def save(): ReadWriteAction[Unit]
}

trait TestDriverComponentWithFlatSpecAndMatchers extends FlatSpec with TestDriverComponent with Matchers
