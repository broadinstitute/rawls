package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.typesafe.config.ConfigFactory
import nl.grons.metrics.scala.{Counter, DefaultInstrumented, MetricName}
import org.broadinstitute.dsde.rawls.{TestExecutionContext, model}
import slick.basic.DatabaseConfig
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.JdbcProfile
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.ProjectRoles.Owner
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.{ProjectOwner, WorkspaceAccessLevel}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers, Suite}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// initialize database tables and connection pool only once
object DbResource {
  // to override, e.g. to run against mysql:
  // $ sbt -Dtestdb=mysql test
  private val testdb = ConfigFactory.load.getStringOr("testdb", "mysql")
  private val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())

  val dataConfig = DatabaseConfig.forConfig[JdbcProfile](testdb)

  private val liquibaseConf = ConfigFactory.load().getConfig("liquibase")
  private val liquibaseChangeLog = liquibaseConf.getString("changelog")

  val dataSource = new SlickDataSource(dataConfig)(TestExecutionContext.testExecutionContext)
  dataSource.initWithLiquibase(liquibaseChangeLog, Map.empty)
}

/**
 * Created by dvoet on 2/3/16.
 */
trait TestDriverComponent extends DriverComponent with DataAccess with DefaultInstrumented {
  this: Suite =>

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  // Implicit counters are required for certain methods on WorkflowComponent and SubmissionComponent
  override lazy val metricBaseName = MetricName("test")
  implicit def wfStatusCounter(wfStatus: WorkflowStatus): Option[Counter] = Option(metrics.counter(s"${wfStatus.toString}"))
  implicit def subStatusCounter(subStatus: SubmissionStatus): Counter = metrics.counter(s"${subStatus.toString}")

  override val driver: JdbcProfile = DbResource.dataConfig.driver
  override val batchSize: Int = DbResource.dataConfig.config.getInt("batchSize")
  val slickDataSource = DbResource.dataSource

  val userInfo = UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345"))

  // NOTE: we previously truncated millis here for DB compatibility reasons, but this is is no longer necessary.
  // now only serves to encapsulate a Java-ism
  def currentTime() = new DateTime()
  val testDate = currentTime()

  protected def runAndWait[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect], duration: Duration = 1 minutes): R = {
    Await.result(DbResource.dataSource.inTransaction { _ => action.asInstanceOf[ReadWriteAction[R]] }, duration)
  }

  protected def runMultipleAndWait[R](count: Int, duration: Duration = 1 minutes)(actionGenerator: Int => DBIOAction[R, _ <: NoStream, _ <: Effect]): R = {
    val futures = (1 to count) map { i => retryConcurrentModificationException(actionGenerator(i)) }
    Await.result(Future.sequence(futures), duration).head
  }

  private def retryConcurrentModificationException[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect]): Future[R] = {
    DbResource.dataSource.database.run(action.map{ x => Thread.sleep((Math.random() * 500).toLong); x }).recoverWith {
      case e: RawlsConcurrentModificationException => retryConcurrentModificationException(action)
      case rollbackException: MySQLTransactionRollbackException if rollbackException.getMessage.contains("try restarting transaction") => retryConcurrentModificationException(action)
    }
  }

  import driver.api._

  def createTestSubmission(workspace: Workspace, methodConfig: MethodConfiguration, submissionEntity: Entity, rawlsUserEmail: WorkbenchEmail,
                           workflowEntities: Seq[Entity], inputResolutions: Map[Entity, Seq[SubmissionValidationValue]],
                           failedWorkflowEntities: Seq[Entity], failedInputResolutions: Map[Entity, Seq[SubmissionValidationValue]],
                           status: WorkflowStatus = WorkflowStatuses.Submitted, useCallCache: Boolean = false,
                           workflowFailureMode: Option[WorkflowFailureMode] = None, individualWorkflowCost: Option[Float] = None): Submission = {

    val workflows = workflowEntities map { ref =>
      val uuid = if(status == WorkflowStatuses.Queued) None else Option(UUID.randomUUID.toString)
      Workflow(uuid, status, testDate, Some(ref.toReference), inputResolutions(ref), cost = individualWorkflowCost)
    }

    Submission(UUID.randomUUID.toString, testDate, rawlsUserEmail, methodConfig.namespace, methodConfig.name, Some(submissionEntity.toReference),
      workflows, SubmissionStatuses.Submitted, useCallCache, workflowFailureMode, individualWorkflowCost.map (_ * workflows.length))
  }

  def billingProjectFromName(name: String) = (RawlsBillingProject(RawlsBillingProjectName(name), "mockBucketUrl", CreationStatuses.Ready, None, None))

  def makeRawlsGroup(name: String, users: Set[RawlsUserRef], groups: Set[RawlsGroupRef] = Set.empty) =
    RawlsGroup(RawlsGroupName(name), RawlsGroupEmail(s"$name@example.com"), users, groups)

  def makeWorkspaceWithUsers(project: RawlsBillingProject,
                    name: String,
                    workspaceId: String,
                    bucketName: String,
                    workflowCollectionName: Option[String],
                    createdDate: DateTime,
                    lastModified: DateTime,
                    createdBy: String,
                    attributes: AttributeMap,
                    isLocked: Boolean) = {

    Workspace(project.projectName.value, name, workspaceId, bucketName, workflowCollectionName, createdDate, createdDate, createdBy, attributes, isLocked)
  }

  class EmptyWorkspace() extends TestData {
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo(RawlsUserEmail("writer-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(RawlsUserEmail("reader-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace)
      )
    }
  }

  class LockedWorkspace() extends TestData {
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo(RawlsUserEmail("writer-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(RawlsUserEmail("reader-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty, isLocked = true)

    override def save() = {
      DBIO.seq (
        workspaceQuery.save(workspace)
      )
    }
  }

  class DefaultTestData() extends TestData {
    // setup workspace objects
    val userProjectOwner = RawlsUser(UserInfo(RawlsUserEmail("project-owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543210101")))
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo(RawlsUserEmail("writer-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(RawlsUserEmail("reader-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))
    val userReaderViaGroup = RawlsUser(UserInfo(RawlsUserEmail("reader-access-via-group"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349")))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val wsName3 = WorkspaceName("myNamespace", "myWSwithADsMethodConfigs")
    val wsName4 = WorkspaceName("myNamespace", "myWSwithADsMCSuccessfulSubmission")
    val wsName5 = WorkspaceName("myNamespace", "myWSwithADsMCFailedSubmission")
    val wsName6 = WorkspaceName("myNamespace", "myWSwithADsMCSubmittedSubmission")
    val wsName7 = WorkspaceName("myNamespace", "myWSwithADsMCAbortedSubmission")
    val wsName8 = WorkspaceName("myNamespace", "myWSwithADsMCAbortedSuccessfulSub")
    val wsName9 = WorkspaceName("myNamespace", "myWSToTestGrantPermissions")
    val wsName10 = WorkspaceName("myNamespace", "myMultiGroupADWorkspace")
    val wsNameConfigCopyDestination = WorkspaceName("myNamespace", "configCopyDestinationWS")
    val wsInterleaved = WorkspaceName("myNamespace", "myWSToTestInterleavedSubs")
    val wsWorkflowFailureMode = WorkspaceName("myNamespace", "myWSToTestWFFailureMode")
    val workspaceToTestGrantId = UUID.randomUUID()

    val nestedProjectGroup = makeRawlsGroup("nested_project_group", Set(userOwner))
    val dbGapAuthorizedUsersGroup = ManagedGroupRef(RawlsGroupName("dbGapAuthorizedUsers"))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)

    val testProject1Name = RawlsBillingProjectName("arbitrary")
    val testProject1 = RawlsBillingProject(testProject1Name, "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None, Some("my_backend"))

    val testProject2Name = RawlsBillingProjectName("project2")
    val testProject2 = RawlsBillingProject(testProject2Name, "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None)

    val testProject3Name = RawlsBillingProjectName("project3")
    val testProject3 = RawlsBillingProject(testProject3Name, "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None)

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspaceNoGroups = Workspace(wsName.namespace, wsName.name + "3", UUID.randomUUID().toString, "aBucket2", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs)

    val (workspace) = makeWorkspaceWithUsers(billingProject, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    val workspacePublished = Workspace(wsName.namespace, wsName.name + "_published", UUID.randomUUID().toString, "aBucket3", Some("workflow-collection"), currentTime(), currentTime(), "testUser",
      wsAttrs + (AttributeName.withLibraryNS("published") -> AttributeBoolean(true)))
    val workspaceNoAttrs = Workspace(wsName.namespace, wsName.name + "_noattrs", UUID.randomUUID().toString, "aBucket4", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

    val realm = ManagedGroupRef(RawlsGroupName("Test-Realm"))
    val realmWsName = wsName.name + "withRealm"

    val realm2 = ManagedGroupRef(RawlsGroupName("Test-Realm2"))
    val realmWs2Name = wsName2.name + "withRealm"

    val (workspaceWithRealm) = makeWorkspaceWithUsers(billingProject, realmWsName, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    val (workspaceWithMultiGroupAD) = makeWorkspaceWithUsers(billingProject, wsName10.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    val (controlledWorkspace) = makeWorkspaceWithUsers(billingProject, "test-tcga", UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    val (otherWorkspaceWithRealm) = makeWorkspaceWithUsers(billingProject, realmWs2Name,  UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, without submissions
    val (workspaceNoSubmissions) = makeWorkspaceWithUsers(billingProject, wsName3.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with successful submission
    val (workspaceSuccessfulSubmission) = makeWorkspaceWithUsers(billingProject, wsName4.name , UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with failed submission
    val (workspaceFailedSubmission) = makeWorkspaceWithUsers(billingProject, wsName5.name , UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with submitted submission
    val (workspaceSubmittedSubmission) = makeWorkspaceWithUsers(billingProject, wsName6.name , UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms with mixed workflows
    val (workspaceMixedSubmissions) = makeWorkspaceWithUsers(billingProject, wsName7.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with aborted and successful submissions
    val (workspaceTerminatedSubmissions) = makeWorkspaceWithUsers(billingProject, wsName8.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with a successful submission that had another submission run and fail while it was running
    val (workspaceInterleavedSubmissions) = makeWorkspaceWithUsers(billingProject, wsInterleaved.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with a custom workflow failure mode
    val (workspaceWorkflowFailureMode) = makeWorkspaceWithUsers(billingProject, wsWorkflowFailureMode.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Standard workspace to test grant permissions
    val (workspaceToTestGrant) = makeWorkspaceWithUsers(billingProject, wsName9.name, workspaceToTestGrantId.toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Test copying configs between workspaces
    val (workspaceConfigCopyDestination) = makeWorkspaceWithUsers(billingProject, wsNameConfigCopyDestination.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs, false)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val sample1 = Entity("sample1", "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> aliquot1.toReference,
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> sample1.toReference ) )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("cycle") -> sample6.toReference))
    val sample8 = Entity("sample8", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("foo_id") -> AttributeString("1029384756")))
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map(AttributeName.withDefaultNS("case") -> sample2.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference,
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")) )
    val pair2 = Entity("pair2", "Pair",
      Map(AttributeName.withDefaultNS("case") -> sample3.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference) )

    val sset1 = Entity("sset1", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
        Seq(sample1.toReference, sample2.toReference, sample3.toReference))))
    val sset2 = Entity("sset2", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(sample2.toReference)) ) )

    val sset3 = Entity("sset3", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(sample5.toReference, sample6.toReference))))

    val sset4 = Entity("sset4", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(sample7.toReference))))

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList ))

    val ps1 = Entity("ps1", "PairSet",
      Map(AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList( Seq(pair1.toReference, pair2.toReference)) ) )

    val indiv1 = Entity("indiv1", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> sset1.toReference ) )

    val indiv2 = Entity("indiv2", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> sset2.toReference ) )

    val agoraMethod = AgoraMethod("ns-config", "meth1", 1)

    val agoraMethodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      Some("Sample"),
      Map("p1" -> AttributeString("prereq")),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      agoraMethod
    )

    val goodAndBadMethod = AgoraMethod("dsde", "good_and_bad", 1)

    val goodAndBadMethodConfig = MethodConfiguration(
      "dsde",
      "good_and_bad",
      Some("samples"),
      Map(),
      Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.foo"), "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("does.not.parse")),
      Map("goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"), "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("also.does.not.parse"), "empty_out" -> AttributeString("")),
      goodAndBadMethod)

    val dockstoreMethod = DockstoreMethod("dockstore-method-path", "dockstore-method-version")

    val dockstoreMethodConfig = MethodConfiguration(
      "dockstore-config-namespace",
      "dockstore-config-name",
      Some("Sample"),
      Map("p1" -> AttributeString("prereq")),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      dockstoreMethod
    )

    val methodConfigDockstore = MethodConfiguration("dsde", "DockstoreConfig", Some("Sample"), Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo"), "param2"-> AttributeString("foo2")), Map("out" -> AttributeString("bar")), DockstoreMethod("dockstore-path", "dockstore-version"))
    val methodConfig2 = MethodConfiguration("dsde", "testConfig2", Some("Sample"), Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), AgoraMethod(wsName.namespace, "method-a", 1))
    val methodConfig3 = MethodConfiguration("dsde", "testConfig", Some("Sample"), Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo"), "param2"-> AttributeString("foo2")), Map("out" -> AttributeString("bar")), AgoraMethod("ns-config", "meth1", 1))

    val methodConfigEntityUpdate = MethodConfiguration("ns", "testConfig11", Some("Sample"), Map(), Map(), Map("o1" -> AttributeString("this.foo")), AgoraMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns", "testConfig1", Some("Sample"), Map(), Map(), Map("o1" -> AttributeString("workspace.foo")), AgoraMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceLibraryUpdate = MethodConfiguration("ns", "testConfigLib", Some("Sample"), Map(), Map(), Map("o1" -> AttributeString("workspace.library:foo")), AgoraMethod("ns-config", "meth1", 1))
    val methodConfigMissingOutputs = MethodConfiguration("ns", "testConfigMissingOutputs", Some("Sample"), Map(), Map(), Map("some.workflow.output" -> AttributeString("this.might_not_be_here")), AgoraMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigUnparseableInputs = MethodConfiguration("dsde", "UnparseableInputsMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigUnparseableOutputs = MethodConfiguration("dsde", "UnparseableOutputsMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map("three_step.cgrep.count" -> AttributeString("this..wont.parse")), AgoraMethod("dsde", "three_step", 1))
    val methodConfigUnparseableBoth = MethodConfiguration("dsde", "UnparseableBothMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..is...bad")), outputs=Map("three_step.cgrep.count" -> AttributeString("this..wont.parse")), AgoraMethod("dsde", "three_step", 1))
    val methodConfigEmptyOutputs = MethodConfiguration("dsde", "EmptyOutputsMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map("three_step.cgrep.count" -> AttributeString("")), AgoraMethod("dsde", "three_step", 1))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.confused")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))

    val methodConfigArrayType = MethodConfiguration("dsde", "ArrayMethodConfig", Some("SampleSet"), prerequisites=Map.empty,
      inputs=Map("aggregate_data_workflow.aggregate_data.input_array" -> AttributeString("this.samples.type")),
      outputs=Map("aggregate_data_workflow.aggregate_data.output_array" -> AttributeString("this.output_array")),
      AgoraMethod("dsde", "array_task", 1))

    val methodConfigEntityless = MethodConfiguration("ns", "Entityless", None, Map(), inputs=Map("three_step.cgrep.pattern" -> AttributeString("\"bees\"")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))

    val agoraMethodConfigName = MethodConfigurationName(agoraMethodConfig.name, agoraMethodConfig.namespace, wsName)
    val dockstoreMethodConfigName = MethodConfigurationName(dockstoreMethodConfig.name, dockstoreMethodConfig.namespace, wsName)
    val methodConfigName2 = agoraMethodConfigName.copy(name="novelName")
    val methodConfigName3 = agoraMethodConfigName.copy(name="noSuchName")
    val methodConfigName4 = agoraMethodConfigName.copy(name=methodConfigWorkspaceLibraryUpdate.name)
    val methodConfigNamePairCreated = MethodConfigurationNamePair(agoraMethodConfigName,methodConfigName2)
    // Copy from "myNamespace/myWorkspace" to "myNamespace/configCopyDestinationWS"
    val methodConfigNamePairCreatedDockstore = MethodConfigurationNamePair(dockstoreMethodConfigName, dockstoreMethodConfigName.copy(workspaceName = wsNameConfigCopyDestination))
    val methodConfigNamePairConflict = MethodConfigurationNamePair(agoraMethodConfigName,agoraMethodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)
    val methodConfigNamePairFromLibrary = MethodConfigurationNamePair(methodConfigName4,methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, agoraMethodConfig.namespace, wsName)
    val methodRepoGood = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, newMethodConfigName)
    val methodRepoMissing = MethodRepoConfigurationImport("workspace_test", "rawls_test_missing", 1, agoraMethodConfigName)
    val methodRepoEmptyPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_empty_payload", 1, agoraMethodConfigName)
    val methodRepoBadPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_bad_payload", 1, agoraMethodConfigName)
    val methodRepoLibrary = MethodRepoConfigurationImport("workspace_test", "rawls_test_library", 1, newMethodConfigName)

    val inputResolutions = Seq(SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name"))
    val inputResolutions2 = Seq(SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2"))
    val missingOutputResolutions = Seq(SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name"))

    val submissionNoWorkflows = createTestSubmission(workspace, agoraMethodConfig, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq.empty, Map.empty,
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission1 = createTestSubmission(workspace, agoraMethodConfig, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val costedSubmission1 = createTestSubmission(workspace, agoraMethodConfig, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2),
      // the constant value we set in MockSubmissionCostService
      individualWorkflowCost = Some(1.23f))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))

    val submissionUpdateEntity = createTestSubmission(workspace, methodConfigEntityUpdate, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(indiv1), Map(indiv1 -> inputResolutions),
      Seq(indiv2), Map(indiv2 -> inputResolutions2))
    val submissionUpdateWorkspace = createTestSubmission(workspace, methodConfigWorkspaceUpdate, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(indiv1), Map(indiv1 -> inputResolutions),
      Seq(indiv2), Map(indiv2 -> inputResolutions2))

    //NOTE: This is deliberately not saved in the list of active submissions!
    val submissionMissingOutputs = createTestSubmission(workspace, methodConfigMissingOutputs, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(indiv1), Map(indiv1 -> missingOutputResolutions), Seq(), Map())

    //NOTE: This is deliberately not saved in the list of active submissions!
    val submissionNoRootEntity = Submission(UUID.randomUUID().toString(),testDate, WorkbenchEmail(userOwner.userEmail.value),methodConfigValid.namespace,methodConfigValid.name,None,
      Seq(Workflow(Option("workflowA"),WorkflowStatuses.Submitted, testDate, None, inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTerminateTest = Submission(UUID.randomUUID().toString(),testDate, WorkbenchEmail(userOwner.userEmail.value),agoraMethodConfig.namespace,agoraMethodConfig.name,Some(indiv1.toReference),
      Seq(Workflow(Option("workflowA"),WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), inputResolutions),
        Workflow(Option("workflowB"),WorkflowStatuses.Submitted,testDate,Some(sample2.toReference), inputResolutions),
        Workflow(Option("workflowC"),WorkflowStatuses.Submitted,testDate,Some(sample3.toReference), inputResolutions),
        Workflow(Option("workflowD"),WorkflowStatuses.Submitted,testDate,Some(sample4.toReference), inputResolutions)), SubmissionStatuses.Submitted, false)

    //a submission with a succeeeded workflow
    val submissionSuccessful1 = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowSuccessful1"), WorkflowStatuses.Succeeded, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Done, false)

    //a submission with a succeeeded workflow
    val submissionSuccessful2 = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowSuccessful2"), WorkflowStatuses.Succeeded, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Done, false)

    //a submission with a failed workflow
    val submissionFailed = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("worklowFailed"), WorkflowStatuses.Failed, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Done, false)

    //a submission with a submitted workflow
    val submissionSubmitted = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowSubmitted"), WorkflowStatuses.Submitted, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Submitted, false)

    //a submission with an aborted workflow
    val submissionAborted1 = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowAborted1"), WorkflowStatuses.Failed, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Aborted, false)

    //a submission with an aborted workflow
    val submissionAborted2 = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowAborted2"), WorkflowStatuses.Failed, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Aborted, false)

    //a submission with multiple failed and succeeded workflows
    val submissionMixed = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowSuccessful3"), WorkflowStatuses.Succeeded, testDate, Some(sample1.toReference), inputResolutions),
        Workflow(Option("workflowSuccessful4"), WorkflowStatuses.Succeeded, testDate, Some(sample2.toReference), inputResolutions),
        Workflow(Option("worklowFailed1"), WorkflowStatuses.Failed, testDate, Some(sample3.toReference), inputResolutions),
        Workflow(Option("workflowFailed2"), WorkflowStatuses.Failed, testDate, Some(sample4.toReference), inputResolutions),
        Workflow(Option("workflowSubmitted1"), WorkflowStatuses.Submitted, testDate, Some(sample5.toReference), inputResolutions),
        Workflow(Option("workflowSubmitted2"), WorkflowStatuses.Submitted, testDate, Some(sample6.toReference), inputResolutions)
      ), SubmissionStatuses.Submitted, false)

    //two submissions interleaved in time
    val t1 = new DateTime(2017, 1, 1, 5, 10)
    val t2 = new DateTime(2017, 1, 1, 5, 15)
    val t3 = new DateTime(2017, 1, 1, 5, 20)
    val t4 = new DateTime(2017, 1, 1, 5, 30)
    val outerSubmission = Submission(UUID.randomUUID().toString(), t1, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowSuccessful1"), WorkflowStatuses.Succeeded, t4, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Done, false)
    val innerSubmission = Submission(UUID.randomUUID().toString(), t2, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowFailed1"), WorkflowStatuses.Failed, t3, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Done, false)

    // a submission with a submitted workflow and a custom workflow failure mode
    val submissionWorkflowFailureMode = Submission(UUID.randomUUID().toString(), testDate, WorkbenchEmail(userOwner.userEmail.value), agoraMethodConfig.namespace, agoraMethodConfig.name, Some(indiv1.toReference),
      Seq(Workflow(Option("workflowFailureMode"), WorkflowStatuses.Submitted, testDate, Some(sample1.toReference), inputResolutions)), SubmissionStatuses.Submitted, false,
      Some(WorkflowFailureModes.ContinueWhilePossible))

    val allWorkspaces = Seq(
      workspace,
      controlledWorkspace,
      workspacePublished,
      workspaceNoAttrs,
      workspaceNoGroups,
      workspaceWithRealm,
      workspaceWithMultiGroupAD,
      otherWorkspaceWithRealm,
      workspaceNoSubmissions,
      workspaceSuccessfulSubmission,
      workspaceFailedSubmission,
      workspaceSubmittedSubmission,
      workspaceMixedSubmissions,
      workspaceTerminatedSubmissions,
      workspaceInterleavedSubmissions,
      workspaceWorkflowFailureMode,
      workspaceToTestGrant,
      workspaceConfigCopyDestination)
    val saveAllWorkspacesAction = DBIO.sequence(allWorkspaces.map(workspaceQuery.save))

    override def save() = {
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        rawlsBillingProjectQuery.create(testProject1),
        rawlsBillingProjectQuery.create(testProject2),
        rawlsBillingProjectQuery.create(testProject3),
        saveAllWorkspacesAction,
        withWorkspaceContext(workspace)({ context =>
          DBIO.seq(
                entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

                methodConfigurationQuery.create(context, agoraMethodConfig),
                methodConfigurationQuery.create(context, dockstoreMethodConfig),
                methodConfigurationQuery.create(context, goodAndBadMethodConfig),
                methodConfigurationQuery.create(context, methodConfig2),
                methodConfigurationQuery.create(context, methodConfig3),
                methodConfigurationQuery.create(context, methodConfigValid),
                methodConfigurationQuery.create(context, methodConfigDockstore),
                methodConfigurationQuery.create(context, methodConfigUnparseableInputs),
                methodConfigurationQuery.create(context, methodConfigUnparseableOutputs),
                methodConfigurationQuery.create(context, methodConfigUnparseableBoth),
                methodConfigurationQuery.create(context, methodConfigEmptyOutputs),
                methodConfigurationQuery.create(context, methodConfigNotAllSamples),
                methodConfigurationQuery.create(context, methodConfigAttrTypeMixup),
                methodConfigurationQuery.create(context, methodConfigArrayType),
                methodConfigurationQuery.create(context, methodConfigEntityless),
                methodConfigurationQuery.create(context, methodConfigEntityUpdate),
                methodConfigurationQuery.create(context, methodConfigWorkspaceLibraryUpdate),
                methodConfigurationQuery.create(context, methodConfigMissingOutputs),
                //HANDY HINT: if you're adding a new method configuration, don't reuse the name!
                //If you do, methodConfigurationQuery.create() will archive the old query and update it to point to the new one!

                submissionQuery.create(context, submissionTerminateTest),
                submissionQuery.create(context, submissionNoWorkflows),
                submissionQuery.create(context, submission1),
                submissionQuery.create(context, costedSubmission1),
                submissionQuery.create(context, submission2),
                submissionQuery.create(context, submissionUpdateEntity),
                submissionQuery.create(context, submissionUpdateWorkspace),

                // update exec key for all test data workflows that have been started.
                updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceWithRealm)({ context =>
          DBIO.seq(
            entityQuery.save(context, extraSample)
          )
        }),
        withWorkspaceContext(workspaceWithMultiGroupAD)({ context =>
          DBIO.seq(
            entityQuery.save(context, extraSample)
          )
        }),
        withWorkspaceContext(workspaceNoSubmissions)({ context =>
          DBIO.seq(
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceSuccessfulSubmission)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),
            methodConfigurationQuery.create(context, methodConfig2),

            submissionQuery.create(context, submissionSuccessful1),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceFailedSubmission)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, submissionFailed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceSubmittedSubmission)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, submissionSubmitted),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceTerminatedSubmissions)( { context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, submissionAborted2),
            submissionQuery.create(context, submissionSuccessful2),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceMixedSubmissions)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, submissionAborted1),
            submissionQuery.create(context, submissionMixed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceInterleavedSubmissions)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, outerSubmission),
            submissionQuery.create(context, innerSubmission),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceWorkflowFailureMode)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.create(context, agoraMethodConfig),

            submissionQuery.create(context, submissionWorkflowFailureMode),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        })
      )
    }
  }

  class MinimalTestData() extends TestData {
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace2", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set.empty)
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set.empty)
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set.empty)
    val ownerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-OWNER", Set.empty)
    val writerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-WRITER", Set.empty)
    val readerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-READER", Set.empty)
    val userReader = RawlsUser(UserInfo(RawlsUserEmail("reader-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))
    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)
    val workspace2 = Workspace(wsName2.namespace, wsName2.name, UUID.randomUUID().toString, "aBucket2", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        workspaceQuery.save(workspace2)
      )
    }
  }

  /* This test data should remain constant! Changing this data set will likely break
   * many of the tests that rely on it. */
  class ConstantTestData() extends TestData {
    // setup workspace objects
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo(RawlsUserEmail("writer-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(RawlsUserEmail("reader-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", wsAttrs)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val sample1 = Entity("sample1", "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> aliquot1.toReference,
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> sample1.toReference ) )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("cycle") -> sample6.toReference))
    val sample8 = Entity("sample8", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    val pair1 = Entity("pair1", "Pair",
      Map(AttributeName.withDefaultNS("case") -> sample2.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference,
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")) )
    val pair2 = Entity("pair2", "Pair",
      Map(AttributeName.withDefaultNS("case") -> sample3.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference ) )

    val sset1 = Entity("sset1", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(
        sample1.toReference,
        sample2.toReference,
        sample3.toReference)) ) )
    val sset2 = Entity("sset2", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(sample2.toReference)) ) )

    val sset3 = Entity("sset3", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        sample5.toReference,
        sample6.toReference))))

    val sset4 = Entity("sset4", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        sample7.toReference))))

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList ))

    val ps1 = Entity("ps1", "PairSet",
      Map(AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList( Seq(pair1.toReference, pair2.toReference)) ) )

    val indiv1 = Entity("indiv1", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> sset1.toReference ) )

    val indiv2 = Entity("indiv2", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> sset2.toReference ) )

    val methodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      Some("Sample"),
      Map("p1" -> AttributeString("prereq")),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfig2 = MethodConfiguration("dsde", "testConfig2", Some("Sample"), Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), AgoraMethod(wsName.namespace, "method-a", 1))
    val methodConfig3 = MethodConfiguration("dsde", "testConfig", Some("Sample"), Map("ready"-> AttributeString("true")), Map("param1"-> AttributeString("foo"), "param2"-> AttributeString("foo2")), Map("out" -> AttributeString("bar")), AgoraMethod("ns-config", "meth1", 1))

    val methodConfigEntityUpdate = MethodConfiguration("ns", "testConfig11", Some("Spample"), Map(), Map(), Map("o1" -> AttributeString("this.foo")), AgoraMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns", "testConfig1", Some("Sample"), Map(), Map(), Map("o1" -> AttributeString("workspace.foo")), AgoraMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigUnparseable = MethodConfiguration("dsde", "UnparseableMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", Some("Sample"), prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.confused")), outputs=Map.empty, AgoraMethod("dsde", "three_step", 1))

    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, wsName)
    val methodConfigName2 = methodConfigName.copy(name="novelName")
    val methodConfigName3 = methodConfigName.copy(name="noSuchName")
    val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName,methodConfigName2)
    val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName,methodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, wsName)

    val inputResolutions = Seq(SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name"))
    val inputResolutions2 = Seq(SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2"))

    val submissionNoWorkflows = createTestSubmission(workspace, methodConfig, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq.empty, Map.empty,
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission1 = createTestSubmission(workspace, methodConfig, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))

    val allEntities = Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)
    val allMCs = Seq(methodConfig, methodConfig2, methodConfig3, methodConfigValid, methodConfigUnparseable, methodConfigNotAllSamples, methodConfigAttrTypeMixup, methodConfigEntityUpdate)
    def saveAllMCs(context: SlickWorkspaceContext) = DBIO.sequence(allMCs map { mc => methodConfigurationQuery.create(context, mc) })

    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace)({ context =>
          DBIO.seq(
            entityQuery.save(context, allEntities),
            saveAllMCs(context),
            submissionQuery.create(context, submissionNoWorkflows),
            submissionQuery.create(context, submission1),
            submissionQuery.create(context, submission2)
          )
        })
      )
    }
  }

  val emptyData = new TestData() {
    override def save() = {
      DBIO.successful(())
    }
  }

  def withEmptyTestDatabase[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(emptyData)(testCode)
  }

  def withEmptyTestDatabase[T](testCode: SlickDataSource => T): T = {
    withCustomTestDatabaseInternal(emptyData)(testCode(slickDataSource))
  }

  val testData = new DefaultTestData()
  val constantData = new ConstantTestData()
  val minimalTestData = new MinimalTestData()

  def withDefaultTestDatabase[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(testData)(testCode)
  }

  def withDefaultTestDatabase[T](testCode: SlickDataSource => T): T = {
    withCustomTestDatabaseInternal(testData)(testCode(slickDataSource))
  }

  def withMinimalTestDatabase[T](testCode: SlickDataSource => T): T ={
    withCustomTestDatabaseInternal(minimalTestData)(testCode(slickDataSource))
  }

  def withConstantTestDatabase[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(constantData)(testCode)
  }

  def withConstantTestDatabase[T](testCode: SlickDataSource => T): T = {
    withCustomTestDatabaseInternal(constantData)(testCode(slickDataSource))
  }

  def withCustomTestDatabase[T](data: TestData)(testCode: SlickDataSource => T): T = {
    withCustomTestDatabaseInternal(data)(testCode(slickDataSource))
  }

  def withCustomTestDatabaseInternal[T](data: TestData)(testCode: => T): T = {
    withCustomTestDatabaseInternal(slickDataSource, data)(testCode)
  }

  def withCustomTestDatabaseInternal[T](dataSource: SlickDataSource, data: TestData)(testCode: => T): T = {
    runAndWait(DBIO.seq(dataSource.dataAccess.truncateAll), 2 minutes)
    try {
      runAndWait(data.save())
      testCode
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    }
  }

  def withWorkspaceContext[T](workspace: Workspace)(testCode: (SlickWorkspaceContext) => T): T = {
    testCode(SlickWorkspaceContext(workspace))
  }

  def updateWorkflowExecutionServiceKey(execKey: String) = {
    // when unit tests seed the test data with workflows, those workflows may land in the database as already-started.
    // however, the runtime create() methods we use to seed the data do not set EXEC_SERVICE_KEY, since that should
    // only be set when a workflow is submitted. Therefore, we have this test-only raw sql to update those
    // workflows to an appropriate EXEC_SERVICE_KEY.
    sql"update WORKFLOW set EXEC_SERVICE_KEY = ${execKey} where EXEC_SERVICE_KEY is null and EXTERNAL_ID is not null;".as[Int]
  }
}

trait TestData {
  def save(): ReadWriteAction[Unit]
}

trait TestDriverComponentWithFlatSpecAndMatchers extends FlatSpec with TestDriverComponent with Matchers
