package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.joda.time.DateTime
import org.scalatest.{Suite, FlatSpec, Matchers}
import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcDriver
import _root_.slick.driver.MySQLDriver.api._
import spray.http.OAuth2BearerToken

import scala.concurrent.duration._
import scala.concurrent.{Future, Await}

// initialize database tables and connection pool only once
object DbResource {
  // to override, e.g. to run against mysql:
  // $ sbt -Dtestdb=mysql test
  private val testdb = ConfigFactory.load.getStringOr("testdb", "mysql")

  val config = DatabaseConfig.forConfig[JdbcDriver](testdb)

  private val liquibaseConf = ConfigFactory.load().getConfig("liquibase")
  private val liquibaseChangeLog = liquibaseConf.getString("changelog")

  val dataSource = new SlickDataSource(config)(TestExecutionContext.testExecutionContext)
  dataSource.initWithLiquibase(liquibaseChangeLog)
}

/**
 * Created by dvoet on 2/3/16.
 */
trait TestDriverComponent extends DriverComponent with DataAccess {
  this: Suite =>

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  val databaseConfig = DbResource.config
  val slickDataSource = DbResource.dataSource

  override val driver: JdbcDriver = databaseConfig.driver
  override val batchSize: Int = databaseConfig.config.getInt("batchSize")

  val testDate = currentTime()
  val userInfo = UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345")

  // NOTE: we previously truncated millis here for DB compatibility reasons, but this is is no longer necessary.
  // now only serves to encapsulate a Java-ism
  def currentTime() = new DateTime()

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

  def createTestSubmission(workspace: Workspace, methodConfig: MethodConfiguration, submissionEntity: Entity, rawlsUserRef: RawlsUserRef
                           , workflowEntities: Seq[Entity], inputResolutions: Map[Entity, Seq[SubmissionValidationValue]]
                           , failedWorkflowEntities: Seq[Entity], failedInputResolutions: Map[Entity, Seq[SubmissionValidationValue]],
                           status: WorkflowStatus = WorkflowStatuses.Submitted): Submission = {

    val workflows = workflowEntities map { ref =>
      val uuid = if(status == WorkflowStatuses.Queued) None else Option(UUID.randomUUID.toString)
      Workflow(uuid, status, testDate, ref.toReference, inputResolutions(ref))
    }

    Submission(UUID.randomUUID.toString, testDate, rawlsUserRef, methodConfig.namespace, methodConfig.name, submissionEntity.toReference,
      workflows, SubmissionStatuses.Submitted, Map(WorkflowStatuses.Submitted.toString -> workflows.size))
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

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

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

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup), isLocked = true)

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
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val wsName3 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigs")
    val wsName4 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsSuccessfulSubmission")
    val wsName5 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsFailedSubmission")
    val wsName6 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsSubmittedSubmission")
    val wsName7 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsAbortedSubmission")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), Set(RawlsUser(userInfo)), Set.empty, "testBucketUrl", CreationStatuses.Ready)

    val wsAttrs = Map(
      "string" -> AttributeString("yep, it's a string"),
      "number" -> AttributeNumber(10),
      "empty" -> AttributeEmptyList,
      "values" -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspaceNoGroups = Workspace(wsName.namespace, wsName.name + "3", None, UUID.randomUUID().toString, "aBucket2", currentTime(), currentTime(), "testUser", wsAttrs, Map.empty, Map.empty)

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

    val realm = makeRawlsGroup(s"Test-Realm", Set.empty)
    val realmWsName = wsName.name + "withRealm"
    val realmOwnerIntersectionGroup = makeRawlsGroup(s"${realmWsName} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup = makeRawlsGroup(s"${realmWsName} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup = makeRawlsGroup(s"${realmWsName} IG READER", Set(userReader))
    val realmOwnerGroup = makeRawlsGroup(s"${realmWsName} OWNER", Set(userOwner))
    val realmWriterGroup = makeRawlsGroup(s"${realmWsName} WRITER", Set(userWriter))
    val realmReaderGroup = makeRawlsGroup(s"${realmWsName} READER", Set(userReader))

    val realm2 = makeRawlsGroup(s"Test-Realm2", Set.empty)
    val realmWs2Name = wsName2.name + "withRealm"
    val realmOwnerIntersectionGroup2 = makeRawlsGroup(s"${realmWs2Name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup2 = makeRawlsGroup(s"${realmWs2Name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup2 = makeRawlsGroup(s"${realmWs2Name} IG READER", Set(userReader))
    val realmOwnerGroup2 = makeRawlsGroup(s"${realmWs2Name} OWNER", Set(userOwner))
    val realmWriterGroup2 = makeRawlsGroup(s"${realmWs2Name} WRITER", Set(userWriter))
    val realmReaderGroup2 = makeRawlsGroup(s"${realmWs2Name} READER", Set(userReader))

    val realm3 = makeRawlsGroup(s"Test-Realm3", Set.empty)
    val realmOwnerIntersectionGroup3 = makeRawlsGroup(s"${wsName3.name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup3 = makeRawlsGroup(s"${wsName3.name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup3 = makeRawlsGroup(s"${wsName3.name} IG READER", Set(userReader))
    val realmOwnerGroup3 = makeRawlsGroup(s"${wsName3.name} OWNER", Set(userOwner))
    val realmWriterGroup3 = makeRawlsGroup(s"${wsName3.name} WRITER", Set(userWriter))
    val realmReaderGroup3 = makeRawlsGroup(s"${wsName3.name} READER", Set(userReader))

    val realm4 = makeRawlsGroup(s"Test-Realm4", Set.empty)
    val realmOwnerIntersectionGroup4 = makeRawlsGroup(s"${wsName4.name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup4 = makeRawlsGroup(s"${wsName4.name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup4 = makeRawlsGroup(s"${wsName4.name} IG READER", Set(userReader))
    val realmOwnerGroup4 = makeRawlsGroup(s"${wsName4.name} OWNER", Set(userOwner))
    val realmWriterGroup4 = makeRawlsGroup(s"${wsName4.name} WRITER", Set(userWriter))
    val realmReaderGroup4 = makeRawlsGroup(s"${wsName4.name} READER", Set(userReader))

    val realm5 = makeRawlsGroup(s"Test-Realm5", Set.empty)
    val realmOwnerIntersectionGroup5 = makeRawlsGroup(s"${wsName5.name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup5 = makeRawlsGroup(s"${wsName5.name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup5 = makeRawlsGroup(s"${wsName5.name} IG READER", Set(userReader))
    val realmOwnerGroup5 = makeRawlsGroup(s"${wsName5.name} OWNER", Set(userOwner))
    val realmWriterGroup5 = makeRawlsGroup(s"${wsName5.name} WRITER", Set(userWriter))
    val realmReaderGroup5 = makeRawlsGroup(s"${wsName5.name} READER", Set(userReader))

    val realm6 = makeRawlsGroup(s"Test-Realm6", Set.empty)
    val realmOwnerIntersectionGroup6 = makeRawlsGroup(s"${wsName6.name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup6 = makeRawlsGroup(s"${wsName6.name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup6 = makeRawlsGroup(s"${wsName6.name} IG READER", Set(userReader))
    val realmOwnerGroup6 = makeRawlsGroup(s"${wsName6.name} OWNER", Set(userOwner))
    val realmWriterGroup6 = makeRawlsGroup(s"${wsName6.name} WRITER", Set(userWriter))
    val realmReaderGroup6 = makeRawlsGroup(s"${wsName6.name} READER", Set(userReader))

    val realm7 = makeRawlsGroup(s"Test-Realm7", Set.empty)
    val realmOwnerIntersectionGroup7 = makeRawlsGroup(s"${wsName7.name} IG OWNER", Set(userOwner))
    val realmWriterIntersectionGroup7 = makeRawlsGroup(s"${wsName7.name} IG WRITER", Set(userWriter))
    val realmReaderIntersectionGroup7 = makeRawlsGroup(s"${wsName7.name} IG READER", Set(userReader))
    val realmOwnerGroup7 = makeRawlsGroup(s"${wsName7.name} OWNER", Set(userOwner))
    val realmWriterGroup7 = makeRawlsGroup(s"${wsName7.name} WRITER", Set(userWriter))
    val realmReaderGroup7 = makeRawlsGroup(s"${wsName7.name} READER", Set(userReader))

    val workspaceWithRealm = Workspace(wsName.namespace, realmWsName, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup,
        WorkspaceAccessLevels.Write -> realmWriterGroup,
        WorkspaceAccessLevels.Read -> realmReaderGroup),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup))

    val otherWorkspaceWithRealm = Workspace(wsName2.namespace, realmWs2Name, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup2,
        WorkspaceAccessLevels.Write -> realmWriterGroup2,
        WorkspaceAccessLevels.Read -> realmReaderGroup2),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup2,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup2,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup2))

    // Workspace with realms, without submissions
    val workspaceNoSubmissions = Workspace(wsName3.namespace, wsName3.name, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup3,
        WorkspaceAccessLevels.Write -> realmWriterGroup3,
        WorkspaceAccessLevels.Read -> realmReaderGroup3),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup3,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup3,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup3))

    // Workspace with realms, with successful submission
    val workspaceSuccessfulSubmission = Workspace(wsName4.namespace, wsName4.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup4,
        WorkspaceAccessLevels.Write -> realmWriterGroup4,
        WorkspaceAccessLevels.Read -> realmReaderGroup4),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup4,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup4,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup4))

    // Workspace with realms, with failed submission
    val workspaceFailedSubmission = Workspace(wsName5.namespace, wsName5.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup5,
        WorkspaceAccessLevels.Write -> realmWriterGroup5,
        WorkspaceAccessLevels.Read -> realmReaderGroup5),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup5,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup5,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup5))

    // Workspace with realms, with submitted submission
    val workspaceSubmittedSubmission = Workspace(wsName6.namespace, wsName6.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup6,
        WorkspaceAccessLevels.Write -> realmWriterGroup6,
        WorkspaceAccessLevels.Read -> realmReaderGroup6),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup6,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup6,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup6))

    // Workspace with realms with aborted workflows
    val workspaceMixedSubmissions = Workspace(wsName7.namespace, wsName7.name, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerGroup7,
        WorkspaceAccessLevels.Write -> realmWriterGroup7,
        WorkspaceAccessLevels.Read -> realmReaderGroup7),
      Map(
        WorkspaceAccessLevels.Owner -> realmOwnerIntersectionGroup7,
        WorkspaceAccessLevels.Write -> realmWriterIntersectionGroup7,
        WorkspaceAccessLevels.Read -> realmReaderIntersectionGroup7))


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
    val sample8 = Entity("sample8", "Sample", Map("type" -> AttributeString("tumor")))
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map( "case" -> AttributeEntityReference("Sample", "sample2"),
        "control" -> AttributeEntityReference("Sample", "sample1"),
        "whatsit" -> AttributeString("occurs in sample too! oh no!")) )
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

    val indiv2 = Entity("indiv2", "Individual",
      Map( "sset" -> AttributeEntityReference("SampleSet", "sset2") ) )

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

    val methodConfigEntityUpdate = MethodConfiguration("ns", "testConfig11", "Sample", Map(), Map(), Map("o1" -> AttributeString("this.foo")), MethodRepoMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns", "testConfig1", "Sample", Map(), Map(), Map("o1" -> AttributeString("workspace.foo")), MethodRepoMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigUnparseable = MethodConfiguration("dsde", "UnparseableMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.confused")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigArrayType = MethodConfiguration("dsde", "ArrayMethodConfig", "SampleSet", prerequisites=Map.empty,
      inputs=Map("aggregate_data_workflow.aggregate_data.input_array" -> AttributeString("this.samples.type")),
      outputs=Map("aggregate_data_workflow.aggregate_data.output_array" -> AttributeString("this.output_array")),
      MethodRepoMethod("dsde", "array_task", 1))

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
    val inputResolutions2 = Seq(SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2"))

    val submissionNoWorkflows = createTestSubmission(workspace, methodConfig, indiv1, userOwner,
      Seq.empty, Map.empty,
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission1 = createTestSubmission(workspace, methodConfig, indiv1, userOwner,
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, userOwner,
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))

    val submissionUpdateEntity = createTestSubmission(workspace, methodConfigEntityUpdate, indiv1, userOwner,
      Seq(indiv1), Map(indiv1 -> inputResolutions),
      Seq(indiv2), Map(indiv2 -> inputResolutions2))
    val submissionUpdateWorkspace = createTestSubmission(workspace, methodConfigWorkspaceUpdate, indiv1, userOwner,
      Seq(indiv1), Map(indiv1 -> inputResolutions),
      Seq(indiv2), Map(indiv2 -> inputResolutions2))

    val submissionTerminateTest = Submission(UUID.randomUUID().toString(),testDate, userOwner,methodConfig.namespace,methodConfig.name,indiv1.toReference,
      Seq(Workflow(Option("workflowA"),WorkflowStatuses.Submitted,testDate,sample1.toReference, inputResolutions),
        Workflow(Option("workflowB"),WorkflowStatuses.Submitted,testDate,sample2.toReference, inputResolutions),
        Workflow(Option("workflowC"),WorkflowStatuses.Submitted,testDate,sample3.toReference, inputResolutions),
        Workflow(Option("workflowD"),WorkflowStatuses.Submitted,testDate,sample4.toReference, inputResolutions)), SubmissionStatuses.Submitted, Map[String, Int]())

    //a submission with a succeeeded workflow
    val submissionSuccessful = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSuccessful"), WorkflowStatuses.Succeeded, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Done, Map("Succeeded" -> 1))

    //a submission with a failed workflow
    val submissionFailed = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("worklowFailed"), WorkflowStatuses.Failed, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Done, Map("Failed" -> 1))

    //a submission with a submitted workflow
    val submissionSubmitted = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSubmitted"), WorkflowStatuses.Submitted, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Submitted, Map("Submitted" -> 1))

    //a submission with an aborted workflow
    val submissionAborted = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowAborted"), WorkflowStatuses.Failed, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Aborted, Map("Aborted" -> 1))

    //a submission with multiple failed and succeeded workflows
    val submissionMixed = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSuccessful1"), WorkflowStatuses.Succeeded, testDate, sample1.toReference, inputResolutions),
        Workflow(Option("workflowSuccessful2"), WorkflowStatuses.Succeeded, testDate, sample2.toReference, inputResolutions),
        Workflow(Option("worklowFailed1"), WorkflowStatuses.Failed, testDate, sample3.toReference, inputResolutions),
        Workflow(Option("workflowFailed2"), WorkflowStatuses.Failed, testDate, sample4.toReference, inputResolutions),
        Workflow(Option("workflowSubmitted1"), WorkflowStatuses.Submitted, testDate, sample5.toReference, inputResolutions),
        Workflow(Option("workflowSubmitted2"), WorkflowStatuses.Submitted, testDate, sample6.toReference, inputResolutions)
      ), SubmissionStatuses.Done, Map[String, Int]())

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
        rawlsGroupQuery.save(realm),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup),
        rawlsGroupQuery.save(realmWriterIntersectionGroup),
        rawlsGroupQuery.save(realmReaderIntersectionGroup),
        rawlsGroupQuery.save(realmOwnerGroup),
        rawlsGroupQuery.save(realmWriterGroup),
        rawlsGroupQuery.save(realmReaderGroup),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup2),
        rawlsGroupQuery.save(realmWriterIntersectionGroup2),
        rawlsGroupQuery.save(realmReaderIntersectionGroup2),
        rawlsGroupQuery.save(realmOwnerGroup2),
        rawlsGroupQuery.save(realmWriterGroup2),
        rawlsGroupQuery.save(realmReaderGroup2),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup3),
        rawlsGroupQuery.save(realmWriterIntersectionGroup3),
        rawlsGroupQuery.save(realmReaderIntersectionGroup3),
        rawlsGroupQuery.save(realmOwnerGroup3),
        rawlsGroupQuery.save(realmWriterGroup3),
        rawlsGroupQuery.save(realmReaderGroup3),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup4),
        rawlsGroupQuery.save(realmWriterIntersectionGroup4),
        rawlsGroupQuery.save(realmReaderIntersectionGroup4),
        rawlsGroupQuery.save(realmOwnerGroup4),
        rawlsGroupQuery.save(realmWriterGroup4),
        rawlsGroupQuery.save(realmReaderGroup4),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup5),
        rawlsGroupQuery.save(realmWriterIntersectionGroup5),
        rawlsGroupQuery.save(realmReaderIntersectionGroup5),
        rawlsGroupQuery.save(realmOwnerGroup5),
        rawlsGroupQuery.save(realmWriterGroup5),
        rawlsGroupQuery.save(realmReaderGroup5),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup6),
        rawlsGroupQuery.save(realmWriterIntersectionGroup6),
        rawlsGroupQuery.save(realmReaderIntersectionGroup6),
        rawlsGroupQuery.save(realmOwnerGroup6),
        rawlsGroupQuery.save(realmWriterGroup6),
        rawlsGroupQuery.save(realmReaderGroup6),
        rawlsGroupQuery.save(realmOwnerIntersectionGroup7),
        rawlsGroupQuery.save(realmWriterIntersectionGroup7),
        rawlsGroupQuery.save(realmReaderIntersectionGroup7),
        rawlsGroupQuery.save(realmOwnerGroup7),
        rawlsGroupQuery.save(realmWriterGroup7),
        rawlsGroupQuery.save(realmReaderGroup7),
        workspaceQuery.save(workspace),
        workspaceQuery.save(workspaceNoGroups),
        workspaceQuery.save(workspaceWithRealm),
        workspaceQuery.save(otherWorkspaceWithRealm),
        workspaceQuery.save(workspaceNoSubmissions),
        workspaceQuery.save(workspaceSuccessfulSubmission),
        workspaceQuery.save(workspaceFailedSubmission),
        workspaceQuery.save(workspaceSubmittedSubmission),
        workspaceQuery.save(workspaceMixedSubmissions),
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
                entityQuery.save(context, sample8),
                entityQuery.save(context, pair1),
                entityQuery.save(context, pair2),
                entityQuery.save(context, ps1),
                entityQuery.save(context, sset1),
                entityQuery.save(context, sset2),
                entityQuery.save(context, sset3),
                entityQuery.save(context, sset4),
                entityQuery.save(context, sset_empty),
                entityQuery.save(context, indiv1),
                entityQuery.save(context, indiv2),

                methodConfigurationQuery.save(context, methodConfig),
                methodConfigurationQuery.save(context, methodConfig2),
                methodConfigurationQuery.save(context, methodConfig3),
                methodConfigurationQuery.save(context, methodConfigValid),
                methodConfigurationQuery.save(context, methodConfigUnparseable),
                methodConfigurationQuery.save(context, methodConfigNotAllSamples),
                methodConfigurationQuery.save(context, methodConfigAttrTypeMixup),
                methodConfigurationQuery.save(context, methodConfigArrayType),
                methodConfigurationQuery.save(context, methodConfigEntityUpdate),

                submissionQuery.create(context, submissionTerminateTest),
                submissionQuery.create(context, submissionNoWorkflows),
                submissionQuery.create(context, submission1),
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
        withWorkspaceContext(workspaceNoSubmissions)({ context =>
          DBIO.seq(
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceSuccessfulSubmission)({ context =>
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
            entityQuery.save(context, sample8),
            entityQuery.save(context, pair1),
            entityQuery.save(context, pair2),
            entityQuery.save(context, ps1),
            entityQuery.save(context, sset1),
            entityQuery.save(context, sset2),
            entityQuery.save(context, sset3),
            entityQuery.save(context, sset4),
            entityQuery.save(context, sset_empty),
            entityQuery.save(context, indiv1),
            entityQuery.save(context, indiv2),

            methodConfigurationQuery.save(context, methodConfig),
            methodConfigurationQuery.save(context, methodConfig2),

            submissionQuery.create(context, submissionSuccessful),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceFailedSubmission)({ context =>
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
            entityQuery.save(context, sample8),
            entityQuery.save(context, pair1),
            entityQuery.save(context, pair2),
            entityQuery.save(context, ps1),
            entityQuery.save(context, sset1),
            entityQuery.save(context, sset2),
            entityQuery.save(context, sset3),
            entityQuery.save(context, sset4),
            entityQuery.save(context, sset_empty),
            entityQuery.save(context, indiv1),
            entityQuery.save(context, indiv2),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionFailed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceSubmittedSubmission)({ context =>
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
            entityQuery.save(context, sample8),
            entityQuery.save(context, pair1),
            entityQuery.save(context, pair2),
            entityQuery.save(context, ps1),
            entityQuery.save(context, sset1),
            entityQuery.save(context, sset2),
            entityQuery.save(context, sset3),
            entityQuery.save(context, sset4),
            entityQuery.save(context, sset_empty),
            entityQuery.save(context, indiv1),
            entityQuery.save(context, indiv2),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionSubmitted),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceMixedSubmissions)({ context =>
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
            entityQuery.save(context, sample8),
            entityQuery.save(context, pair1),
            entityQuery.save(context, pair2),
            entityQuery.save(context, ps1),
            entityQuery.save(context, sset1),
            entityQuery.save(context, sset2),
            entityQuery.save(context, sset3),
            entityQuery.save(context, sset4),
            entityQuery.save(context, sset_empty),
            entityQuery.save(context, indiv1),
            entityQuery.save(context, indiv2),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionAborted),
            submissionQuery.create(context, submissionMixed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        })
      )
    }
  }

  /* This test data should remain constant! Changing this data set will likely break
   * many of the tests that rely on it. */
  class ConstantTestData() extends TestData {
    // setup workspace objects
    val userOwner = RawlsUser(UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345"))
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), Set(RawlsUser(userInfo)), Set.empty, "testBucketUrl", CreationStatuses.Ready)

    val wsAttrs = Map(
      "string" -> AttributeString("yep, it's a string"),
      "number" -> AttributeNumber(10),
      "empty" -> AttributeEmptyList,
      "values" -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

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
    val sample8 = Entity("sample8", "Sample", Map("type" -> AttributeString("tumor")))
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map( "case" -> AttributeEntityReference("Sample", "sample2"),
        "control" -> AttributeEntityReference("Sample", "sample1"),
        "whatsit" -> AttributeString("occurs in sample too! oh no!")) )
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

    val indiv2 = Entity("indiv2", "Individual",
      Map( "sset" -> AttributeEntityReference("SampleSet", "sset2") ) )

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

    val methodConfigEntityUpdate = MethodConfiguration("ns", "testConfig11", "Sample", Map(), Map(), Map("o1" -> AttributeString("this.foo")), MethodRepoMethod("ns-config", "meth1", 1))
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns", "testConfig1", "Sample", Map(), Map(), Map("o1" -> AttributeString("workspace.foo")), MethodRepoMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
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

    val inputResolutions = Seq(SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name"))
    val inputResolutions2 = Seq(SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2"))

    val submissionNoWorkflows = createTestSubmission(workspace, methodConfig, indiv1, userOwner,
      Seq.empty, Map.empty,
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission1 = createTestSubmission(workspace, methodConfig, indiv1, userOwner,
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))
    val submission2 = createTestSubmission(workspace, methodConfig2, indiv1, userOwner,
      Seq(sample1, sample2, sample3), Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2))

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
            entityQuery.save(context, sample8),
            entityQuery.save(context, pair1),
            entityQuery.save(context, pair2),
            entityQuery.save(context, ps1),
            entityQuery.save(context, sset1),
            entityQuery.save(context, sset2),
            entityQuery.save(context, sset3),
            entityQuery.save(context, sset4),
            entityQuery.save(context, sset_empty),
            entityQuery.save(context, indiv1),
            entityQuery.save(context, indiv2),

            methodConfigurationQuery.save(context, methodConfig),
            methodConfigurationQuery.save(context, methodConfig2),
            methodConfigurationQuery.save(context, methodConfig3),
            methodConfigurationQuery.save(context, methodConfigValid),
            methodConfigurationQuery.save(context, methodConfigUnparseable),
            methodConfigurationQuery.save(context, methodConfigNotAllSamples),
            methodConfigurationQuery.save(context, methodConfigAttrTypeMixup),
            methodConfigurationQuery.save(context, methodConfigEntityUpdate),

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
      DBIO.successful(Unit)
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

  def withDefaultTestDatabase[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(testData)(testCode)
  }

  def withDefaultTestDatabase[T](testCode: SlickDataSource => T): T = {
    withCustomTestDatabaseInternal(testData)(testCode(slickDataSource))
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
    try {
      runAndWait(data.save())
      testCode
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    } finally {
      runAndWait(DBIO.seq(slickDataSource.dataAccess.truncateAll))
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
