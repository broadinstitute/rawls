package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.ProjectRoles.Owner
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.{WorkspaceAccessLevel, ProjectOwner}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers, Suite}
import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcDriver
import _root_.slick.driver.MySQLDriver.api._
import spray.http.OAuth2BearerToken

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// initialize database tables and connection pool only once
object DbResource {
  // to override, e.g. to run against mysql:
  // $ sbt -Dtestdb=mysql test
  private val testdb = ConfigFactory.load.getStringOr("testdb", "mysql")

  val config = DatabaseConfig.forConfig[JdbcDriver](testdb)

  private val liquibaseConf = ConfigFactory.load().getConfig("liquibase")
  private val liquibaseChangeLog = liquibaseConf.getString("changelog")

  val dataSource = new SlickDataSource(config)(TestExecutionContext.testExecutionContext)
  dataSource.initWithLiquibase(liquibaseChangeLog, Map.empty)
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

  val userInfo = UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345")

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

  def createTestSubmission(workspace: Workspace, methodConfig: MethodConfiguration, submissionEntity: Entity, rawlsUserRef: RawlsUserRef
                           , workflowEntities: Seq[Entity], inputResolutions: Map[Entity, Seq[SubmissionValidationValue]]
                           , failedWorkflowEntities: Seq[Entity], failedInputResolutions: Map[Entity, Seq[SubmissionValidationValue]],
                           status: WorkflowStatus = WorkflowStatuses.Submitted): Submission = {

    val workflows = workflowEntities map { ref =>
      val uuid = if(status == WorkflowStatuses.Queued) None else Option(UUID.randomUUID.toString)
      Workflow(uuid, status, testDate, ref.toReference, inputResolutions(ref))
    }

    Submission(UUID.randomUUID.toString, testDate, rawlsUserRef, methodConfig.namespace, methodConfig.name, submissionEntity.toReference,
      workflows, SubmissionStatuses.Submitted)
  }

  def generateBillingGroups(projectName: RawlsBillingProjectName, users: Map[ProjectRoles.ProjectRole, Set[RawlsUserRef]], subGroups: Map[ProjectRoles.ProjectRole, Set[RawlsGroupRef]]): Map[ProjectRoles.ProjectRole, RawlsGroup] = {
    val gcsDAO = new MockGoogleServicesDAO("foo")
    ProjectRoles.all.map { role =>
      val usersToAdd = users.getOrElse(role, Set.empty)
      val groupsToAdd = subGroups.getOrElse(role, Set.empty)
      val groupName = RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, role))
      val groupEmail = RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupName))
      role -> RawlsGroup(groupName, groupEmail, usersToAdd, groupsToAdd)
    }.toMap
  }

  def billingProjectFromName(name: String) = RawlsBillingProject(RawlsBillingProjectName(name), generateBillingGroups(RawlsBillingProjectName(name), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)

  def makeRawlsGroup(name: String, users: Set[RawlsUserRef], groups: Set[RawlsGroupRef] = Set.empty) =
    RawlsGroup(RawlsGroupName(name), RawlsGroupEmail(s"$name@example.com"), users, groups)

  def makeWorkspaceWithUsers(usersByLevel: Map[WorkspaceAccessLevels.WorkspaceAccessLevel, Set[RawlsUserRef]], groupsByLevel: Map[WorkspaceAccessLevel, Set[RawlsGroupRef]] = Map(WorkspaceAccessLevels.Owner -> Set.empty, WorkspaceAccessLevels.Write -> Set.empty, WorkspaceAccessLevels.Read -> Set.empty))(project: RawlsBillingProject,
                    name: String,
                    realm: Option[RawlsGroupRef],
                    workspaceId: String,
                    bucketName: String,
                    createdDate: DateTime,
                    lastModified: DateTime,
                    createdBy: String,
                    attributes: AttributeMap,
                    isLocked: Boolean) = {

    val intersectionGroupsByLevel = realm.map { _ => usersByLevel.map { case (level, users) =>
      level -> makeRawlsGroup(s"${project.projectName.value}/${name} IG ${level.toString}", users, groupsByLevel(level))
    } + (ProjectOwner -> makeRawlsGroup(s"${project.projectName.value}/${name} IG ${ProjectOwner.toString}", project.groups(Owner).users, Set.empty)) }


    val newAccessGroupsByLevel = usersByLevel.map { case (level, users) =>
      level -> makeRawlsGroup(s"${project.projectName.value}/${name} ${level.toString}", users, groupsByLevel(level))
    }

    val accessGroupsByLevel = newAccessGroupsByLevel + (ProjectOwner -> project.groups(Owner))

    (Workspace(project.projectName.value, name, realm, workspaceId, bucketName, createdDate, createdDate, createdBy, attributes,
      accessGroupsByLevel.map { case (level, group) => level -> RawlsGroup.toRef(group) },
      intersectionGroupsByLevel.getOrElse(accessGroupsByLevel).map { case (level, group) => level -> RawlsGroup.toRef(group) }, isLocked),

      intersectionGroupsByLevel.getOrElse(Map.empty).values ++ newAccessGroupsByLevel.values)
  }

  class EmptyWorkspace() extends TestData {
    val userOwner = RawlsUser(userInfo)
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
    val userOwner = RawlsUser(userInfo)
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
    val userProjectOwner = RawlsUser(UserInfo("project-owner-access", OAuth2BearerToken("token"), 123, "123456789876543210101"))
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val userReaderViaGroup = RawlsUser(UserInfo("reader-access-via-group", OAuth2BearerToken("token"), 123, "123456789876543212349"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val wsName3 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigs")
    val wsName4 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsSuccessfulSubmission")
    val wsName5 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsFailedSubmission")
    val wsName6 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsSubmittedSubmission")
    val wsName7 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsAbortedSubmission")
    val wsName8 = WorkspaceName("myNamespace", "myWorkspacewithRealmsMethodConfigsAbortedSuccessfulSubmission")
    val wsName9 = WorkspaceName("myNamespace", "myWorkspaceToTestGrantPermissions")
    val workspaceToTestGrantId = UUID.randomUUID()

    val nestedProjectGroup = makeRawlsGroup("nested project group", Set(userOwner))
    val dbGapAuthorizedUsersGroup = makeRawlsGroup("dbGapAuthorizedUsers", Set(userOwner, userReaderViaGroup))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), generateBillingGroups(RawlsBillingProjectName(wsName.namespace), Map(ProjectRoles.Owner -> Set(userProjectOwner, userOwner), ProjectRoles.User -> Set.empty), Map.empty), "testBucketUrl", CreationStatuses.Ready, None, None)

    val testProject1Name = RawlsBillingProjectName("arbitrary")
    val testProject1 = RawlsBillingProject(testProject1Name, generateBillingGroups(testProject1Name, Map(ProjectRoles.Owner -> Set(userProjectOwner), ProjectRoles.User -> Set(userWriter)), Map(ProjectRoles.User -> Set(nestedProjectGroup))), "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None)

    val testProject2Name = RawlsBillingProjectName("project2")
    val testProject2 = RawlsBillingProject(testProject2Name, generateBillingGroups(testProject2Name, Map(ProjectRoles.Owner -> Set(userProjectOwner), ProjectRoles.User -> Set(userWriter)), Map.empty), "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None)

    val testProject3Name = RawlsBillingProjectName("project3")
    val testProject3 = RawlsBillingProject(testProject3Name, generateBillingGroups(testProject3Name, Map(ProjectRoles.Owner -> Set(userProjectOwner), ProjectRoles.User -> Set(userReader)), Map.empty), "http://cromwell-auth-url.example.com", CreationStatuses.Ready, None, None)

    val makeWorkspace = makeWorkspaceWithUsers(Map(
      WorkspaceAccessLevels.Owner -> Set(userOwner),
      WorkspaceAccessLevels.Write -> Set(userWriter),
      WorkspaceAccessLevels.Read -> Set(userReader)
    ))_

    val makeWorkspaceToTestGrant = makeWorkspaceWithUsers(Map(
      WorkspaceAccessLevels.Owner -> Set(userOwner),
      WorkspaceAccessLevels.Write -> Set(userWriter),
      WorkspaceAccessLevels.Read -> Set.empty
    ),Map(
      WorkspaceAccessLevels.Owner -> Set.empty,
      WorkspaceAccessLevels.Write -> Set.empty,
      WorkspaceAccessLevels.Read -> Set(dbGapAuthorizedUsersGroup)
    ))_

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspaceNoGroups = Workspace(wsName.namespace, wsName.name + "3", None, UUID.randomUUID().toString, "aBucket2", currentTime(), currentTime(), "testUser", wsAttrs, Map.empty, Map.empty)

    val (workspace, workspaceGroups) = makeWorkspace(billingProject, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    val workspacePublished = Workspace(wsName.namespace, wsName.name + "_published", None, UUID.randomUUID().toString, "aBucket3", currentTime(), currentTime(), "testUser",
      wsAttrs + (AttributeName.withLibraryNS("published") -> AttributeBoolean(true)), Map.empty, Map.empty)
    val workspaceNoAttrs = Workspace(wsName.namespace, wsName.name + "_noattrs", None, UUID.randomUUID().toString, "aBucket4", currentTime(), currentTime(), "testUser", Map.empty, Map.empty, Map.empty)

    val realm = makeRawlsGroup(s"Test-Realm", Set.empty)
    val realmWsName = wsName.name + "withRealm"

    val realm2 = makeRawlsGroup(s"Test-Realm2", Set.empty)
    val realmWs2Name = wsName2.name + "withRealm"

    val realm3 = makeRawlsGroup(s"Test-Realm3", Set.empty)

    val realm4 = makeRawlsGroup(s"Test-Realm4", Set.empty)

    val realm5 = makeRawlsGroup(s"Test-Realm5", Set.empty)

    val realm6 = makeRawlsGroup(s"Test-Realm6", Set.empty)

    val realm7 = makeRawlsGroup(s"Test-Realm7", Set.empty)

    val (workspaceWithRealm, workspaceWithRealmGroups) = makeWorkspace(billingProject, realmWsName, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    val (controlledWorkspace, controlledWorkspaceGroups) = makeWorkspace(billingProject, "test-tcga", Option(dbGapAuthorizedUsersGroup), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    val (otherWorkspaceWithRealm, otherWorkspaceWithRealmGroups) = makeWorkspace(billingProject, realmWs2Name, Option(realm2), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, without submissions
    val (workspaceNoSubmissions, workspaceNoSubmissionsGroups) = makeWorkspace(billingProject, wsName3.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with successful submission
    val (workspaceSuccessfulSubmission, workspaceSuccessfulSubmissionGroups) = makeWorkspace(billingProject, wsName4.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with failed submission
    val (workspaceFailedSubmission, workspaceFailedSubmissionGroups) = makeWorkspace(billingProject, wsName5.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with submitted submission
    val (workspaceSubmittedSubmission, workspaceSubmittedSubmissionGroups) = makeWorkspace(billingProject, wsName6.name , Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms with mixed workflows
    val (workspaceMixedSubmissions, workspaceMixedSubmissionsGroups) = makeWorkspace(billingProject, wsName7.name, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Workspace with realms, with aborted and successful submissions
    val (workspaceTerminatedSubmissions, workspaceTerminatedSubmissionsGroups) = makeWorkspace(billingProject, wsName8.name, Option(realm), UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    // Standard workspace to test grant permissions
    val (workspaceToTestGrant, workspaceToTestGrantGroups) = makeWorkspaceToTestGrant(billingProject, wsName9.name, None, workspaceToTestGrantId.toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs, false)

    val sample1 = Entity("sample1", "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> AttributeEntityReference("Aliquot", "aliquot1"),
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeEntityReference("Sample", "sample1") ) )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("Sample", "sample6")))
    val sample8 = Entity("sample8", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("foo_id") -> AttributeString("1029384756")))
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map(AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample2"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1"),
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")) )
    val pair2 = Entity("pair2", "Pair",
      Map(AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1") ) )

    val sset1 = Entity("sset1", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample1"),
        AttributeEntityReference("Sample", "sample2"),
        AttributeEntityReference("Sample", "sample3"))) ) )
    val sset2 = new Entity("sset2", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample2"))) ) )

    val sset3 = Entity("sset3", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample5"),
        AttributeEntityReference("Sample", "sample6")))))

    val sset4 = Entity("sset4", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample7")))))

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList ))

    val ps1 = Entity("ps1", "PairSet",
      Map(AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Pair", "pair1"),
        AttributeEntityReference("Pair", "pair2"))) ) )

    val indiv1 = Entity("indiv1", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> AttributeEntityReference("SampleSet", "sset1") ) )

    val indiv2 = Entity("indiv2", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> AttributeEntityReference("SampleSet", "sset2") ) )

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
    val methodConfigWorkspaceLibraryUpdate = MethodConfiguration("ns", "testConfigLib", "Sample", Map(), Map(), Map("o1" -> AttributeString("workspace.library:foo")), MethodRepoMethod("ns-config", "meth1", 1))

    val methodConfigValid = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.name")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigUnparseable = MethodConfiguration("dsde", "UnparseableMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigNotAllSamples = MethodConfiguration("dsde", "NotAllSamplesMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))
    val methodConfigAttrTypeMixup = MethodConfiguration("dsde", "AttrTypeMixupMethodConfig", "Sample", prerequisites=Map.empty, inputs=Map("three_step.cgrep.pattern" -> AttributeString("this.confused")), outputs=Map.empty, MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigArrayType = MethodConfiguration("dsde", "ArrayMethodConfig", "SampleSet", prerequisites=Map.empty,
      inputs=Map("aggregate_data_workflow.aggregate_data.input_array" -> AttributeString("this.samples.type")),
      outputs=Map("aggregate_data_workflow.aggregate_data.output_array" -> AttributeString("this.output_array")),
      MethodRepoMethod("dsde", "array_task", 1))

    val methodConfigValidExprs = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty,
      inputs=Map(
        "foo" -> AttributeString("this.thing.foo"),
        "bar" -> AttributeString("workspace.bar"),
        "baz" -> AttributeString("this.library:thing.baz"),
        "quux" -> AttributeString("4"),
        "splat" -> AttributeString("\"splat\""),
        "bang" -> AttributeString("[1,2,3]")),
      outputs=Map("foo" -> AttributeString("this.foo"), "bar" -> AttributeString("workspace.bar"), "baz" -> AttributeString("this.library:baz"), "quux" -> AttributeString("workspace.library:quux")),
      MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigInvalidExprs = MethodConfiguration("dsde", "GoodMethodConfig", "Sample", prerequisites=Map.empty,
      inputs=Map("foo" -> AttributeString("bonk.thing.foo"), "bar" -> AttributeString("workspace.bar")),
      outputs=Map(
        "foo" -> AttributeString("this.bonk.foo"),
        "bar" -> AttributeString("foo.bar"),
        "baz" -> AttributeString("4"),
        "qux" -> AttributeString("[1,2,3]")),
      MethodRepoMethod("dsde", "three_step", 1))

    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, wsName)
    val methodConfigName2 = methodConfigName.copy(name="novelName")
    val methodConfigName3 = methodConfigName.copy(name="noSuchName")
    val methodConfigName4 = methodConfigName.copy(name=methodConfigWorkspaceLibraryUpdate.name)
    val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName,methodConfigName2)
    val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName,methodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3,methodConfigName2)
    val methodConfigNamePairFromLibrary = MethodConfigurationNamePair(methodConfigName4,methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, wsName)
    val methodRepoGood = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, newMethodConfigName)
    val methodRepoMissing = MethodRepoConfigurationImport("workspace_test", "rawls_test_missing", 1, methodConfigName)
    val methodRepoEmptyPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_empty_payload", 1, methodConfigName)
    val methodRepoBadPayload = MethodRepoConfigurationImport("workspace_test", "rawls_test_bad_payload", 1, methodConfigName)
    val methodRepoLibrary = MethodRepoConfigurationImport("workspace_test", "rawls_test_library", 1, newMethodConfigName)

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
        Workflow(Option("workflowD"),WorkflowStatuses.Submitted,testDate,sample4.toReference, inputResolutions)), SubmissionStatuses.Submitted)

    //a submission with a succeeeded workflow
    val submissionSuccessful1 = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSuccessful1"), WorkflowStatuses.Succeeded, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Done)

    //a submission with a succeeeded workflow
    val submissionSuccessful2 = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSuccessful2"), WorkflowStatuses.Succeeded, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Done)

    //a submission with a failed workflow
    val submissionFailed = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("worklowFailed"), WorkflowStatuses.Failed, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Done)

    //a submission with a submitted workflow
    val submissionSubmitted = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSubmitted"), WorkflowStatuses.Submitted, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Submitted)

    //a submission with an aborted workflow
    val submissionAborted1 = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowAborted1"), WorkflowStatuses.Failed, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Aborted)

    //a submission with an aborted workflow
    val submissionAborted2 = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowAborted2"), WorkflowStatuses.Failed, testDate, sample1.toReference, inputResolutions)), SubmissionStatuses.Aborted)

    //a submission with multiple failed and succeeded workflows
    val submissionMixed = Submission(UUID.randomUUID().toString(), testDate, userOwner, methodConfig.namespace, methodConfig.name, indiv1.toReference,
      Seq(Workflow(Option("workflowSuccessful3"), WorkflowStatuses.Succeeded, testDate, sample1.toReference, inputResolutions),
        Workflow(Option("workflowSuccessful4"), WorkflowStatuses.Succeeded, testDate, sample2.toReference, inputResolutions),
        Workflow(Option("worklowFailed1"), WorkflowStatuses.Failed, testDate, sample3.toReference, inputResolutions),
        Workflow(Option("workflowFailed2"), WorkflowStatuses.Failed, testDate, sample4.toReference, inputResolutions),
        Workflow(Option("workflowSubmitted1"), WorkflowStatuses.Submitted, testDate, sample5.toReference, inputResolutions),
        Workflow(Option("workflowSubmitted2"), WorkflowStatuses.Submitted, testDate, sample6.toReference, inputResolutions)
      ), SubmissionStatuses.Submitted)

    def createWorkspaceGoogleGroups(gcsDAO: GoogleServicesDAO): Unit = {
      val groups = billingProject.groups.values ++
        testProject1.groups.values ++
        testProject2.groups.values ++
        testProject3.groups.values ++
        workspaceGroups ++
        workspaceWithRealmGroups ++
        otherWorkspaceWithRealmGroups ++
        workspaceNoSubmissionsGroups ++
        workspaceSuccessfulSubmissionGroups ++
        workspaceFailedSubmissionGroups ++
        workspaceSubmittedSubmissionGroups ++
        workspaceMixedSubmissionsGroups ++
        workspaceTerminatedSubmissionsGroups ++
        controlledWorkspaceGroups ++
        Seq(realm, realm2)

      groups.foreach(gcsDAO.createGoogleGroup(_))
    }

    val allWorkspaces = Seq(workspace,
      controlledWorkspace,
      workspacePublished,
      workspaceNoAttrs,
      workspaceNoGroups,
      workspaceWithRealm,
      otherWorkspaceWithRealm,
      workspaceNoSubmissions,
      workspaceSuccessfulSubmission,
      workspaceFailedSubmission,
      workspaceSubmittedSubmission,
      workspaceMixedSubmissions,
      workspaceTerminatedSubmissions,
      workspaceToTestGrant)
    val saveAllWorkspacesAction = DBIO.sequence(allWorkspaces.map(workspaceQuery.save))

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(userProjectOwner),
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        rawlsUserQuery.save(userReaderViaGroup),
        rawlsGroupQuery.save(nestedProjectGroup),
        rawlsGroupQuery.save(dbGapAuthorizedUsersGroup),
        DBIO.sequence(billingProject.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(billingProject),
        DBIO.sequence(testProject1.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(testProject1),
        DBIO.sequence(testProject2.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(testProject2),
        DBIO.sequence(testProject3.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(testProject3),
        DBIO.sequence(workspaceGroups.map(rawlsGroupQuery.save).toSeq),
        rawlsGroupQuery.save(realm),
        rawlsGroupQuery.save(realm2),
        DBIO.sequence(workspaceWithRealmGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(controlledWorkspaceGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(otherWorkspaceWithRealmGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceNoSubmissionsGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceSuccessfulSubmissionGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceFailedSubmissionGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceSubmittedSubmissionGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceMixedSubmissionsGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceTerminatedSubmissionsGroups.map(rawlsGroupQuery.save).toSeq),
        DBIO.sequence(workspaceToTestGrantGroups.map(rawlsGroupQuery.save).toSeq),
        saveAllWorkspacesAction,
        workspaceQuery.insertUserSharePermissions(workspaceToTestGrantId, Seq(RawlsUserRef(userWriter.userSubjectId))),
        workspaceQuery.insertGroupSharePermissions(workspaceToTestGrantId, Seq(RawlsGroupRef(dbGapAuthorizedUsersGroup.groupName))),
        withWorkspaceContext(workspace)({ context =>
          DBIO.seq(
                entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

                methodConfigurationQuery.save(context, methodConfig),
                methodConfigurationQuery.save(context, methodConfig2),
                methodConfigurationQuery.save(context, methodConfig3),
                methodConfigurationQuery.save(context, methodConfigValid),
                methodConfigurationQuery.save(context, methodConfigUnparseable),
                methodConfigurationQuery.save(context, methodConfigNotAllSamples),
                methodConfigurationQuery.save(context, methodConfigAttrTypeMixup),
                methodConfigurationQuery.save(context, methodConfigArrayType),
                methodConfigurationQuery.save(context, methodConfigEntityUpdate),
                methodConfigurationQuery.save(context, methodConfigWorkspaceLibraryUpdate),

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
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.save(context, methodConfig),
            methodConfigurationQuery.save(context, methodConfig2),

            submissionQuery.create(context, submissionSuccessful1),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceFailedSubmission)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionFailed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceSubmittedSubmission)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionSubmitted),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceTerminatedSubmissions)( { context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionAborted2),
            submissionQuery.create(context, submissionSuccessful2),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }),
        withWorkspaceContext(workspaceMixedSubmissions)({ context =>
          DBIO.seq(
            entityQuery.save(context, Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)),

            methodConfigurationQuery.save(context, methodConfig),

            submissionQuery.create(context, submissionAborted1),
            submissionQuery.create(context, submissionMixed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        })
      )
    }
  }

  class MinimalTestData() extends TestData {
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace2", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set.empty)
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set.empty)
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set.empty)
    val ownerGroup2 = makeRawlsGroup(s"${wsName2} OWNER", Set.empty)
    val writerGroup2 = makeRawlsGroup(s"${wsName2} WRITER", Set.empty)
    val readerGroup2 = makeRawlsGroup(s"${wsName2} READER", Set.empty)
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), generateBillingGroups(RawlsBillingProjectName(wsName.namespace), Map.empty, Map.empty), "testBucketUrl", CreationStatuses.Ready, None, None)
    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))
    val workspace2 = Workspace(wsName2.namespace, wsName2.name, None, UUID.randomUUID().toString, "aBucket2", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup2, WorkspaceAccessLevels.Write -> writerGroup2, WorkspaceAccessLevels.Read -> readerGroup2),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup2, WorkspaceAccessLevels.Write -> writerGroup2, WorkspaceAccessLevels.Read -> readerGroup2))

    override def save() = {
      DBIO.seq(
        DBIO.sequence(billingProject.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        rawlsGroupQuery.save(ownerGroup2),
        rawlsGroupQuery.save(writerGroup2),
        rawlsGroupQuery.save(readerGroup2),
        workspaceQuery.save(workspace),
        workspaceQuery.save(workspace2),
        rawlsUserQuery.save(userReader)
      )
    }
  }

  /* This test data should remain constant! Changing this data set will likely break
   * many of the tests that rely on it. */
  class ConstantTestData() extends TestData {
    // setup workspace objects
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(UserInfo("writer-access", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userReader = RawlsUser(UserInfo("reader-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName} OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName} WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName} READER", Set(userReader))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), generateBillingGroups(RawlsBillingProjectName(wsName.namespace), Map(ProjectRoles.Owner -> Set(RawlsUser(userInfo))), Map.empty), "testBucketUrl", CreationStatuses.Ready, None, None)

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))
    )

    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", wsAttrs,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

    val sample1 = Entity("sample1", "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> AttributeEntityReference("Aliquot", "aliquot1"),
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")))

    val sample2 = Entity("sample2", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeString("huh?") ) )
    val sample3 = Entity("sample3", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"), AttributeName.withDefaultNS("confused") -> AttributeEntityReference("Sample", "sample1") ) )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"), AttributeName.withDefaultNS("cycle") -> AttributeEntityReference("Sample", "sample6")))
    val sample8 = Entity("sample8", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val pair1 = Entity("pair1", "Pair",
      Map(AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample2"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1"),
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")) )
    val pair2 = Entity("pair2", "Pair",
      Map(AttributeName.withDefaultNS("case") -> AttributeEntityReference("Sample", "sample3"),
        AttributeName.withDefaultNS("control") -> AttributeEntityReference("Sample", "sample1") ) )

    val sset1 = Entity("sset1", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample1"),
        AttributeEntityReference("Sample", "sample2"),
        AttributeEntityReference("Sample", "sample3"))) ) )
    val sset2 = new Entity("sset2", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Sample", "sample2"))) ) )

    val sset3 = Entity("sset3", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample5"),
        AttributeEntityReference("Sample", "sample6")))))

    val sset4 = Entity("sset4", "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(
        AttributeEntityReference("Sample", "sample7")))))

    val sset_empty = Entity("sset_empty", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList ))

    val ps1 = Entity("ps1", "PairSet",
      Map(AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList( Seq(AttributeEntityReference("Pair", "pair1"),
        AttributeEntityReference("Pair", "pair2"))) ) )

    val indiv1 = Entity("indiv1", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> AttributeEntityReference("SampleSet", "sset1") ) )

    val indiv2 = Entity("indiv2", "Individual",
      Map(AttributeName.withDefaultNS("sset") -> AttributeEntityReference("SampleSet", "sset2") ) )

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

    val allEntities = Seq(aliquot1, aliquot2, sample1, sample2, sample3, sample4, sample5, sample6, sample7, sample8, pair1, pair2, ps1, sset1, sset2, sset3, sset4, sset_empty, indiv1, indiv2)
    val allMCs = Seq(methodConfig, methodConfig2, methodConfig3, methodConfigValid, methodConfigUnparseable, methodConfigNotAllSamples, methodConfigAttrTypeMixup, methodConfigEntityUpdate)
    def saveAllMCs(context: SlickWorkspaceContext) = DBIO.sequence(allMCs map { mc => methodConfigurationQuery.save(context, mc) })

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(userOwner),
        DBIO.sequence(billingProject.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsUserQuery.save(userWriter),
        rawlsUserQuery.save(userReader),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
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
