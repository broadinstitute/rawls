package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{Counter, DefaultInstrumented, MetricName}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.WDLParserConfig
import org.broadinstitute.dsde.rawls.dataaccess.MockCromwellSwaggerClient.{
  makeToolInputParameter,
  makeToolOutputParameter,
  makeValueType,
  makeWorkflowDescription
}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.jdbc.MySQLProfile.api._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.CachingWDLParser
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceVersions.WorkspaceVersion
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import org.scalatest.Suite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.SQLTransactionRollbackException
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}

// initialize database tables and connection pool only once
object DbResource extends LazyLogging {
  // to override, e.g. to run against mysql:
  // $ sbt -Dtestdb=mysql test
  private val testdb = ConfigFactory.load.getStringOr("testdb", "mysql")

  val dataConfig = DatabaseConfig.forConfig[JdbcProfile](testdb)

  private val liquibaseConf = ConfigFactory.load().getConfig("liquibase")
  private val liquibaseChangeLog = liquibaseConf.getString("changelog")

  val dataSource = new SlickDataSource(dataConfig)(TestExecutionContext.testExecutionContext)
  dataSource.initWithLiquibase(liquibaseChangeLog, Map.empty)
  logger.info("executing liquibase a second time to verify changesets work on an already-initialized database ...")
  dataSource.initWithLiquibase(liquibaseChangeLog, Map.empty)

}

/**
 * Created by dvoet on 2/3/16.
 */
//noinspection TypeAnnotation,NameBooleanParameters,SqlDialectInspection,JavaMutatorMethodAccessedAsParameterless,ScalaUnnecessaryParentheses,SqlNoDataSourceInspection,RedundantBlock,ScalaUnusedSymbol
trait TestDriverComponent extends DriverComponent with DataAccess with DefaultInstrumented {
  this: Suite =>

  implicit override val executionContext = TestExecutionContext.testExecutionContext

  // Implicit counters are required for certain methods on WorkflowComponent and SubmissionComponent
  override lazy val metricBaseName = MetricName("test")
  implicit def wfStatusCounter(wfStatus: WorkflowStatus): Option[Counter] = Option(
    metrics.counter(s"${wfStatus.toString}")
  )
  implicit def subStatusCounter(subStatus: SubmissionStatus): Counter = metrics.counter(s"${subStatus.toString}")

  override val driver: JdbcProfile = DbResource.dataConfig.profile
  override val batchSize: Int = DbResource.dataConfig.config.getInt("batchSize")
  override val fetchSize: Int = DbResource.dataConfig.config.getInt("fetchSize")
  val slickDataSource = DbResource.dataSource

  val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                          OAuth2BearerToken("token"),
                          123,
                          RawlsUserSubjectId("123456789876543212345")
  )
  val testContext = RawlsRequestContext(userInfo)

  // NOTE: we previously truncated millis here for DB compatibility reasons, but this is is no longer necessary.
  // now only serves to encapsulate a Java-ism
  def currentTime() = new DateTime()
  val testDate = currentTime()

  val wdlParserConfig = WDLParserConfig(ConfigFactory.load().getConfig("wdl-parsing"))
  val mockCromwellSwaggerClient = new MockCromwellSwaggerClient()
  val wdlParser = new CachingWDLParser(wdlParserConfig, mockCromwellSwaggerClient)
  val methodConfigResolver = new MethodConfigResolver(wdlParser)

  // TODO: can we be any more targeted about inTransaction vs. inTransactionWithAttrTempTable here?
  // this is used by many tests to set up fixture data, which includes saving entities, therefore it needs
  // the temp table.
  def runAndWait[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect], duration: Duration = 1 minutes): R =
    Await.result(
      DbResource.dataSource.inTransactionWithAttrTempTable(
        Set(AttributeTempTableType.Entity, AttributeTempTableType.Workspace)
      )(_ => action.asInstanceOf[ReadWriteAction[R]]),
      duration
    )

  protected def runMultipleAndWait[R](count: Int, duration: Duration = 1 minutes)(
    actionGenerator: Int => DBIOAction[R, _ <: NoStream, _ <: Effect]
  ): R = {
    val futures = (1 to count) map { i => retryConcurrentModificationException(actionGenerator(i)) }
    Await.result(Future.sequence(futures), duration).head
  }

  private def retryConcurrentModificationException[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect]): Future[R] = {

    import scala.language.existentials

    val chain = (DbResource.dataSource.createEntityAttributeTempTable andThen action.map { x =>
      Thread.sleep((Math.random() * 500).toLong); x
    } andFinally DbResource.dataSource.dropEntityAttributeTempTable).withPinnedSession

    DbResource.dataSource.database.run(chain).recoverWith {
      case e: RawlsConcurrentModificationException => retryConcurrentModificationException(action)
      case rollbackException: SQLTransactionRollbackException
          if rollbackException.getMessage.contains("try restarting transaction") =>
        retryConcurrentModificationException(action)
    }
  }

  import driver.api._

  def createTestSubmission(workspace: Workspace,
                           methodConfig: MethodConfiguration,
                           submissionEntity: Entity,
                           rawlsUserEmail: WorkbenchEmail,
                           workflowEntities: Seq[Entity],
                           inputResolutions: Map[Entity, Seq[SubmissionValidationValue]],
                           /*
                           See https://broadinstitute.atlassian.net/browse/GAWB-645
                           That PR (and review) removed the test that was using these variables, but left the variables.

                           File a new ticket if you have opinions, that could include:
                           - Restoring the deleted test from GAWB-645
                           - Removing the variables and ignoring the deleted test
                           - other?

                           As is, these extra variables are ignored and passing the remaining tests by the 30+ callers
                           to this function.
                            */
                           thisValueDoesNothingSinceGawb645: Seq[Entity] = Nil,
                           thisValueIsAlsoNotUsed: Map[Entity, Seq[SubmissionValidationValue]] = Map.empty,
                           status: WorkflowStatus = WorkflowStatuses.Submitted,
                           useCallCache: Boolean = false,
                           deleteIntermediateOutputFiles: Boolean = false,
                           useReferenceDisks: Boolean = false,
                           memoryRetryMultiplier: Double = 1.0,
                           workflowFailureMode: Option[WorkflowFailureMode] = None,
                           individualWorkflowCost: Option[Float] = None,
                           externalEntityInfo: Option[ExternalEntityInfo] = None,
                           ignoreEmptyOutputs: Boolean = false
  ): Submission = {

    val workflows = workflowEntities map { ref =>
      val uuid = if (status == WorkflowStatuses.Queued) None else Option(UUID.randomUUID.toString)
      Workflow(uuid, status, testDate, Some(ref.toReference), inputResolutions(ref), cost = individualWorkflowCost)
    }

    val submissionId = UUID.randomUUID.toString

    Submission(
      submissionId = submissionId,
      submissionDate = testDate,
      submitter = rawlsUserEmail,
      methodConfigurationNamespace = methodConfig.namespace,
      methodConfigurationName = methodConfig.name,
      submissionEntity = Option(submissionEntity.toReference),
      submissionRoot = s"gs://${workspace.bucketName}/${submissionId}",
      workflows = workflows,
      status = SubmissionStatuses.Submitted,
      useCallCache = useCallCache,
      deleteIntermediateOutputFiles = deleteIntermediateOutputFiles,
      useReferenceDisks = useReferenceDisks,
      memoryRetryMultiplier = memoryRetryMultiplier,
      workflowFailureMode = workflowFailureMode,
      cost = individualWorkflowCost.map(_ * workflows.length),
      externalEntityInfo,
      ignoreEmptyOutputs = ignoreEmptyOutputs
    )
  }

  def billingProjectFromName(name: String) =
    RawlsBillingProject(RawlsBillingProjectName(name), CreationStatuses.Ready, None, None)

  def billingProjectFromName(name: String, billingProfileId: UUID) =
    RawlsBillingProject(RawlsBillingProjectName(name),
                        CreationStatuses.Ready,
                        None,
                        None,
                        billingProfileId = Some(billingProfileId.toString)
    )

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
                             isLocked: Boolean
  ) =
    Workspace(
      project.projectName.value,
      name,
      workspaceId,
      bucketName,
      workflowCollectionName,
      createdDate,
      createdDate,
      createdBy,
      attributes,
      isLocked,
      WorkspaceVersions.V2,
      GoogleProjectId(UUID.randomUUID().toString),
      Option(GoogleProjectNumber(UUID.randomUUID().toString)),
      project.billingAccount,
      None,
      Option(createdDate),
      WorkspaceType.RawlsWorkspace
    )
  def makeWorkspaceWithUsers(project: RawlsBillingProject,
                             name: String,
                             workspaceId: String,
                             bucketName: String,
                             workflowCollectionName: Option[String],
                             createdDate: DateTime,
                             lastModified: DateTime,
                             createdBy: String,
                             attributes: AttributeMap,
                             isLocked: Boolean,
                             workspaceVersion: WorkspaceVersion,
                             googleProjectId: GoogleProjectId,
                             googleProjectNumber: Option[GoogleProjectNumber],
                             currentBillingAccountOnWorkspace: Option[RawlsBillingAccountName],
                             errorMessage: Option[String],
                             completedCloneWorkspaceFileTransfer: Option[DateTime]
  ) =
    Workspace(
      project.projectName.value,
      name,
      workspaceId,
      bucketName,
      workflowCollectionName,
      createdDate,
      createdDate,
      createdBy,
      attributes,
      isLocked,
      workspaceVersion,
      googleProjectId,
      googleProjectNumber,
      currentBillingAccountOnWorkspace,
      errorMessage,
      completedCloneWorkspaceFileTransfer,
      WorkspaceType.RawlsWorkspace
    )

  class EmptyWorkspace() extends TestData {
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(
      UserInfo(RawlsUserEmail("writer-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace)
      )
  }

  class EmptyWorkspaceWithProjectAndBillingAccount(project: RawlsBillingProject,
                                                   maybeBillingAccount: Option[RawlsBillingAccountName]
  ) extends TestData {
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(
      UserInfo(RawlsUserEmail("writer-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val wsName = WorkspaceName(project.projectName.value, UUID.randomUUID().toString)
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))
    val workspaceVersion = WorkspaceVersions.V2
    val googleProjectId = project.googleProjectId
    val googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
    val billingAccount = maybeBillingAccount

    val workspace = Workspace(
      wsName.namespace,
      wsName.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty,
      false,
      workspaceVersion,
      googleProjectId,
      googleProjectNumber,
      billingAccount,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace
    )

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace)
      )
  }

  class LockedWorkspace() extends TestData {
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(
      UserInfo(RawlsUserEmail("writer-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val workspace = Workspace(
      wsName.namespace,
      wsName.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty,
      isLocked = true
    )

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace)
      )
  }

  // noinspection TypeAnnotation,ScalaUnnecessaryParentheses,ScalaUnusedSymbol
  class DefaultTestData() extends TestData {
    // setup workspace objects
    val userProjectOwner = RawlsUser(
      UserInfo(RawlsUserEmail("project-owner-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543210101")
      )
    )
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(
      UserInfo(RawlsUserEmail("writer-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val userReaderViaGroup = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access-via-group"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212349")
      )
    )
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
    val wsRegionalName = WorkspaceName("myNamespace", "myRegionalWorkspace")
    val workspaceToTestGrantId = UUID.randomUUID()

    val nestedProjectGroup = makeRawlsGroup("nested_project_group", Set(userOwner))
    val dbGapAuthorizedUsersGroup = ManagedGroupRef(RawlsGroupName("dbGapAuthorizedUsers"))

    val billingAccountName = RawlsBillingAccountName("fakeBillingAcct")

    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace),
                                             CreationStatuses.Ready,
                                             Option(billingAccountName),
                                             None
    )

    val testProject1Name = RawlsBillingProjectName("arbitrary")
    val testProject1 = RawlsBillingProject(testProject1Name, CreationStatuses.Ready, Option(billingAccountName), None)

    val testProject2Name = RawlsBillingProjectName("project2")
    val testProject2 = RawlsBillingProject(testProject2Name, CreationStatuses.Ready, Option(billingAccountName), None)

    val testProject3Name = RawlsBillingProjectName("project3")
    val testProject3 = RawlsBillingProject(testProject3Name, CreationStatuses.Ready, Option(billingAccountName), None)

    val azureBillingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .tenantId(UUID.randomUUID())
      .subscriptionId(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .managedResourceGroupId("fake-mrg")

    val azureBillingProjectName = RawlsBillingProjectName("azure-billing-project")
    val azureBillingProject = RawlsBillingProject(
      azureBillingProjectName,
      CreationStatuses.Ready,
      Option(billingAccountName),
      None,
      billingProfileId = Some(azureBillingProfile.getId.toString)
    )

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(
        Seq(AttributeString("another string"), AttributeString("true"))
      )
    )

    val workspaceNoGroups = Workspace(wsName.namespace,
                                      wsName.name + "3",
                                      UUID.randomUUID().toString,
                                      "aBucket2",
                                      Some("workflow-collection"),
                                      currentTime(),
                                      currentTime(),
                                      "testUser",
                                      wsAttrs
    )

    val workspace = makeWorkspaceWithUsers(billingProject,
                                           wsName.name,
                                           UUID.randomUUID().toString,
                                           "aBucket",
                                           Some("workflow-collection"),
                                           currentTime(),
                                           currentTime(),
                                           "testUser",
                                           wsAttrs,
                                           false
    )
    val workspaceLocked = makeWorkspaceWithUsers(
      billingProject,
      wsName.name + "_locked",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      true
    )
    val v1Workspace = makeWorkspaceWithUsers(
      billingProject,
      wsName.name + "v1",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false,
      WorkspaceVersions.V1,
      billingProject.googleProjectId,
      billingProject.googleProjectNumber,
      billingProject.billingAccount,
      None,
      Option(currentTime())
    )

    val regionalWorkspace = makeWorkspaceWithUsers(
      billingProject,
      wsRegionalName.name,
      UUID.randomUUID().toString,
      "fc-regional-bucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false
    )

    val workspacePublished = Workspace(
      wsName.namespace,
      wsName.name + "_published",
      UUID.randomUUID().toString,
      "aBucket3",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs + (AttributeName.withLibraryNS("published") -> AttributeBoolean(true))
    )
    val workspaceNoAttrs = Workspace(
      wsName.namespace,
      wsName.name + "_noattrs",
      UUID.randomUUID().toString,
      "aBucket4",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty
    )

    val realm = ManagedGroupRef(RawlsGroupName("Test-Realm"))
    val realmWsName = wsName.name + "withRealm"

    val realm2 = ManagedGroupRef(RawlsGroupName("Test-Realm2"))
    val realmWs2Name = wsName2.name + "withRealm"

    val workspaceWithRealm = makeWorkspaceWithUsers(billingProject,
                                                    realmWsName,
                                                    UUID.randomUUID().toString,
                                                    "aBucket",
                                                    Some("workflow-collection"),
                                                    currentTime(),
                                                    currentTime(),
                                                    "testUser",
                                                    wsAttrs,
                                                    false
    )

    val workspaceWithMultiGroupAD = makeWorkspaceWithUsers(billingProject,
                                                           wsName10.name,
                                                           UUID.randomUUID().toString,
                                                           "aBucket",
                                                           Some("workflow-collection"),
                                                           currentTime(),
                                                           currentTime(),
                                                           "testUser",
                                                           wsAttrs,
                                                           false
    )

    val controlledWorkspace = makeWorkspaceWithUsers(billingProject,
                                                     "test-tcga",
                                                     UUID.randomUUID().toString,
                                                     "aBucket",
                                                     Some("workflow-collection"),
                                                     currentTime(),
                                                     currentTime(),
                                                     "testUser",
                                                     wsAttrs,
                                                     false
    )

    val otherWorkspaceWithRealm = makeWorkspaceWithUsers(billingProject,
                                                         realmWs2Name,
                                                         UUID.randomUUID().toString,
                                                         "aBucket",
                                                         Some("workflow-collection"),
                                                         currentTime(),
                                                         currentTime(),
                                                         "testUser",
                                                         wsAttrs,
                                                         false
    )

    // Workspace with realms, without submissions
    val workspaceNoSubmissions = makeWorkspaceWithUsers(billingProject,
                                                        wsName3.name,
                                                        UUID.randomUUID().toString,
                                                        "aBucket",
                                                        Some("workflow-collection"),
                                                        currentTime(),
                                                        currentTime(),
                                                        "testUser",
                                                        wsAttrs,
                                                        false
    )

    // Workspace with no entities
    val workspaceNoEntities = makeWorkspaceWithUsers(billingProject,
                                                     "no-entities",
                                                     UUID.randomUUID().toString,
                                                     "aBucket",
                                                     Some("workflow-collection"),
                                                     currentTime(),
                                                     currentTime(),
                                                     "testUser",
                                                     wsAttrs,
                                                     false
    )

    // Workspace with realms, with successful submission
    val workspaceSuccessfulSubmission = makeWorkspaceWithUsers(billingProject,
                                                               wsName4.name,
                                                               UUID.randomUUID().toString,
                                                               "aBucket",
                                                               Some("workflow-collection"),
                                                               currentTime(),
                                                               currentTime(),
                                                               "testUser",
                                                               wsAttrs,
                                                               false
    )

    // Workspace with realms, with failed submission
    val workspaceFailedSubmission = makeWorkspaceWithUsers(billingProject,
                                                           wsName5.name,
                                                           UUID.randomUUID().toString,
                                                           "aBucket",
                                                           Some("workflow-collection"),
                                                           currentTime(),
                                                           currentTime(),
                                                           "testUser",
                                                           wsAttrs,
                                                           false
    )

    // Workspace with realms, with submitted submission
    val workspaceSubmittedSubmission = makeWorkspaceWithUsers(billingProject,
                                                              wsName6.name,
                                                              UUID.randomUUID().toString,
                                                              "aBucket",
                                                              Some("workflow-collection"),
                                                              currentTime(),
                                                              currentTime(),
                                                              "testUser",
                                                              wsAttrs,
                                                              false
    )

    // Workspace with realms with mixed workflows
    val workspaceMixedSubmissions = makeWorkspaceWithUsers(billingProject,
                                                           wsName7.name,
                                                           UUID.randomUUID().toString,
                                                           "aBucket",
                                                           Some("workflow-collection"),
                                                           currentTime(),
                                                           currentTime(),
                                                           "testUser",
                                                           wsAttrs,
                                                           false
    )

    // Workspace with realms, with aborted and successful submissions
    val workspaceTerminatedSubmissions = makeWorkspaceWithUsers(billingProject,
                                                                wsName8.name,
                                                                UUID.randomUUID().toString,
                                                                "aBucket",
                                                                Some("workflow-collection"),
                                                                currentTime(),
                                                                currentTime(),
                                                                "testUser",
                                                                wsAttrs,
                                                                false
    )

    // Workspace with a successful submission that had another submission run and fail while it was running
    val workspaceInterleavedSubmissions = makeWorkspaceWithUsers(
      billingProject,
      wsInterleaved.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false
    )

    // Workspace with a custom workflow failure mode
    val workspaceWorkflowFailureMode = makeWorkspaceWithUsers(
      billingProject,
      wsWorkflowFailureMode.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false
    )

    // Standard workspace to test grant permissions
    val workspaceToTestGrant = makeWorkspaceWithUsers(
      billingProject,
      wsName9.name,
      workspaceToTestGrantId.toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false
    )

    // Test copying configs between workspaces
    val workspaceConfigCopyDestination = makeWorkspaceWithUsers(
      billingProject,
      wsNameConfigCopyDestination.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false
    )

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val sample1 = Entity(
      "sample1",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> aliquot1.toReference,
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")
      )
    )

    val sample2 = Entity(
      "sample2",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
        AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"),
        AttributeName.withDefaultNS("confused") -> AttributeString("huh?")
      )
    )
    val sample3 = Entity(
      "sample3",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
        AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"),
        AttributeName.withDefaultNS("confused") -> sample1.toReference
      )
    )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7",
                         "Sample",
                         Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
                             AttributeName.withDefaultNS("cycle") -> sample6.toReference
                         )
    )
    val sample8 = Entity(
      "sample8",
      "Sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
          AttributeName.withDefaultNS("foo_id") -> AttributeString("1029384756")
      )
    )
    val extraSample = Entity("extraSample", "Sample", Map.empty)

    val pair1 = Entity(
      "pair1",
      "Pair",
      Map(
        AttributeName.withDefaultNS("case") -> sample2.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference,
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")
      )
    )
    val pair2 = Entity("pair2",
                       "Pair",
                       Map(AttributeName.withDefaultNS("case") -> sample3.toReference,
                           AttributeName.withDefaultNS("control") -> sample1.toReference
                       )
    )

    val sset1 = Entity(
      "sset1",
      "SampleSet",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          Seq(sample1.toReference, sample2.toReference, sample3.toReference)
        )
      )
    )
    val sset2 = Entity(
      "sset2",
      "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample2.toReference)))
    )

    val sset3 = Entity("sset3",
                       "SampleSet",
                       Map(
                         AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(
                           Seq(sample5.toReference, sample6.toReference)
                         )
                       )
    )

    val sset4 = Entity(
      "sset4",
      "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(sample7.toReference)))
    )

    val sset_empty =
      Entity("sset_empty", "SampleSet", Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList))

    val ps1 = Entity(
      "ps1",
      "PairSet",
      Map(
        AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList(Seq(pair1.toReference, pair2.toReference))
      )
    )

    val indiv1 = Entity("indiv1", "Individual", Map(AttributeName.withDefaultNS("sset") -> sset1.toReference))

    val indiv2 = Entity("indiv2", "Individual", Map(AttributeName.withDefaultNS("sset") -> sset2.toReference))

    val agoraMethod = AgoraMethod("ns-config", "meth1", 1)

    val agoraMethodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      agoraMethod
    )

    val agoraMethodConfigMaxWorkspaceAttributes = MethodConfiguration(
      "ns",
      "testConfigMaxWorkspaceAttributes",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("workspace.long_attributes")),
      agoraMethod
    )

    val agoraMethodConfigMaxEntityAttributes = MethodConfiguration(
      "ns",
      "testConfigMaxEntityAttributes",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("this.long_attributes")),
      agoraMethod
    )

    val goodAndBadMethod = AgoraMethod("dsde", "good_and_bad", 1)

    val goodAndBadMethodConfig = MethodConfiguration(
      "dsde",
      "good_and_bad",
      Some("samples"),
      Some(Map.empty[String, AttributeString]),
      Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.foo"),
          "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("does.not.parse")
      ),
      Map(
        "goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"),
        "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("also.does.not.parse"),
        "empty_out" -> AttributeString("")
      ),
      goodAndBadMethod
    )

    val dockstoreMethod = DockstoreMethod("dockstore-method-path", "dockstore-method-version")

    val dockstoreMethodConfig = MethodConfiguration(
      "dockstore-config-namespace",
      "dockstore-config-name",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      dockstoreMethod
    )

    val methodConfigDockstore = MethodConfiguration(
      "dsde",
      "DockstoreConfig",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("param1" -> AttributeString("foo"), "param2" -> AttributeString("foo2")),
      Map("out" -> AttributeString("bar")),
      DockstoreMethod("dockstore-path", "dockstore-version")
    )
    val methodConfig2 = MethodConfiguration(
      "dsde",
      "testConfig2",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("param1" -> AttributeString("foo")),
      Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")),
      AgoraMethod(wsName.namespace, "method-a", 1)
    )
    val methodConfig3 = MethodConfiguration(
      "dsde",
      "testConfig",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map("param1" -> AttributeString("foo"), "param2" -> AttributeString("foo2")),
      Map("out" -> AttributeString("bar")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigEntityUpdate = MethodConfiguration(
      "ns",
      "testConfig11",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("o1" -> AttributeString("this.foo")),
      AgoraMethod("ns-config", "meth1", 1)
    )
    val methodConfigWorkspaceUpdate = MethodConfiguration(
      "ns",
      "testConfig1",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("o1" -> AttributeString("workspace.foo")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigWorkspaceMaxAttributes = MethodConfiguration(
      "ns",
      "testConfigMaxWorkspaceAttributes",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("o1" -> AttributeString("this.foo")),
      AgoraMethod("ns-config", "meth1", 1)
    )
    val methodConfigEntityMaxAttributes = MethodConfiguration(
      "ns",
      "testConfigMaxEntityAttributes",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("o1" -> AttributeString("workspace.foo")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigWorkspaceLibraryUpdate = MethodConfiguration(
      "ns",
      "testConfigLib",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("o1" -> AttributeString("workspace.library:foo")),
      AgoraMethod("ns-config", "meth1", 1)
    )
    val methodConfigMissingOutputs = MethodConfiguration(
      "ns",
      "testConfigMissingOutputs",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(),
      Map("some.workflow.output" -> AttributeString("this.might_not_be_here")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigValid = MethodConfiguration(
      "dsde",
      "GoodMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.name")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigUnparseableInputs = MethodConfiguration(
      "dsde",
      "UnparseableInputsMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigUnparseableOutputs = MethodConfiguration(
      "dsde",
      "UnparseableOutputsMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.name")),
      outputs = Map("three_step.cgrep.count" -> AttributeString("this..wont.parse")),
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigUnparseableBoth = MethodConfiguration(
      "dsde",
      "UnparseableBothMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this..is...bad")),
      outputs = Map("three_step.cgrep.count" -> AttributeString("this..wont.parse")),
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigEmptyOutputs = MethodConfiguration(
      "dsde",
      "EmptyOutputsMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.name")),
      outputs = Map("three_step.cgrep.count" -> AttributeString("")),
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigNotAllSamples = MethodConfiguration(
      "dsde",
      "NotAllSamplesMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigAttrTypeMixup = MethodConfiguration(
      "dsde",
      "AttrTypeMixupMethodConfig",
      Some("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.confused")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )

    val methodConfigArrayType = MethodConfiguration(
      "dsde",
      "ArrayMethodConfig",
      Some("SampleSet"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map("aggregate_data_workflow.aggregate_data.input_array" -> AttributeString("this.samples.type")),
      outputs = Map("aggregate_data_workflow.aggregate_data.output_array" -> AttributeString("this.output_array")),
      AgoraMethod("dsde", "array_task", 1)
    )

    val methodConfigEntityless = MethodConfiguration(
      "ns",
      "Entityless",
      None,
      Some(Map.empty[String, AttributeString]),
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("\"bees\"")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )

    val agoraMethodConfigName = MethodConfigurationName(agoraMethodConfig.name, agoraMethodConfig.namespace, wsName)
    val dockstoreMethodConfigName =
      MethodConfigurationName(dockstoreMethodConfig.name, dockstoreMethodConfig.namespace, wsName)
    val methodConfigName2 = agoraMethodConfigName.copy(name = "novelName")
    val methodConfigName3 = agoraMethodConfigName.copy(name = "noSuchName")
    val methodConfigName4 = agoraMethodConfigName.copy(name = methodConfigWorkspaceLibraryUpdate.name)
    val methodConfigNamePairCreated = MethodConfigurationNamePair(agoraMethodConfigName, methodConfigName2)
    // Copy from "myNamespace/myWorkspace" to "myNamespace/configCopyDestinationWS"
    val methodConfigNamePairCreatedDockstore = MethodConfigurationNamePair(
      dockstoreMethodConfigName,
      dockstoreMethodConfigName.copy(workspaceName = wsNameConfigCopyDestination)
    )
    val methodConfigNamePairConflict = MethodConfigurationNamePair(agoraMethodConfigName, agoraMethodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3, methodConfigName2)
    val methodConfigNamePairFromLibrary = MethodConfigurationNamePair(methodConfigName4, methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, agoraMethodConfig.namespace, wsName)
    val methodRepoGood = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, newMethodConfigName)
    val methodRepoMissing =
      MethodRepoConfigurationImport("workspace_test", "rawls_test_missing", 1, agoraMethodConfigName)
    val methodRepoEmptyPayload =
      MethodRepoConfigurationImport("workspace_test", "rawls_test_empty_payload", 1, agoraMethodConfigName)
    val methodRepoBadPayload =
      MethodRepoConfigurationImport("workspace_test", "rawls_test_bad_payload", 1, agoraMethodConfigName)
    val methodRepoLibrary =
      MethodRepoConfigurationImport("workspace_test", "rawls_test_library", 1, newMethodConfigName)

    val methodConfigForWdlStruct = MethodConfiguration(
      "dsde",
      "WdlStructConfig",
      Some("Sample"),
      Some(Map.empty[String, AttributeString]),
      Map(
        "wdl_struct_wf.struct_obj" -> AttributeString("""{"id":this.participant_id,"sample_name":this.sample_name}""")
      ),
      Map.empty,
      AgoraMethod("dsde", "wdl_struct_wf", 1)
    )

    val methodConfigEntityUpdateReservedOutput = MethodConfiguration(
      namespace = "ns",
      name = "testConfigWithReservedOutput",
      rootEntityType = Option("Sample"),
      prerequisites = Some(Map.empty[String, AttributeString]),
      inputs = Map(),
      /*
      this.individual_id is a reserved and will always throw an exception when trying to attach an output to an
      individual.

      - https://broadinstitute.atlassian.net/browse/GAWB-3386
      - https://broadworkbench.atlassian.net/browse/WA-223
       */
      outputs = Map("o1" -> AttributeString("this.individual_id")),
      methodRepoMethod = AgoraMethod("ns-config", "meth1", 1)
    )

    val inputResolutions = Seq(
      SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name")
    )
    val inputResolutions2 = Seq(
      SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2")
    )
    val missingOutputResolutions = Seq(
      SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name")
    )

    val submissionNoWorkflows = createTestSubmission(
      workspace,
      agoraMethodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq.empty,
      Map.empty,
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )
    val submission1 = createTestSubmission(
      workspace,
      agoraMethodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )
    val regionalSubmission = createTestSubmission(
      regionalWorkspace,
      agoraMethodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )
    val costedSubmission1 = createTestSubmission(
      workspace,
      agoraMethodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2),
      // the constant value we set in MockSubmissionCostService
      individualWorkflowCost = Some(1.23f)
    )
    val submission2 = createTestSubmission(
      workspace,
      methodConfig2,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )

    val submissionUpdateEntity = createTestSubmission(
      workspace,
      methodConfigEntityUpdate,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(indiv1),
      Map(indiv1 -> inputResolutions),
      Seq(indiv2),
      Map(indiv2 -> inputResolutions2)
    )
    val submissionUpdateWorkspace = createTestSubmission(
      workspace,
      methodConfigWorkspaceUpdate,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(indiv1),
      Map(indiv1 -> inputResolutions),
      Seq(indiv2),
      Map(indiv2 -> inputResolutions2)
    )

    // NOTE: This is deliberately not saved in the list of active submissions!
    val submissionMissingOutputs = createTestSubmission(workspace,
                                                        methodConfigMissingOutputs,
                                                        indiv1,
                                                        WorkbenchEmail(userOwner.userEmail.value),
                                                        Seq(indiv1),
                                                        Map(indiv1 -> missingOutputResolutions),
                                                        Seq(),
                                                        Map()
    )

    // NOTE: This is deliberately not saved in the list of active submissions!
    val submissionUpdateEntityReservedOutput = createTestSubmission(
      workspace = workspace,
      methodConfig = methodConfigEntityUpdateReservedOutput,
      submissionEntity = indiv1,
      rawlsUserEmail = WorkbenchEmail(userOwner.userEmail.value),
      workflowEntities = Seq(indiv1),
      inputResolutions = Map(indiv1 -> inputResolutions)
    )

    val submissionMaxWorkspaceAttributes = createTestSubmission(
      workspace = workspace,
      methodConfig = methodConfigWorkspaceMaxAttributes,
      submissionEntity = indiv1,
      rawlsUserEmail = WorkbenchEmail(userOwner.userEmail.value),
      workflowEntities = Seq(indiv1),
      inputResolutions = Map(indiv1 -> inputResolutions)
    )

    val submissionMaxEntityAttributes = createTestSubmission(
      workspace = workspace,
      methodConfig = methodConfigEntityMaxAttributes,
      submissionEntity = indiv1,
      rawlsUserEmail = WorkbenchEmail(userOwner.userEmail.value),
      workflowEntities = Seq(indiv1),
      inputResolutions = Map(indiv1 -> inputResolutions)
    )

    // NOTE: This is deliberately not saved in the list of active submissions!
    val submissionNoRootEntity = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = methodConfigValid.namespace,
      methodConfigurationName = methodConfigValid.name,
      submissionEntity = None,
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowA"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = None,
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTerminateTest = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowA"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowB"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample2.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowC"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample3.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowD"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample4.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with a succeeeded workflow
    val submissionSuccessful1 = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowSuccessful1"),
          status = WorkflowStatuses.Succeeded,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with a succeeeded workflow
    val submissionSuccessful2 = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowSuccessful2"),
          status = WorkflowStatuses.Succeeded,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with a failed workflow
    val submissionFailed = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("worklowFailed"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with a submitted workflow
    val submissionSubmitted = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowSubmitted"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with an aborted workflow
    val submissionAborted1 = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowAborted1"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Aborted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with an aborted workflow
    val submissionAborted2 = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowAborted2"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Aborted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with multiple failed and succeeded workflows
    val submissionMixed = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowSuccessful3"),
          status = WorkflowStatuses.Succeeded,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowSuccessful4"),
          status = WorkflowStatuses.Succeeded,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample2.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("worklowFailed1"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample3.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowFailed2"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample4.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowSubmitted1"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample5.toReference),
          inputResolutions = inputResolutions
        ),
        Workflow(
          workflowId = Option("workflowSubmitted2"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample6.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // two submissions interleaved in time
    val t1 = new DateTime(2017, 1, 1, 5, 10)
    val t2 = new DateTime(2017, 1, 1, 5, 15)
    val t3 = new DateTime(2017, 1, 1, 5, 20)
    val t4 = new DateTime(2017, 1, 1, 5, 30)
    val outerSubmission = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = t1,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowSuccessful1"),
          status = WorkflowStatuses.Succeeded,
          statusLastChangedDate = t4,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )
    val innerSubmission = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = t2,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowFailed1"),
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = t3,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    // a submission with a submitted workflow and a custom workflow failure mode
    val submissionWorkflowFailureMode = Submission(
      submissionId = UUID.randomUUID().toString,
      submissionDate = testDate,
      submitter = WorkbenchEmail(userOwner.userEmail.value),
      methodConfigurationNamespace = agoraMethodConfig.namespace,
      methodConfigurationName = agoraMethodConfig.name,
      submissionEntity = Option(indiv1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = Option("workflowFailureMode"),
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false,
      workflowFailureMode = Option(WorkflowFailureModes.ContinueWhilePossible)
    )

    val azureWorkspace = new Workspace(
      namespace = azureBillingProjectName.value,
      name = "test-azure-workspace",
      workspaceId = UUID.randomUUID().toString,
      bucketName = "",
      workflowCollectionName = None,
      createdDate = currentTime(),
      lastModified = currentTime(),
      createdBy = "testUser",
      attributes = Map.empty,
      isLocked = false,
      workspaceVersion = WorkspaceVersions.V2,
      googleProjectId = GoogleProjectId(""),
      googleProjectNumber = None,
      currentBillingAccountOnGoogleProject = None,
      errorMessage = None,
      completedCloneWorkspaceFileTransfer = None,
      workspaceType = WorkspaceType.McWorkspace
    )

    val allWorkspaces = Seq(
      workspace,
      workspaceLocked,
      v1Workspace,
      controlledWorkspace,
      workspacePublished,
      workspaceNoAttrs,
      workspaceNoGroups,
      workspaceWithRealm,
      workspaceWithMultiGroupAD,
      otherWorkspaceWithRealm,
      workspaceNoSubmissions,
      workspaceNoEntities,
      workspaceSuccessfulSubmission,
      workspaceFailedSubmission,
      workspaceSubmittedSubmission,
      workspaceMixedSubmissions,
      workspaceTerminatedSubmissions,
      workspaceInterleavedSubmissions,
      workspaceWorkflowFailureMode,
      workspaceToTestGrant,
      workspaceConfigCopyDestination,
      regionalWorkspace,
      azureWorkspace
    )
    val saveAllWorkspacesAction = DBIO.sequence(allWorkspaces.map(workspaceQuery.createOrUpdate))

    override def save() = {
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        rawlsBillingProjectQuery.create(testProject1),
        rawlsBillingProjectQuery.create(testProject2),
        rawlsBillingProjectQuery.create(testProject3),
        saveAllWorkspacesAction,
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            methodConfigurationQuery.create(context, agoraMethodConfigMaxWorkspaceAttributes),
            methodConfigurationQuery.create(context, agoraMethodConfigMaxEntityAttributes),
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
            methodConfigurationQuery.create(context, methodConfigForWdlStruct),
            methodConfigurationQuery.create(context, methodConfigEntityUpdateReservedOutput),
            // HANDY HINT: if you're adding a new method configuration, don't reuse the name!
            // If you do, methodConfigurationQuery.create() will archive the old query and update it to point to the new one!

            submissionQuery.create(context, submissionTerminateTest),
            submissionQuery.create(context, submissionNoWorkflows),
            submissionQuery.create(context, submission1),
            submissionQuery.create(context, regionalSubmission),
            submissionQuery.create(context, costedSubmission1),
            submissionQuery.create(context, submission2),
            submissionQuery.create(context, submissionUpdateEntity),
            submissionQuery.create(context, submissionUpdateWorkspace),

            // update exec key for all test data workflows that have been started.
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceWithRealm) { context =>
          DBIO.seq(
            entityQuery.save(context, extraSample)
          )
        },
        withWorkspaceContext(workspaceWithMultiGroupAD) { context =>
          DBIO.seq(
            entityQuery.save(context, extraSample)
          )
        },
        withWorkspaceContext(workspaceNoSubmissions) { context =>
          DBIO.seq(
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceSuccessfulSubmission) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            methodConfigurationQuery.create(context, methodConfig2),
            submissionQuery.create(context, submissionSuccessful1),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceFailedSubmission) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, submissionFailed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceSubmittedSubmission) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, submissionSubmitted),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceTerminatedSubmissions) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, submissionAborted2),
            submissionQuery.create(context, submissionSuccessful2),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceMixedSubmissions) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, submissionAborted1),
            submissionQuery.create(context, submissionMixed),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceInterleavedSubmissions) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, outerSubmission),
            submissionQuery.create(context, innerSubmission),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        },
        withWorkspaceContext(workspaceWorkflowFailureMode) { context =>
          DBIO.seq(
            entityQuery.save(
              context,
              Seq(aliquot1,
                  aliquot2,
                  sample1,
                  sample2,
                  sample3,
                  sample4,
                  sample5,
                  sample6,
                  sample7,
                  sample8,
                  pair1,
                  pair2,
                  ps1,
                  sset1,
                  sset2,
                  sset3,
                  sset4,
                  sset_empty,
                  indiv1,
                  indiv2
              )
            ),
            methodConfigurationQuery.create(context, agoraMethodConfig),
            submissionQuery.create(context, submissionWorkflowFailureMode),
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }
      )
    }
  }

  class MinimalTestData() extends TestData {
    val billingProject = RawlsBillingProject(RawlsBillingProjectName("myNamespace"),
                                             CreationStatuses.Ready,
                                             Option(RawlsBillingAccountName("billingAccounts/000000-111111-222222")),
                                             None
    )
    val wsName = WorkspaceName(billingProject.projectName.value, "myWorkspace")
    val wsName2 = WorkspaceName(billingProject.projectName.value, "myWorkspace2")
    val v1WsName = WorkspaceName(billingProject.projectName.value, "myV1Workspace")
    val v1WsName2 = WorkspaceName(billingProject.projectName.value, "myV1Workspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set.empty)
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set.empty)
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set.empty)
    val ownerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-OWNER", Set.empty)
    val writerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-WRITER", Set.empty)
    val readerGroup2 = makeRawlsGroup(s"${wsName2.namespace}-${wsName2.name}-READER", Set.empty)
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )
    val workspace2 = Workspace(wsName2.namespace,
                               wsName2.name,
                               UUID.randomUUID().toString,
                               "aBucket2",
                               Some("workflow-collection"),
                               currentTime(),
                               currentTime(),
                               "testUser",
                               Map.empty
    )
    // TODO (CA-1235): Remove these after PPW Migration is complete
    val v1Workspace = Workspace(
      v1WsName.namespace,
      v1WsName.name,
      UUID.randomUUID().toString,
      "aBucket3",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty,
      false,
      WorkspaceVersions.V1,
      billingProject.googleProjectId,
      billingProject.googleProjectNumber,
      None,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace
    )
    val v1Workspace2 = Workspace(
      v1WsName2.namespace,
      v1WsName2.name,
      UUID.randomUUID().toString,
      "aBucket4",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty,
      false,
      WorkspaceVersions.V1,
      billingProject.googleProjectId,
      billingProject.googleProjectNumber,
      None,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace
    )

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        workspaceQuery.createOrUpdate(workspace2),
        // TODO (CA-1235): Remove these after PPW Migration is complete
        workspaceQuery.createOrUpdate(v1Workspace),
        workspaceQuery.createOrUpdate(v1Workspace2),
        rawlsBillingProjectQuery.create(billingProject)
      )
  }

  class LocalEntityProviderTestData() extends TestData {
    val workspaceName = WorkspaceName("namespace", "workspace-with-cache")
    val wsAttrs = Map(AttributeName.withDefaultNS("description") -> AttributeString("a description"))
    val creationTime = currentTime()
    val workspace = Workspace(
      workspaceName.namespace,
      workspaceName.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      creationTime,
      creationTime,
      "testUser",
      wsAttrs
    )

    val participant1 = Entity(
      "participant1",
      "participant",
      Map(AttributeName.withDefaultNS("attr1") -> AttributeString("value1"),
          AttributeName.withDefaultNS("attr2") -> AttributeString("value2")
      )
    )
    val sample1 = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("attr5") -> AttributeString("value5"),
          AttributeName.withDefaultNS("attr6") -> AttributeString("value6")
      )
    )

    val workspaceEntities = Seq(participant1, sample1)

    val workspaceAttrNameCacheEntries = workspaceEntities.groupBy(_.entityType).map { case (entityType, entities) =>
      entityType -> entities.flatMap(_.attributes.keys)
    }

    val workspaceEntityTypeCacheEntries = workspaceEntities.groupBy(_.entityType).view.mapValues(_.length).toMap

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            // note that we don't save sample1 here, it was only used to generate cache entries that will differ from what full queries return
            entityQuery.save(context, participant1),
            entityAttributeStatisticsQuery.batchInsert(workspace.workspaceIdAsUUID, workspaceAttrNameCacheEntries),
            entityTypeStatisticsQuery.batchInsert(workspace.workspaceIdAsUUID, workspaceEntityTypeCacheEntries)
          )
        }
      )

  }

  /* This test data should remain constant! Changing this data set will likely break
   * many of the tests that rely on it. */
  class ConstantTestData() extends TestData {
    // setup workspace objects
    val userOwner = RawlsUser(userInfo)
    val userWriter = RawlsUser(
      UserInfo(RawlsUserEmail("writer-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(RawlsUserEmail("reader-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val wsName = WorkspaceName("myNamespace", "myWorkspace")
    val wsName2 = WorkspaceName("myNamespace", "myWorkspace2")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set(userWriter))
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set(userReader))

    val billingProject =
      RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), CreationStatuses.Ready, None, None)

    val wsAttrs = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(
        Seq(AttributeString("another string"), AttributeString("true"))
      )
    )

    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              wsAttrs
    )

    val aliquot1 = Entity("aliquot1", "Aliquot", Map.empty)
    val aliquot2 = Entity("aliquot2", "Aliquot", Map.empty)

    val sample1 = Entity(
      "sample1",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("normal"),
        AttributeName.withDefaultNS("whatsit") -> AttributeNumber(100),
        AttributeName.withDefaultNS("thingies") -> AttributeValueList(Seq(AttributeString("a"), AttributeString("b"))),
        AttributeName.withDefaultNS("quot") -> aliquot1.toReference,
        AttributeName.withDefaultNS("somefoo") -> AttributeString("itsfoo")
      )
    )

    val sample2 = Entity(
      "sample2",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
        AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"),
        AttributeName.withDefaultNS("confused") -> AttributeString("huh?")
      )
    )
    val sample3 = Entity(
      "sample3",
      "Sample",
      Map(
        AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
        AttributeName.withDefaultNS("tumortype") -> AttributeString("LUSC"),
        AttributeName.withDefaultNS("confused") -> sample1.toReference
      )
    )
    val sample4 = Entity("sample4", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample5 = Entity("sample5", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample6 = Entity("sample6", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))
    val sample7 = Entity("sample7",
                         "Sample",
                         Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor"),
                             AttributeName.withDefaultNS("cycle") -> sample6.toReference
                         )
    )
    val sample8 = Entity("sample8", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("tumor")))

    val pair1 = Entity(
      "pair1",
      "Pair",
      Map(
        AttributeName.withDefaultNS("case") -> sample2.toReference,
        AttributeName.withDefaultNS("control") -> sample1.toReference,
        AttributeName.withDefaultNS("whatsit") -> AttributeString("occurs in sample too! oh no!")
      )
    )
    val pair2 = Entity("pair2",
                       "Pair",
                       Map(AttributeName.withDefaultNS("case") -> sample3.toReference,
                           AttributeName.withDefaultNS("control") -> sample1.toReference
                       )
    )

    val sset1 = Entity(
      "sset1",
      "SampleSet",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          Seq(sample1.toReference, sample2.toReference, sample3.toReference)
        )
      )
    )
    val sset2 = Entity(
      "sset2",
      "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample2.toReference)))
    )

    val sset3 = Entity("sset3",
                       "SampleSet",
                       Map(
                         AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(
                           Seq(sample5.toReference, sample6.toReference)
                         )
                       )
    )

    val sset4 = Entity(
      "sset4",
      "SampleSet",
      Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(Seq(sample7.toReference)))
    )

    val sset_empty =
      Entity("sset_empty", "SampleSet", Map(AttributeName.withDefaultNS("samples") -> AttributeValueEmptyList))

    val ps1 = Entity(
      "ps1",
      "PairSet",
      Map(
        AttributeName.withDefaultNS("pairs") -> AttributeEntityReferenceList(Seq(pair1.toReference, pair2.toReference))
      )
    )

    val indiv1 = Entity("indiv1", "Individual", Map(AttributeName.withDefaultNS("sset") -> sset1.toReference))

    val indiv2 = Entity("indiv2", "Individual", Map(AttributeName.withDefaultNS("sset") -> sset2.toReference))

    val methodConfig = MethodConfiguration(
      "ns",
      "testConfig1",
      Some("Sample"),
      None,
      Map("i1" -> AttributeString("input")),
      Map("o1" -> AttributeString("output")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfig2 = MethodConfiguration(
      "dsde",
      "testConfig2",
      Some("Sample"),
      None,
      Map("param1" -> AttributeString("foo")),
      Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")),
      AgoraMethod(wsName.namespace, "method-a", 1)
    )
    val methodConfig3 = MethodConfiguration(
      "dsde",
      "testConfig",
      Some("Sample"),
      None,
      Map("param1" -> AttributeString("foo"), "param2" -> AttributeString("foo2")),
      Map("out" -> AttributeString("bar")),
      AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigEntityUpdate = MethodConfiguration("ns",
                                                       "testConfig11",
                                                       Some("Spample"),
                                                       None,
                                                       Map(),
                                                       Map("o1" -> AttributeString("this.foo")),
                                                       AgoraMethod("ns-config", "meth1", 1)
    )
    val methodConfigWorkspaceUpdate = MethodConfiguration("ns",
                                                          "testConfig1",
                                                          Some("Sample"),
                                                          None,
                                                          Map(),
                                                          Map("o1" -> AttributeString("workspace.foo")),
                                                          AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigMaxAttributeWorkspaceUpdate = MethodConfiguration("ns",
                                                                      "testConfig1",
                                                                      Some("Sample"),
                                                                      None,
                                                                      Map(),
                                                                      Map("o1" -> AttributeString("workspace.foo")),
                                                                      AgoraMethod("ns-config", "meth1", 1)
    )
    val methodConfigMaxAttributeEntityUpdate = MethodConfiguration("ns",
                                                                   "testConfig11",
                                                                   Some("Spample"),
                                                                   None,
                                                                   Map(),
                                                                   Map("o1" -> AttributeString("this.foo")),
                                                                   AgoraMethod("ns-config", "meth1", 1)
    )

    val methodConfigValid = MethodConfiguration(
      "dsde",
      "GoodMethodConfig",
      Some("Sample"),
      prerequisites = None,
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.name")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigUnparseable = MethodConfiguration(
      "dsde",
      "UnparseableMethodConfig",
      Some("Sample"),
      prerequisites = None,
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this..wont.parse")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigNotAllSamples = MethodConfiguration(
      "dsde",
      "NotAllSamplesMethodConfig",
      Some("Sample"),
      prerequisites = None,
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.tumortype")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )
    val methodConfigAttrTypeMixup = MethodConfiguration(
      "dsde",
      "AttrTypeMixupMethodConfig",
      Some("Sample"),
      prerequisites = None,
      inputs = Map("three_step.cgrep.pattern" -> AttributeString("this.confused")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1)
    )

    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, wsName)
    val methodConfigName2 = methodConfigName.copy(name = "novelName")
    val methodConfigName3 = methodConfigName.copy(name = "noSuchName")
    val methodConfigNamePairCreated = MethodConfigurationNamePair(methodConfigName, methodConfigName2)
    val methodConfigNamePairConflict = MethodConfigurationNamePair(methodConfigName, methodConfigName)
    val methodConfigNamePairNotFound = MethodConfigurationNamePair(methodConfigName3, methodConfigName2)
    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, wsName)

    val inputResolutions = Seq(
      SubmissionValidationValue(Option(AttributeString("value")), Option("message"), "test_input_name")
    )
    val inputResolutions2 = Seq(
      SubmissionValidationValue(Option(AttributeString("value2")), Option("message2"), "test_input_name2")
    )

    val submissionNoWorkflows = createTestSubmission(
      workspace,
      methodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq.empty,
      Map.empty,
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )
    val submission1 = createTestSubmission(
      workspace,
      methodConfig,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )
    val submission2 = createTestSubmission(
      workspace,
      methodConfig2,
      indiv1,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> inputResolutions, sample2 -> inputResolutions, sample3 -> inputResolutions),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> inputResolutions2, sample5 -> inputResolutions2, sample6 -> inputResolutions2)
    )

    val allEntities = Seq(aliquot1,
                          aliquot2,
                          sample1,
                          sample2,
                          sample3,
                          sample4,
                          sample5,
                          sample6,
                          sample7,
                          sample8,
                          pair1,
                          pair2,
                          ps1,
                          sset1,
                          sset2,
                          sset3,
                          sset4,
                          sset_empty,
                          indiv1,
                          indiv2
    )
    val allMCs = Seq(
      methodConfig,
      methodConfig2,
      methodConfig3,
      methodConfigValid,
      methodConfigUnparseable,
      methodConfigNotAllSamples,
      methodConfigAttrTypeMixup,
      methodConfigEntityUpdate
    )
    def saveAllMCs(context: Workspace) = DBIO.sequence(allMCs map { mc =>
      methodConfigurationQuery.create(context, mc)
    })

    override def save() =
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, allEntities),
            saveAllMCs(context),
            submissionQuery.create(context, submissionNoWorkflows),
            submissionQuery.create(context, submission1),
            submissionQuery.create(context, submission2)
          )
        }
      )
  }

  val emptyData = new TestData() {
    override def save() =
      DBIO.successful(())
  }

  def withEmptyTestDatabase[T](testCode: => T): T =
    withCustomTestDatabaseInternal(emptyData)(testCode)

  def withEmptyTestDatabase[T](testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(emptyData)(testCode(slickDataSource))

  val testData = new DefaultTestData()
  val constantData = new ConstantTestData()
  val minimalTestData = new MinimalTestData()
  val localEntityProviderTestData = new LocalEntityProviderTestData()

  def withDefaultTestDatabase[T](testCode: => T): T =
    withCustomTestDatabaseInternal(testData)(testCode)

  def withDefaultTestDatabase[T](testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(testData)(testCode(slickDataSource))

  def withMinimalTestDatabase[T](testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(minimalTestData)(testCode(slickDataSource))

  def withLocalEntityProviderTestDatabase[T](testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(localEntityProviderTestData)(testCode(slickDataSource))

  def withConstantTestDatabase[T](testCode: => T): T =
    withCustomTestDatabaseInternal(constantData)(testCode)

  def withConstantTestDatabase[T](testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(constantData)(testCode(slickDataSource))

  def withCustomTestDatabase[T](data: TestData)(testCode: SlickDataSource => T): T =
    withCustomTestDatabaseInternal(data)(testCode(slickDataSource))

  def withCustomTestDatabaseInternal[T](data: TestData)(testCode: => T): T =
    withCustomTestDatabaseInternal(slickDataSource, data)(testCode)

  def withCustomTestDatabaseInternal[T](dataSource: SlickDataSource, data: TestData)(testCode: => T): T = {
    runAndWait(DBIO.seq(dataSource.dataAccess.truncateAll), 2 minutes)
    try {
      runAndWait(data.save())
      testCode
    } catch {
      case t: Throwable => t.printStackTrace; throw t
    }
  }

  def withWorkspaceContext[T](workspace: Workspace)(testCode: (Workspace) => T): T =
    testCode(workspace)

  def updateWorkflowExecutionServiceKey(execKey: String) =
    // when unit tests seed the test data with workflows, those workflows may land in the database as already-started.
    // however, the runtime create() methods we use to seed the data do not set EXEC_SERVICE_KEY, since that should
    // only be set when a workflow is submitted. Therefore, we have this test-only raw sql to update those
    // workflows to an appropriate EXEC_SERVICE_KEY.
    sql"update WORKFLOW set EXEC_SERVICE_KEY = ${execKey} where EXEC_SERVICE_KEY is null and EXTERNAL_ID is not null;"
      .as[Int]

  val threeStepWDL =
    WdlSource("""
                |task ps {
                |  command {
                |    ps
                |  }
                |  output {
                |    File procs = stdout()
                |  }
                |}
                |
                |task cgrep {
                |  File in_file
                |  String pattern
                |  command {
                |    grep '${pattern}' ${in_file} | wc -l
                |  }
                |  output {
                |    Int count = read_int(stdout())
                |  }
                |}
                |
                |task wc {
                |  File in_file
                |  command {
                |    cat ${in_file} | wc -l
                |  }
                |  output {
                |    Int count = read_int(stdout())
                |  }
                |}
                |
                |workflow three_step {
                |  call ps
                |  call cgrep {
                |    input: in_file=ps.procs
                |  }
                |  call wc {
                |    input: in_file=ps.procs
                |  }
                |}
    """.stripMargin)

  val threeStepWDLName = "three_step"

  val patternInput = makeToolInputParameter("cgrep.pattern", false, makeValueType("String"), "String")
  val cgrepcountOutput = makeToolOutputParameter("cgrep.count", makeValueType("Int"), "Int")
  val wccountOutput = makeToolOutputParameter("wc.count", makeValueType("Int"), "Int")
  val psprocsOutput = makeToolOutputParameter("ps.procs", makeValueType("File"), "File")
  val threeStepWDLWorkflowDescription =
    makeWorkflowDescription(threeStepWDLName, List(patternInput), List(cgrepcountOutput, wccountOutput, psprocsOutput))
  mockCromwellSwaggerClient.workflowDescriptions += (threeStepWDL -> threeStepWDLWorkflowDescription)

  val threeStepMethod = AgoraEntity(Some("dsde"),
                                    Some(threeStepWDLName),
                                    Some(1),
                                    None,
                                    None,
                                    None,
                                    None,
                                    Option(threeStepWDL.source),
                                    None,
                                    Some(AgoraEntityType.Workflow)
  )

  val threeStepDockstoreWDLName = threeStepWDLName + "_dockstore"
  val patternInputDockstore = makeToolInputParameter("cgrep.pattern", false, makeValueType("String"), "String")
  val cgrepcountOutputDockstore = makeToolOutputParameter("cgrep.count", makeValueType("Int"), "Int")
  val wccountOutputDockstore = makeToolOutputParameter("wc.count", makeValueType("Int"), "Int")
  val psprocsOutputDockstore = makeToolOutputParameter("ps.procs", makeValueType("File"), "File")
  val threeStepDockStoreWDLWorkflowDescription = makeWorkflowDescription(
    threeStepDockstoreWDLName,
    List(patternInputDockstore),
    List(cgrepcountOutputDockstore, wccountOutputDockstore, psprocsOutputDockstore)
  )
  mockCromwellSwaggerClient.workflowDescriptions += WdlUrl(
    "/url-to-github/from/ga4gh-url-field/three-step-dockstore"
  ) -> threeStepDockStoreWDLWorkflowDescription

  val noInputWdl =
    WdlSource("""
                |task t1 {
                |  command {
                |    echo "Hello"
                |  }
                |}
                |
                |workflow w1 {
                |  call t1
                |}
    """.stripMargin)

  val noInputWdlWorkflowDescription = makeWorkflowDescription("w1", List(), List())
  mockCromwellSwaggerClient.workflowDescriptions += (noInputWdl -> noInputWdlWorkflowDescription)

  val noInputMethodDockstoreWDLSource =
    noInputWdl.source.replace("t1", "t1_dockstore")
  val noInputMethodDockstoreResponse =
    s"""{"type":"WDL","descriptor":"${noInputMethodDockstoreWDLSource
        .replace("\"", "\\\"")
        .replace("\n", "\\n")}","url":"/url-to-github/from/ga4gh-url-field/no-input-dockstore"}"""
  val noInputMethodDockstoreWorkflowDescription = makeWorkflowDescription("w1", List(), List())
  mockCromwellSwaggerClient.workflowDescriptions += WdlUrl(
    "/url-to-github/from/ga4gh-url-field/no-input-dockstore"
  ) -> noInputWdlWorkflowDescription

  val noInputMethod = AgoraEntity(Some("dsde"),
                                  Some("no_input"),
                                  Some(1),
                                  None,
                                  None,
                                  None,
                                  None,
                                  Option(noInputWdl.source),
                                  None,
                                  Some(AgoraEntityType.Workflow)
  )

  val goodAndBadInputsWDL =
    WdlSource("""
                |workflow goodAndBad {
                |  call goodAndBadTask
                |}
                |
                |task goodAndBadTask {
                |  String good_in
                |  String bad_in
                |  command {
                |    echo "hello world"
                |  }
                |  output {
                |    String good_out = "everything is good"
                |    String bad_out = "everything is bad"
                |    String empty_out = "everything is empty"
                |  }
                |}
    """.stripMargin)

  val goodAndBadWDLName = "goodAndBad"
  val badIn = makeToolInputParameter("goodAndBadTask.bad_in", false, makeValueType("String"), "String")
  val goodIn = makeToolInputParameter("goodAndBadTask.good_in", false, makeValueType("String"), "String")
  val goodOut = makeToolOutputParameter("goodAndBadTask.good_out", makeValueType("String"), "String")
  val badOut = makeToolOutputParameter("goodAndBadTask.bad_out", makeValueType("String"), "String")
  val emptyOut = makeToolOutputParameter("goodAndBadTask.empty_out", makeValueType("String"), "String")
  val goodAndBadWDLWorkflowDescription =
    makeWorkflowDescription(goodAndBadWDLName, List(badIn, goodIn), List(goodOut, badOut, emptyOut))
  mockCromwellSwaggerClient.workflowDescriptions += (goodAndBadInputsWDL -> goodAndBadWDLWorkflowDescription)

  val meth1WDL =
    WdlSource("""
                |workflow meth1 {
                |  call method1
                |}
                |
                |task method1 {
                |  String i1
                |  command {
                |    echo "hello world"
                |  }
                |  output {
                |    String o1 = "output one"
                |  }
                |}
    """.stripMargin)

  val meth1WDLName = "meth1"
  val i1Input = makeToolInputParameter("method1.i1", false, makeValueType("String"), "String")
  val o1Output = makeToolOutputParameter("method1.o1", makeValueType("String"), "String")
  val meth1WDLWorkflowDescription = makeWorkflowDescription(meth1WDLName, List(i1Input), List(o1Output))
  mockCromwellSwaggerClient.workflowDescriptions += (meth1WDL -> meth1WDLWorkflowDescription)

  val wdlStructWDL =
    WdlSource("""
                |version 1.0
                |
                |struct Participant {
                |  Int id
                |  String sample_name
                |}
                |
                |task do_something {
                |  input {
                |    Int id
                |    String sample_name
                |  }
                |
                |  command {
                |    echo "Hello participant ~{id} ~{sample_name} !"
                |  }
                |
                |  runtime {
                |    docker: "docker image"
                |  }
                |}
                |
                |workflow wdl_struct_wf {
                |  input {
                |    Participant struct_obj
                |  }
                |
                |  call do_something {input: id = struct_obj.id, sample_name = struct_obj.sample_name}
                |}
      """.stripMargin)

  val wdlStructWDLName = "wdl_struct_wf"
  val wdlStructWDLInputName = "wdl_struct_wf.struct_obj"
  val wdlStructWDLInput = makeToolInputParameter("struct_obj", false, makeValueType("Object"), "Participant")
  val wdlStructWDLWorkflowDescription = makeWorkflowDescription(wdlStructWDLName, List(wdlStructWDLInput), List.empty)
  mockCromwellSwaggerClient.workflowDescriptions += (wdlStructWDL -> wdlStructWDLWorkflowDescription)

  val wdlStructMethod = AgoraEntity(Some("dsde"),
                                    Some(wdlStructWDLName),
                                    Some(1),
                                    None,
                                    None,
                                    None,
                                    None,
                                    Option(wdlStructWDL.source),
                                    None,
                                    Some(AgoraEntityType.Workflow)
  )
}

trait TestData {
  def save(): ReadWriteAction[Unit]
}

trait TestDriverComponentWithFlatSpecAndMatchers extends AnyFlatSpec with TestDriverComponent with Matchers
