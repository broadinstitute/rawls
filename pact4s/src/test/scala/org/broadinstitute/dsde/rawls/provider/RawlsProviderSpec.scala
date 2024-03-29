package org.broadinstitute.dsde.rawls.provider

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import bio.terra.workspace.model.{
  CloningInstructionsEnum,
  DataRepoSnapshotAttributes,
  DataRepoSnapshotResource,
  ResourceMetadata,
  ResourceType,
  StewardshipType
}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingProjectOrchestrator
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceCluster, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, RawlsRequestContext, SnapshotListResponse, UserInfo}
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.RawlsApiServiceImpl
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{reset, when}
import org.mockito.stubbing.OngoingStubbing
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import pact4s.provider.Authentication.BasicAuth
import pact4s.provider.{ConsumerVersionSelectors, PactSource, ProviderInfoBuilder, ProviderTags}
import pact4s.scalatest.PactVerifier
import pact4s.provider._
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pact4s.provider.StateManagement.StateManagementFunction

import java.lang.Thread.sleep
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.FiniteDuration

object States {
  val oneSnapshot = "one snapshot in the given workspace"
}

class RawlsProviderSpec extends AnyFlatSpec with BeforeAndAfterAll with PactVerifier {

  implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val system: ActorSystem = ActorSystem("rawlstests")
  implicit val materializer: Materializer.type = Materializer
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val mockMultiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService =
    _ => mock[MultiCloudWorkspaceService]
  val mockWorkspaceServiceConstructor: RawlsRequestContext => WorkspaceService =
    _ => mock[WorkspaceService]
  val mockEntityServiceConstructor: RawlsRequestContext => EntityService = _ => mock[EntityService]
  val mockUserServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService]
  val mockGenomicsServiceConstructor: RawlsRequestContext => GenomicsService =
    _ => mock[GenomicsService]
  val mockSnapshotServiceConstructor: RawlsRequestContext => SnapshotService =
    _ => mock[SnapshotService]
  val mockSpendReportingConstructor: RawlsRequestContext => SpendReportingService =
    _ => mock[SpendReportingService]
  val mockBillingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator =
    _ => mock[BillingProjectOrchestrator]
  val mockBucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService =
    _ => mock[BucketMigrationService]
  val mockStatusServiceConstructor: () => StatusService = mock[() => StatusService]
  val mockExecutionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster]
  val mockAppVersion: ApplicationVersion = mock[ApplicationVersion]
  val mockSamDAO: SamDAO = mock[SamDAO]
  val mockOpenIDConnectConfiguration: OpenIDConnectConfiguration = mock[OpenIDConnectConfiguration]
  val mockUserInfo: UserInfo = mock[UserInfo]
  val mockOtelContext: Option[Context] = mock[Option[Context]]

  val rawlsApiService = new RawlsApiServiceImpl(
    mockMultiCloudWorkspaceServiceConstructor,
    mockWorkspaceServiceConstructor,
    mockEntityServiceConstructor,
    mockUserServiceConstructor,
    mockGenomicsServiceConstructor,
    mockSnapshotServiceConstructor,
    mockSpendReportingConstructor,
    mockBillingProjectOrchestratorConstructor,
    mockBucketMigrationServiceConstructor,
    mockStatusServiceConstructor,
    mockExecutionServiceCluster,
    mockAppVersion,
    FiniteDuration(1, TimeUnit.MINUTES),
    1000000L,
    "test",
    mockSamDAO,
    mockOpenIDConnectConfiguration
  )

  // Create ResourceMetadata
  val resourceMetadata = new ResourceMetadata()
    .workspaceId(UUID.randomUUID())
    .resourceId(UUID.randomUUID())
    .name("testName")
    .description("testDescription")
    .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
    .stewardshipType(StewardshipType.REFERENCED)
    .cloningInstructions(CloningInstructionsEnum.NOTHING)

  // Create DataRepoSnapshotAttributes
  val dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes()
    .instanceName("testInstanceName")
    .snapshot("testSnapshot")

  // Create DataRepoSnapshotResource
  val dataRepoSnapshotResource = new DataRepoSnapshotResource()
    .metadata(resourceMetadata)
    .attributes(dataRepoSnapshotAttributes)

  // Create Seq[DataRepoSnapshotResource]
  val dataRepoSnapshotResources: Seq[DataRepoSnapshotResource] = Seq(dataRepoSnapshotResource)

  private val mockedEnumerateSnapshotsResponse = SnapshotListResponse(dataRepoSnapshotResources)

  private val providerStatesHandler: StateManagementFunction = StateManagementFunction {
    case ProviderState(States.oneSnapshot, _) =>
      mockEnumerateSnapshots(
        mockSnapshotServiceConstructor(RawlsRequestContext(userInfo = mockUserInfo, otelContext = mockOtelContext)),
        mockedEnumerateSnapshotsResponse
      )
    case _ =>
      loggerIO.debug("State not found")
  }

  private def mockEnumerateSnapshots(mockSnapshotService: SnapshotService,
                                     mockResponse: SnapshotListResponse
  ): OngoingStubbing[Future[SnapshotListResponse]] =
    when {
      mockSnapshotService.enumerateSnapshotsById(anyString(), anyInt(), anyInt(), any[Option[UUID]])
    } thenReturn {
      Future.successful(mockResponse)
    }

  lazy val pactBrokerUrl: String = sys.env.getOrElse("PACT_BROKER_URL", "http://localhost:9292")
  lazy val pactBrokerUser: String = sys.env.getOrElse("PACT_BROKER_USERNAME", "")
  lazy val pactBrokerPass: String = sys.env.getOrElse("PACT_BROKER_PASSWORD", "")
  // Provider branch, semver
  lazy val providerBranch: String = sys.env.getOrElse("PROVIDER_BRANCH", "")
  lazy val providerVer: String = sys.env.getOrElse("PROVIDER_VERSION", "")
  // Consumer name, branch, semver (used for webhook events only)
  lazy val consumerName: Option[String] = sys.env.get("CONSUMER_NAME")
//  lazy val consumerBranch: Option[String] = sys.env.get("CONSUMER_BRANCH")
  lazy val consumerBranch: Option[String] = Some("aj-1697-rawls-contract")
  // This matches the latest commit of the consumer branch that triggered the webhook event
  lazy val consumerVer: Option[String] = sys.env.get("CONSUMER_VERSION")

  var consumerVersionSelectors: ConsumerVersionSelectors = ConsumerVersionSelectors()
  // consumerVersionSelectors = consumerVersionSelectors.mainBranch
  // The following match condition basically says
  // 1. If verification is triggered by consumer pact change, verify only the changed pact.
  // 2. For normal Sam PR, verify all consumer pacts in Pact Broker labelled with a deployed environment (alpha, dev, prod, staging).
  consumerBranch match {
    case Some(s) if !s.isBlank => consumerVersionSelectors = consumerVersionSelectors.branch(s, consumerName)
    case _                     => consumerVersionSelectors = consumerVersionSelectors.deployedOrReleased.mainBranch
  }

  val provider: ProviderInfoBuilder =
    ProviderInfoBuilder(
      name = "rawls",
      pactSource = PactSource
        .PactBrokerWithSelectors(pactBrokerUrl)
        .withAuth(BasicAuth(pactBrokerUser, pactBrokerPass))
        .withPendingPactsEnabled(ProviderTags(providerBranch)) // TODO providerBranch or providerVer?
        .withConsumerVersionSelectors(consumerVersionSelectors)
    )
      .withStateManagementFunction(
        providerStatesHandler
//          .withBeforeEach(() => resetMocks())
      )
      .withHost("localhost")
      .withPort(9292)
//      .withPort(8080)

  override def beforeAll(): Unit = {
    startRawls.unsafeToFuture()
    startRawls.start
    sleep(5000)

  }

  private def startRawls: IO[Http.ServerBinding] =
    for {
      binding <- IO
        .fromFuture(IO(Http().newServerAt("localhost", 8080).bind(rawlsApiService.route)))
        .onError { t: Throwable =>
          loggerIO.error(t.toString)
        }
      _ <- IO.fromFuture(IO(binding.whenTerminated))
      _ <- IO(system.terminate())
    } yield binding

//  def resetMocks(): Unit = {
//    reset(mockOpenIDConnectConfiguration)
//    reset(mockMultiCloudWorkspaceServiceConstructor)
//    reset(mockWorkspaceServiceConstructor)
//    reset(mockEntityServiceConstructor)
//    reset(mockUserServiceConstructor)
//    reset(mockGenomicsServiceConstructor)
//    reset(mockSnapshotServiceConstructor)
//    reset(mockSpendReportingConstructor)
//    reset(mockBillingProjectOrchestratorConstructor)
//    reset(mockBucketMigrationServiceConstructor)
//    reset(mockStatusServiceConstructor)
//    reset(mockExecutionServiceCluster)
//    reset(mockAppVersion)
//    reset(mockSamDAO)
//  }

  it should "Verify pacts" in {
    val publishResults = sys.env.getOrElse("PACT_PUBLISH_RESULTS", "false").toBoolean
    verifyPacts(
      providerBranch = if (providerBranch.isEmpty) None else Some(Branch(providerBranch)),
      publishVerificationResults =
        if (publishResults)
          Some(
            PublishVerificationResults(providerVer, ProviderTags(providerBranch))
          )
        else None,
      providerVerificationOptions = Seq(
        ProviderVerificationOption.SHOW_STACKTRACE
      ).toList,
      verificationTimeout = Some(30.seconds)
    )
  }

}
