import sbt._

object Dependencies {
  val akkaV = "2.6.20"
  val akkaHttpV = "10.2.10"
  val slickV = "3.4.1"

  val googleV = "2.0.0"

  def excludeGuavaJDK5(m: ModuleID): ModuleID = m.exclude("com.google.guava", "guava-jdk5")

  val slick: ModuleID =         "com.typesafe.slick" %% "slick"           % slickV
  val slickHikariCP: ModuleID = "com.typesafe.slick" %% "slick-hikaricp"  % slickV

  val excludeAkkaActor =        ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeAkkaStream =       ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream_2.12")

  val excludePostgresql =       ExclusionRule("org.postgresql", "postgresql")
  val excludeSnakeyaml =        ExclusionRule("org.yaml", "snakeyaml")

  val akkaActor: ModuleID =             "com.typesafe.akka" %% "akka-actor"               % akkaV
  val akkaActorTyped: ModuleID =        "com.typesafe.akka" %% "akka-actor-typed"         % akkaV
  val akkaStream: ModuleID =            "com.typesafe.akka" %% "akka-stream"              % akkaV
  val akkaContrib: ModuleID =           "com.typesafe.akka" %% "akka-contrib"             % akkaV
  val akkaSlf4j: ModuleID =             "com.typesafe.akka" %% "akka-slf4j"               % akkaV
  val akkaHttp: ModuleID =              "com.typesafe.akka" %% "akka-http"                % akkaHttpV           excludeAll(excludeAkkaActor, excludeAkkaStream)
  val akkaHttpSprayJson: ModuleID =     "com.typesafe.akka" %% "akka-http-spray-json"     % akkaHttpV
  val akkaActorTestKitTyped: ModuleID = "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaV     % "test"
  val akkaTestKit: ModuleID =           "com.typesafe.akka" %% "akka-testkit"             % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =       "com.typesafe.akka" %% "akka-http-testkit"        % akkaHttpV % "test"

  // This version of `cromwell-client` was packaged by `sbt` running on Scala 2.12 but actually contains only Java
  // classes (generated from OpenAPI YAML) that were not compiled against any version of the Scala library.
  // `cromwell-client` is therefore referenced here as a Java artifact with "_2.12" incorporated into its name,
  // allowing for Rawls to upgrade its Scala version without requiring any changes to this artifact.
  val cromwellClient: ModuleID =    "org.broadinstitute.cromwell" % "cromwell-client_2.12" % "0.1-8b413b45f-SNAP"

  val googleApiClient: ModuleID =             excludeGuavaJDK5("com.google.api-client"  % "google-api-client"                         % googleV)
  val googleCloudBilling: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudbilling"          % ("v1-rev20220908-" + googleV))
  val googleGenomics: ModuleID =              excludeGuavaJDK5("com.google.apis"        % "google-api-services-genomics"              % ("v2alpha1-rev20220913-" + googleV))
  val googleLifeSciences: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-lifesciences"          % ("v2beta-rev20220916-" + googleV))
  val googleStorage: ModuleID =               excludeGuavaJDK5("com.google.apis"        % "google-api-services-storage"               % ("v1-rev20220705-" + googleV))
  val googleCloudResourceManager: ModuleID =  excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudresourcemanager"  % ("v1-rev20220828-" + googleV))
  val googleIam: ModuleID =                   excludeGuavaJDK5("com.google.apis"        % "google-api-services-iam"                   % ("v1-rev20230105-" + googleV))
  val googleIamCredentials: ModuleID =        excludeGuavaJDK5("com.google.apis"        % "google-api-services-iamcredentials"        % ("v1-rev20211203-" + googleV))

  val googleCompute: ModuleID =           "com.google.apis"   % "google-api-services-compute"           % ("v1-rev20230119-" + googleV)
  val googlePubSub: ModuleID =            "com.google.apis"   % "google-api-services-pubsub"            % ("v1-rev20230112-" + googleV)
  val accessContextManager: ModuleID =    "com.google.apis"   % "google-api-services-accesscontextmanager" % ("v1-rev20230109-" + googleV)
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "33.2.1-jre"

  // metrics4-scala and metrics3-statsd are pulled in by workbench-metrics, which is pulled in by
  // workbench-google (workbenchGoogle variable in this file). Thus, anything that depends on workbench-google, such as
  // rawlsCoreDependencies, does not need these. As of this writing, metrics4-scala and metrics3-statsd are only
  // needed by the metrics subproject of Rawls.
  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics4-scala"    % "4.2.9"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.9.5"
  val jacksonCore: ModuleID =     "com.fasterxml.jackson.core"    % "jackson-core"          % "2.17.1"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.12.7"
  val jodaConvert: ModuleID =     "org.joda"                      % "joda-convert"          % "2.2.3"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.4.3"
  val sentryLogback: ModuleID =   "io.sentry"                     % "sentry-logback"        % "7.10.0"
  val webjarsLocator: ModuleID =  "org.webjars"                   % "webjars-locator"       % "0.52"
  val commonsJEXL: ModuleID =     "org.apache.commons"            % "commons-jexl"          % "2.1.1"
  val cats: ModuleID =            "org.typelevel"                 %% "cats-core"                 % "2.12.0"
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.5.6"
  val scalaUri: ModuleID =        "io.lemonlabs"                  %% "scala-uri"            % "3.0.0"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "3.2.18" % "test"
  val mockito: ModuleID =         "org.scalatestplus"             %% "mockito-4-2"          % "3.2.11.0" % Test
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "5.15.0" % "test"
  val breeze: ModuleID =          "org.scalanlp"                  %% "breeze"               % "1.2" % "test"
  val ficus: ModuleID =           "com.iheart"                    %% "ficus"                % "1.5.2"
  val apacheCommonsIO: ModuleID = "commons-io"                    % "commons-io"            % "2.16.1"
  val antlrParser: ModuleID =     "org.antlr"                     % "antlr4-runtime"        % "4.13.1"
  val mysqlConnector: ModuleID =  "com.mysql"                         % "mysql-connector-j"  % "8.4.0"
  // Update warning for liquibase-core: Here be dragons! See https://broadworkbench.atlassian.net/browse/WOR-1197
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "4.17.2" // scala-steward:off
  val jakartaWsRs: ModuleID =     "jakarta.ws.rs"                 % "jakarta.ws.rs-api"     % "4.0.0"
  val jerseyJnhConnector: ModuleID = "org.glassfish.jersey.connectors" % "jersey-jnh-connector" % "3.1.7"

  val workbenchLibsHash = "a6ad7dc"

  val workbenchModelV  = s"0.19-${workbenchLibsHash}"
  val workbenchGoogleV = s"0.32-${workbenchLibsHash}"
  val workbenchNotificationsV = s"0.6-${workbenchLibsHash}"
  val workbenchGoogle2V = s"0.36-${workbenchLibsHash}"
  val workbenchOauth2V = s"0.7-${workbenchLibsHash}"
  val workbenchOpenTelemetryV = s"0.8-$workbenchLibsHash"

  def excludeWorkbenchGoogle = ExclusionRule("org.broadinstitute.dsde.workbench", "workbench-google_2.13")

  val workbenchModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV
  val workbenchGoogle: ModuleID =       "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV
  val workbenchGoogleMocks: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV % "test" classifier "tests"
  // workbenchGoogle2 excludes slf4j because it pulls in too advanced a version
  val workbenchGoogle2: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V excludeAll(excludeSlf4j)
  val workbenchGoogle2Tests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests"
  val workbenchNotifications: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-notifications" % workbenchNotificationsV excludeAll(excludeWorkbenchGoogle)
  val workbenchOauth2: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V
  val workbenchOauth2Tests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V % "test" classifier "tests"

  val googleStorageLocal: ModuleID = "com.google.cloud" % "google-cloud-nio" % "0.127.18" % "test"

  val workbenchUtil: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-util" % s"0.10-${workbenchLibsHash}"

  val circeYAML: ModuleID = "io.circe" %% "circe-yaml" % "1.15.0"

  val azureIdentity: ModuleID = "com.azure" % "azure-identity" % "1.12.1"
  val azureCoreManagement: ModuleID = "com.azure" % "azure-core-management" % "1.15.0"


  def excludeOpenTelemetry = ExclusionRule("io.opentelemetry.instrumentation")
  def clientLibExclusions(m: ModuleID): ModuleID = m.excludeAll(excludeOpenTelemetry)

  def excludeSpringBoot = ExclusionRule("org.springframework.boot")
  def excludeSpringAop = ExclusionRule("org.springframework.spring-aop")
  def excludeSpringData = ExclusionRule("org.springframework.data")
  def excludeSpringFramework = ExclusionRule("org.springframework")
  def excludeOpenCensus = ExclusionRule("io.opencensus")
  def excludeGoogleFindBugs = ExclusionRule("com.google.code.findbugs")
  def excludeBroadWorkbench = ExclusionRule("org.broadinstitute.dsde.workbench")
  def excludeSlf4j = ExclusionRule("org.slf4j")
  // "Terra Common Lib" Exclusions:
  def tclExclusions(m: ModuleID): ModuleID = m.excludeAll(excludeSpringBoot, excludeSpringAop, excludeSpringData, excludeSpringFramework, excludeOpenCensus, excludeGoogleFindBugs, excludeBroadWorkbench, excludePostgresql, excludeSnakeyaml, excludeSlf4j)

  val workspaceManager = clientLibExclusions("bio.terra" % "workspace-manager-client" % "0.254.1116-SNAPSHOT")
  val dataRepo = clientLibExclusions("bio.terra" % "datarepo-jakarta-client" % "1.568.0-SNAPSHOT")
  val resourceBufferService = clientLibExclusions("bio.terra" % "terra-resource-buffer-client" % "0.198.42-SNAPSHOT")
  val billingProfileManager = clientLibExclusions("bio.terra" % "billing-profile-manager-client" % "0.1.549-SNAPSHOT")
  val terraCommonLib = tclExclusions(clientLibExclusions("bio.terra" % "terra-common-lib" % "0.1.23-SNAPSHOT" classifier "plain"))
  val sam: ModuleID = clientLibExclusions("org.broadinstitute.dsde.workbench" %% "sam-client" % "0.1-70fda75")
  val leonardo: ModuleID = "org.broadinstitute.dsde.workbench" % "leonardo-client_2.13" % "1.3.6-2e87300"

  // OpenTelemetry
  val openTelemetryInstrumentationVersion = "2.0.0"
  val otelInstrumentationResources: ModuleID =
    "io.opentelemetry.instrumentation" % "opentelemetry-resources" % (openTelemetryInstrumentationVersion + "-alpha")

  // Google cloud open telemetry exporters
  var gcpOpenTelemetryExporterVersion = "0.27.0"
  var googleTraceExporter: ModuleID = "com.google.cloud.opentelemetry" % "exporter-trace" % gcpOpenTelemetryExporterVersion

  val kindProjector = compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.3").cross(CrossVersion.full))
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

  val openApiParser: ModuleID = "io.swagger.parser.v3" % "swagger-parser-v3" % "2.1.22"

  // Overrides for transitive dependencies. These apply - via Settings.scala - to all projects in this codebase.
  // These are overrides only; if the direct dependencies stop including any of these, they will not be included
  // in Rawls by being listed here.
  // One reason to specify an override here is to avoid static-analysis security warnings.
  val transitiveDependencyOverrides = Seq(
    //Override for reactor-netty to address CVE-2023-34054 and CVE-2023-34062
    "io.projectreactor.netty"       % "reactor-netty-http"    % "1.0.39",
    // override commons-codec to address a non-CVE warning from DefectDojo
    "commons-codec"                 % "commons-codec"         % "1.16.1"
  )

  val extraOpenTelemetryDependencies = Seq(
    otelInstrumentationResources
  )

  val metricsDependencies = Seq(
    metricsScala,
    metricsStatsd,
    akkaHttp,
    akkaStream,
    scalatest,
    mockito
  )

  val googleDependencies = Seq(

    accessContextManager,

    akkaHttpSprayJson,
    akkaHttp,
    akkaStream,
    akkaActor,
    akkaHttpTestKit,
    scalatest,

    googleCloudBilling,
    googleGenomics,
    googleLifeSciences,
    googleStorage,
    googleCloudResourceManager,
    googleIam,
    googleIamCredentials,
    googleCompute,
    googlePubSub,
    googleGuava
  )

  val google2Dependencies = Seq(
    workbenchGoogle2,
    workbenchGoogle2Tests,
  )

  val utilDependencies = Seq(
    scalaLogging,
    akkaActor,
    cats,
    akkaHttpTestKit,
    scalatest,
    akkaTestKit,
    mockito
  )

  val modelDependencies = Seq(
    // I am not certain why I need jackson-core here but IntelliJ is confused without it and tests don't run
    workbenchModel,
    jacksonCore,
    akkaHttpSprayJson,
    akkaHttp,
    akkaStream,
    jodaTime,
    jodaConvert,
    scalaLogging,
    googleApiClient,
    scalaUri,
    workspaceManager,
    scalatest
  )

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ google2Dependencies ++ extraOpenTelemetryDependencies ++ Seq(
    typesafeConfig,
    sentryLogback,
    slick,
    slickHikariCP,
    akkaHttp,
    akkaStream,
    webjarsLocator,
    circeYAML,
    commonsJEXL,
    cromwellClient,
    cats,
    mysqlConnector,
    liquibaseCore,
    logbackClassic,
    akkaActorTyped,
    akkaActorTestKitTyped,
    akkaTestKit,
    akkaHttpTestKit,
    mockserverNetty,
    mockito,
    breeze,
    workbenchModel,
    workbenchNotifications,
    workbenchGoogle,
    googleStorageLocal,
    workbenchGoogleMocks,
    workbenchUtil,
    ficus,
    apacheCommonsIO,
    workspaceManager,
    dataRepo,
    antlrParser,
    resourceBufferService,
    billingProfileManager,
    kindProjector,
    betterMonadicFor,
    workbenchOauth2,
    workbenchOauth2Tests,
    terraCommonLib,
    sam,
    leonardo,
    jakartaWsRs,
    openApiParser,
    jerseyJnhConnector,
    azureIdentity,
    azureCoreManagement
  )

  val pact4sV = "0.10.0"
  val pact4sScalaTest = "io.github.jbwheatley" %% "pact4s-scalatest" % pact4sV % Test
  val pact4sCirce = "io.github.jbwheatley" %% "pact4s-circe" % pact4sV

  val pact4sDependencies = Seq(
    pact4sScalaTest,
    pact4sCirce
  )
}
