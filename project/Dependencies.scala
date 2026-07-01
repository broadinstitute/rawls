import sbt._

object Dependencies {
  val akkaV = "2.6.20"
  val akkaHttpV = "10.2.10"
  val slickV = "3.6.1"

  val googleV = "2.0.0" // service-specific client libraries
  val googleApiV = "2.8.1" // the main google-api-client

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

  val bardClient: ModuleID = "bio.terra" % "bard-client-resttemplate" % "1.0.10" exclude("org.springframework", "spring-aop") exclude("org.springframework", "spring-jcl")
  val httpComponents5: ModuleID = "org.apache.httpcomponents.client5" % "httpclient5" % "5.5.1" // Needed for connection pooling with the Bard client

  val googleApiClient: ModuleID =             excludeGuavaJDK5("com.google.api-client"  % "google-api-client"                         % googleApiV)
  val googleCloudBilling: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudbilling"          % ("v1-rev20241011-" + googleV))
  val googleLifeSciences: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-lifesciences"          % ("v2beta-rev20240329-" + googleV))
  val googleStorage: ModuleID =               excludeGuavaJDK5("com.google.apis"        % "google-api-services-storage"               % ("v1-rev20241206-" + googleV))
  val googleCloudResourceManager: ModuleID =  excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudresourcemanager"  % ("v1-rev20240310-" + googleV)) // has v2 and v3 versions 2.0.0, v2-rev20240310-2.0.0
  val googleIam: ModuleID =                   excludeGuavaJDK5("com.google.apis"        % "google-api-services-iam"                   % ("v1-rev20250116-" + googleV)) // has a v2 version
  val googleIamCredentials: ModuleID =        excludeGuavaJDK5("com.google.apis"        % "google-api-services-iamcredentials"        % ("v1-rev20241024-" + googleV))

  val googleCompute: ModuleID =           "com.google.apis"   % "google-api-services-compute"           % ("v1-rev20250211-" + googleV)
  val googlePubSub: ModuleID =            "com.google.apis"   % "google-api-services-pubsub"            % ("v1-rev20250208-" + googleV)
  val accessContextManager: ModuleID =    "com.google.apis"   % "google-api-services-accesscontextmanager" % ("v1-rev20250212-" + googleV)
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "33.5.0-jre"

  val googleMonitoring: ModuleID =  "com.google.apis" % "google-api-services-monitoring" % ("v3-rev20250130-" + googleV)

  // metrics4-scala and metrics3-statsd are pulled in by workbench-metrics, which is pulled in by
  // workbench-google (workbenchGoogle variable in this file). Thus, anything that depends on workbench-google, such as
  // rawlsCoreDependencies, does not need these. As of this writing, metrics4-scala and metrics3-statsd are only
  // needed by the metrics subproject of Rawls.
  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics4-scala"    % "4.3.7"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.9.6"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.14.0"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.4.5"
  val sentryLogback: ModuleID =   "io.sentry"                     % "sentry-logback"        % "8.27.1"
  val webjarsLocator: ModuleID =  "org.webjars"                   % "webjars-locator"       % "0.52"
  val cats: ModuleID =            "org.typelevel"                 %% "cats-core"                 % "2.13.0"
  val fs2Reactive: ModuleID =     "co.fs2"                        %% "fs2-reactive-streams" % "3.6.1" // 3.7.0 has possibly-breaking changes
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.5.21"
  val logstashLogback: ModuleID = "net.logstash.logback"          % "logstash-logback-encoder" % "9.0"
  val scalaUri: ModuleID =        "com.indoorvivants"                  %% "scala-uri"            % "4.2.0"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "3.2.19" % "test"
  val mockito: ModuleID =         "org.scalatestplus"             %% "mockito-4-2"          % "3.2.11.0" % Test
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "7.3.0" % "test"
  val breeze: ModuleID =          "org.scalanlp"                  %% "breeze"               % "1.2" % "test"
  val apacheCommonsIO: ModuleID = "commons-io"                    % "commons-io"            % "2.21.0"
  val antlrParser: ModuleID =     "org.antlr"                     % "antlr4-runtime"        % "4.13.2"
  // protobuf is only need to use the MySQL X DevAPI which we don't. exclude it to avoid interference with Google client libraries
  val mysqlConnector: ModuleID =  "com.mysql"                         % "mysql-connector-j"  % "9.5.0" exclude("com.google.protobuf", "protobuf-java")
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "4.33.0"
  val jakartaWsRs: ModuleID =     "jakarta.ws.rs"                 % "jakarta.ws.rs-api"     % "4.0.0"
  val jerseyJnhConnector: ModuleID = "org.glassfish.jersey.connectors" % "jersey-jnh-connector" % "3.1.11"
  val janino: ModuleID = "org.codehaus.janino" % "janino" % "3.1.12" // For if-else logic in logging config

  val workbenchLibsHash = "80e4b8d"

  val workbenchModelV  = s"0.20-${workbenchLibsHash}"
  val workbenchGoogleV = s"0.33-${workbenchLibsHash}"
  val workbenchNotificationsV = s"0.8-${workbenchLibsHash}"
  val workbenchGoogle2V = s"0.36-${workbenchLibsHash}"
  val workbenchOauth2V = s"0.8-${workbenchLibsHash}"
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

  val googleStorageLocal: ModuleID = "com.google.cloud" % "google-cloud-nio" % "0.128.8" % "test"

  val workbenchUtil: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-util" % s"0.10-${workbenchLibsHash}"

  val circeYAML: ModuleID = "io.circe" %% "circe-yaml" % "1.15.0"

  val azureIdentity: ModuleID = "com.azure" % "azure-identity" % "1.18.1"
  val azureCoreManagement: ModuleID = "com.azure" % "azure-core-management" % "1.19.2"

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

  val dataRepo = clientLibExclusions("bio.terra" % "datarepo-client" % "2.382.0-SNAPSHOT")
  val resourceBufferService = clientLibExclusions("bio.terra" % "terra-resource-buffer-client" % "0.198.153-SNAPSHOT")
  val terraCommonLib = tclExclusions(clientLibExclusions("bio.terra" % "terra-common-lib" % "1.1.70-SNAPSHOT" classifier "plain"))
  val sam: ModuleID = clientLibExclusions("org.broadinstitute.dsde.workbench" %% "sam-client" % "v0.0.440")
  val leonardo: ModuleID = "org.broadinstitute.dsde.workbench" % "leonardo-client_2.13" % "1.3.6-82c1f7d"
  val policyService = clientLibExclusions("bio.terra" % "terra-policy-client" % "1.0.41-SNAPSHOT")

  // OpenTelemetry
  val openTelemetryInstrumentationVersion = "2.20.0"
  val otelInstrumentationResources: ModuleID =
    "io.opentelemetry.instrumentation" % "opentelemetry-resources" % (openTelemetryInstrumentationVersion + "-alpha")

  // Google cloud open telemetry exporters
  var gcpOpenTelemetryExporterVersion = "0.27.0"
  var googleTraceExporter: ModuleID = "com.google.cloud.opentelemetry" % "exporter-trace" % gcpOpenTelemetryExporterVersion

  val kindProjector = compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.4").cross(CrossVersion.full))
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

  val openApiParser: ModuleID = "io.swagger.parser.v3" % "swagger-parser-v3" % "2.1.36"

  // Overrides for transitive dependencies. These apply - via Settings.scala - to all projects in this codebase.
  // These are overrides only; if the direct dependencies stop including any of these, they will not be included
  // in Rawls by being listed here.
  // One reason to specify an override here is to avoid static-analysis security warnings.
  val transitiveDependencyOverrides = Seq(
    // override commons-codec to address a non-CVE warning from DefectDojo
    "commons-codec"                 % "commons-codec"         % "1.20.0",
    // override cats-parse to address conflicting dependency versions for scala-uri
    "org.typelevel" %% "cats-parse" % "1.1.0",
    // override tools.jackson.core pulled in by logstash-logback-encoder 9.0
    "tools.jackson.core" % "jackson-core"     % "3.1.0",
    "tools.jackson.core" % "jackson-databind" % "3.1.0",
    // override bouncycastle to address CVE-2026-5598 (requires >= 1.84)
    "org.bouncycastle" % "bcprov-jdk18on" % "1.84",
    "org.bouncycastle" % "bcpkix-jdk18on" % "1.84",
    "org.bouncycastle" % "bcutil-jdk18on" % "1.84",
    // override netty-codec* to address CVE-2026-42587 (requires >= 4.1.133.Final)
    "io.netty" % "netty-codec"       % "4.1.133.Final",
    "io.netty" % "netty-codec-dns"   % "4.1.133.Final",
    "io.netty" % "netty-codec-http"  % "4.1.133.Final",
    "io.netty" % "netty-codec-http2" % "4.1.133.Final",
    "io.netty" % "netty-codec-socks" % "4.1.133.Final"
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
    googleLifeSciences,
    googleStorage,
    googleCloudResourceManager,
    googleIam,
    googleIamCredentials,
    googleCompute,
    googlePubSub,
    googleGuava,
    googleMonitoring
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
    workbenchModel,
    akkaHttpSprayJson,
    akkaHttp,
    akkaStream,
    jodaTime,
    scalaLogging,
    googleApiClient,
    scalaUri,
    scalatest
  )

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ google2Dependencies ++ extraOpenTelemetryDependencies ++ Seq(
    typesafeConfig,
    sentryLogback,
    bardClient,
    httpComponents5,
    slick,
    slickHikariCP,
    akkaHttp,
    akkaStream,
    webjarsLocator,
    circeYAML,
    cromwellClient,
    cats,
    fs2Reactive,
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
    apacheCommonsIO,
    dataRepo,
    antlrParser,
    resourceBufferService,
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
    azureCoreManagement,
    policyService,
    logstashLogback,
    janino
  )

  val pact4sV = "0.10.0"
  val pact4sScalaTest = "io.github.jbwheatley" %% "pact4s-scalatest" % pact4sV % Test
  val pact4sCirce = "io.github.jbwheatley" %% "pact4s-circe" % pact4sV

  val pact4sDependencies = Seq(
    pact4sScalaTest,
    pact4sCirce
  )
}
