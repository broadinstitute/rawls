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

  val excludeBouncyCastle =     ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-jdk15on")
  val excludeProtobufJavalite = ExclusionRule(organization = "com.google.protobuf", name = "protobuf-javalite")

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
  val googleDeploymentManager: ModuleID = "com.google.apis"   % "google-api-services-deploymentmanager" % ("v2-rev20220908-" + googleV)
  val accessContextManager: ModuleID =    "com.google.apis"   % "google-api-services-accesscontextmanager" % ("v1-rev20230109-" + googleV)
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "31.1-jre"

  // metrics4-scala and metrics3-statsd are pulled in by workbench-metrics, which is pulled in by
  // workbench-google (workbenchGoogle variable in this file). Thus, anything that depends on workbench-google, such as
  // rawlsCoreDependencies, does not need these. As of this writing, metrics4-scala and metrics3-statsd are only
  // needed by the metrics subproject of Rawls.
  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics4-scala"    % "4.2.9"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.9.5"
  val jacksonCore: ModuleID =     "com.fasterxml.jackson.core"    % "jackson-core"          % "2.14.2"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.12.5"
  val jodaConvert: ModuleID =     "org.joda"                      % "joda-convert"          % "2.2.3"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.4.2"
  val sentryLogback: ModuleID =   "io.sentry"                     % "sentry-logback"        % "6.17.0"
  val webjarsLocator: ModuleID =  "org.webjars"                   % "webjars-locator"       % "0.46"
  val commonsJEXL: ModuleID =     "org.apache.commons"            % "commons-jexl"          % "2.1.1"
  val cats: ModuleID =            "org.typelevel"                 %% "cats-core"                 % "2.9.0"
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.4.6"
  val scalaUri: ModuleID =        "io.lemonlabs"                  %% "scala-uri"            % "3.0.0"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "3.2.15" % "test"
  val mockito: ModuleID =         "org.scalatestplus"             %% "mockito-4-2"          % "3.2.11.0" % Test
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "5.15.0" % "test"
  val breeze: ModuleID =          "org.scalanlp"                  %% "breeze"               % "1.2" % "test"
  val ficus: ModuleID =           "com.iheart"                    %% "ficus"                % "1.5.2"
  val apacheCommonsIO: ModuleID = "commons-io"                    % "commons-io"            % "2.11.0"
  val antlrParser: ModuleID =     "org.antlr"                     % "antlr4-runtime"        % "4.8-1"
  val mysqlConnector: ModuleID =  "mysql"                         % "mysql-connector-java"  % "8.0.30"
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "4.17.2"

  val workbenchLibsHash = "e20067a"

  val workbenchModelV  = s"0.16-${workbenchLibsHash}"
  val workbenchGoogleV = s"0.25-${workbenchLibsHash}"
  val workbenchNotificationsV = s"0.3-${workbenchLibsHash}"
  val workbenchGoogle2V = s"0.25-${workbenchLibsHash}"
  val workbenchOauth2V = s"0.2-${workbenchLibsHash}"
  val workbenchOpenTelemetryV = s"0.3-$workbenchLibsHash"

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
  val workbenchOpenTelemetry: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV
  val workbenchOpenTelemetryTests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-opentelemetry" % workbenchOpenTelemetryV classifier "tests"

  val googleStorageLocal: ModuleID = "com.google.cloud" % "google-cloud-nio" % "0.126.13" % "test"

  val workbenchUtil: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-util" % s"0.6-${workbenchLibsHash}"

  val circeYAML: ModuleID = "io.circe" %% "circe-yaml" % "0.14.2"

  // should we prefer jakarta over javax.xml?
  def excludeJakartaActivationApi = ExclusionRule("jakarta.activation", "jakarta.activation-api")
  def excludeJakartaXmlBindApi = ExclusionRule("jakarta.xml.bind", "jakarta.xml.bind-api")
  def excludeJakarta(m: ModuleID): ModuleID = m.excludeAll(excludeJakartaActivationApi, excludeJakartaXmlBindApi)

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

  val workspaceManager = excludeJakarta("bio.terra" % "workspace-manager-client" % "0.254.676-SNAPSHOT")
  val dataRepo = excludeJakarta("bio.terra" % "datarepo-client" % "1.379.0-SNAPSHOT")
  val resourceBufferService = excludeJakarta("bio.terra" % "terra-resource-buffer-client" % "0.4.3-SNAPSHOT")
  val billingProfileManager = excludeJakarta("bio.terra" % "billing-profile-manager-client" % "0.1.128-SNAPSHOT")
  val terraCommonLib = tclExclusions(excludeJakarta("bio.terra" % "terra-common-lib" % "0.0.63-SNAPSHOT" classifier "plain"))
  val sam: ModuleID = excludeJakarta("org.broadinstitute.dsde.workbench" %% "sam-client" % "0.1-f554115")
  val leonardo: ModuleID = "org.broadinstitute.dsde.workbench" % "leonardo-client_2.11" % "1.3.6-801b78f"

  val opencensusScalaCode: ModuleID = "com.github.sebruck" %% "opencensus-scala-core" % "0.7.2"
  val opencensusAkkaHttp: ModuleID = "com.github.sebruck" %% "opencensus-scala-akka-http" % "0.7.2"
  val opencensusStackDriverExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-stackdriver" % "0.31.1" excludeAll(excludeProtobufJavalite)
  val opencensusLoggingExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-logging"     % "0.31.1"

  val kindProjector = compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.2").cross(CrossVersion.full))
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

  // Overrides for transitive dependencies. These apply - via Settings.scala - to all projects in this codebase.
  // These are overrides only; if the direct dependencies stop including any of these, they will not be included
  // in Rawls by being listed here.
  // One reason to specify an override here is to avoid static-analysis security warnings.
  val transitiveDependencyOverrides = Seq(
    "commons-codec"                 % "commons-codec"         % "1.15",
    "org.glassfish.jersey.core"     % "jersey-client"         % "2.36" // scala-steward:off (must match TDR)
  )

  val openCensusDependencies = Seq(
    opencensusScalaCode,
    opencensusAkkaHttp,
    opencensusStackDriverExporter,
    opencensusLoggingExporter
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
    googleDeploymentManager,
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

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ google2Dependencies ++ openCensusDependencies ++ Seq(
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
    workbenchOpenTelemetry,
    workbenchOpenTelemetryTests,
    terraCommonLib,
    sam,
    leonardo
  )
}
