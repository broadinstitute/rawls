import sbt._

object Dependencies {
  val akkaV = "2.5.19"
  val akkaHttpV = "10.1.7"
  val slickV = "3.3.2"

  val googleV = "1.22.0"
  val olderGoogleV = "1.20.0"   // TODO why do we have two google versions?  GAWB-2149
  val workbenchGoogle2V = "0.11-8d09185"

  val cromwellVersion = "40-2754783"

  def excludeGuavaJDK5(m: ModuleID): ModuleID = m.exclude("com.google.guava", "guava-jdk5")

  val slick: ModuleID =         "com.typesafe.slick" %% "slick"           % slickV
  val slickHikariCP: ModuleID = "com.typesafe.slick" %% "slick-hikaricp"  % slickV

  val excludeAkkaActor =        ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeAkkaStream =       ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream_2.12")
  val excludeWorkbenchModel =   ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-model_2.12")
  val excludeWorkbenchUtil =    ExclusionRule(organization = "org.broadinstitute.dsde.workbench", name = "workbench-util_2.12")

  val excludeBouncyCastle =     ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-jdk15on")
  val excludeProtobufJavalite = ExclusionRule(organization = "com.google.protobuf", name = "protobuf-javalite")

  def workbenchGoogleExcludes(m: ModuleID): ModuleID = m.excludeAll(
    excludeWorkbenchModel, excludeWorkbenchUtil,
    excludeBouncyCastle,
    excludeProtobufJavalite)

  val akkaActor: ModuleID =         "com.typesafe.akka"   %%  "akka-actor"           % akkaV
  val akkaStream: ModuleID =        "com.typesafe.akka"   %%  "akka-stream"          % akkaV
  val akkaContrib: ModuleID =       "com.typesafe.akka"   %%  "akka-contrib"         % akkaV
  val akkaSlf4j: ModuleID =         "com.typesafe.akka"   %%  "akka-slf4j"           % akkaV
  val akkaHttp: ModuleID =          "com.typesafe.akka"   %%  "akka-http"            % akkaHttpV           excludeAll(excludeAkkaActor, excludeAkkaStream)
  val akkaHttpSprayJson: ModuleID = "com.typesafe.akka"   %%  "akka-http-spray-json" % akkaHttpV
  val akkaTestKit: ModuleID =       "com.typesafe.akka"   %%  "akka-testkit"         % akkaV     % "test"
  val akkaHttpTestKit: ModuleID =   "com.typesafe.akka"   %%  "akka-http-testkit"    % akkaHttpV % "test"

  val cromwellClient: ModuleID =    "org.broadinstitute.cromwell" %% "cromwell-client" % "0.1-8b413b45f-SNAP"

  val googleApiClient: ModuleID =             excludeGuavaJDK5("com.google.api-client"  % "google-api-client"                         % googleV)
  val googleCloudBilling: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudbilling"          % ("v1-rev7-" + googleV))
  val googleGenomics: ModuleID =              excludeGuavaJDK5("com.google.apis"        % "google-api-services-genomics"              % ("v2alpha1-rev61-" + googleV))
  val googleStorage: ModuleID =               excludeGuavaJDK5("com.google.apis"        % "google-api-services-storage"               % ("v1-rev157-" + "1.25.0"))
  val googleCloudResourceManager: ModuleID =  excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudresourcemanager"  % ("v1-rev7-" + googleV))
  val googleIam: ModuleID =                   excludeGuavaJDK5("com.google.apis"        % "google-api-services-iam"                   % ("v1-rev247-" + googleV))
  val googleIamCredentials: ModuleID =        excludeGuavaJDK5("com.google.apis"        % "google-api-services-iamcredentials"        % ("v1-rev38-" + googleV))

  val googleCompute: ModuleID =           "com.google.apis"   % "google-api-services-compute"           % ("v1-rev72-" + olderGoogleV)
  val googleAdminDirectory: ModuleID =    "com.google.apis"   % "google-api-services-admin-directory"   % ("directory_v1-rev53-" + olderGoogleV)
  val googlePlus: ModuleID =              "com.google.apis"   % "google-api-services-plus"              % ("v1-rev381-" + olderGoogleV)
  val googleOAuth2: ModuleID =            "com.google.apis"   % "google-api-services-oauth2"            % ("v1-rev112-" + olderGoogleV)
  val googlePubSub: ModuleID =            "com.google.apis"   % "google-api-services-pubsub"            % ("v1-rev14-" + googleV)
  val googleServicemanagement: ModuleID = "com.google.apis"   % "google-api-services-servicemanagement" % ("v1-rev17-" + googleV)
  val googleDeploymentManager: ModuleID = "com.google.apis"   % "google-api-services-deploymentmanager" % ("v2beta-rev20181207-1.28.0")
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "19.0"

  val googleRpc: ModuleID =               "io.grpc" % "grpc-core" % "1.30.0"
  val googleRpcNettyShaded: ModuleID =    "io.grpc" % "grpc-netty-shaded" % "1.30.0"
  val googleCloudCoreGrpc: ModuleID =     "com.google.cloud" % "google-cloud-core-grpc" % "1.93.6"

  val googleAutoValue: ModuleID =         "com.google.auto.value" % "auto-value-annotations" % "1.7.4"

  val googleOAuth2too: ModuleID = "com.google.auth" % "google-auth-library-oauth2-http" % "0.9.0"

  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics-scala"    % "3.5.6"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.7.1"
  val jacksonCore: ModuleID =     "com.fasterxml.jackson.core"    % "jackson-core"          % "2.8.10"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.9.4"
  val jodaConvert: ModuleID =     "org.joda"                      % "joda-convert"          % "1.8"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.3.0"
  val sentryLogback: ModuleID =   "io.sentry"                     % "sentry-logback"        % "1.7.30"
  val swaggerUI: ModuleID =       "org.webjars"                   % "swagger-ui"            % "3.25.0"
  val commonsJEXL: ModuleID =     "org.apache.commons"            % "commons-jexl"          % "2.1.1"
  val httpClient: ModuleID =      "org.apache.httpcomponents"     % "httpclient"            % "4.5.3"  // upgrading a transitive dependency to avoid security warnings
  val cats: ModuleID =            "org.typelevel"                 %% "cats"                 % "0.9.0"
  val parserCombinators =         "org.scala-lang.modules"        %% "scala-parser-combinators" % "1.1.1"
  val mysqlConnector: ModuleID =  "mysql"                         % "mysql-connector-java"  % "5.1.42"
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "3.5.3"
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.2.2"
  val scalaUri: ModuleID =        "io.lemonlabs"                  %% "scala-uri"            % "0.5.3"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "3.0.1" % "test"
  val mockito: ModuleID =         "org.mockito"                   % "mockito-core"          % "2.7.22" % "test"
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "3.9.2" % "test"
  val ficus: ModuleID =           "com.iheart"                    %% "ficus"                % "1.4.0"
  val scalaCache: ModuleID =      "com.github.cb372"              %% "scalacache-caffeine"  % "0.24.2"
  val apacheCommonsIO: ModuleID = "commons-io"                    % "commons-io"            % "2.6"
  val antlrParser: ModuleID =     "org.antlr"                     % "antlr4-runtime"        % "4.8-1"

  val workbenchModelV  = "0.13-58c913d"
  val workbenchModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV
  val workbenchGoogleV = "0.21-890a74f"
  val workbenchGoogle: ModuleID =       workbenchGoogleExcludes("org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV)
  val workbenchGoogleMocks: ModuleID =  workbenchGoogleExcludes("org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV % "test" classifier "tests")
  val workbenchGoogle2: ModuleID =      workbenchGoogleExcludes("org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V)
  val workbenchGoogle2Tests: ModuleID = workbenchGoogleExcludes("org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests")
  val googleStorageLocal: ModuleID = "com.google.cloud" % "google-cloud-nio" % "0.111.0-alpha" % "test"

  val workbenchUtil: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-util" % "0.5-d4b4838" excludeAll(excludeWorkbenchModel)
  val log4cats = "io.chrisdavenport" %% "log4cats-slf4j"   % "0.3.0"

  val circeYAML: ModuleID = "io.circe" %% "circe-yaml" % "0.9.0"
  val circeFS2: ModuleID = "io.circe" %% "circe-fs2" % "0.13.0"

  val accessContextManager = "com.google.apis" % "google-api-services-accesscontextmanager" % "v1beta-rev55-1.25.0"

  val workspaceManager = excludeGuavaJDK5("bio.terra" % "workspace-manager-client" % "0.2.0-SNAPSHOT")
  val dataRepo = excludeGuavaJDK5("bio.terra" % "datarepo-client" % "1.0.44-SNAPSHOT")

  val opencensusScalaCode: ModuleID = "com.github.sebruck" %% "opencensus-scala-core" % "0.7.0-M2"
  val opencensusAkkaHttp: ModuleID = "com.github.sebruck" %% "opencensus-scala-akka-http" % "0.7.0-M2"
  val opencensusStackDriverExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-stackdriver" % "0.23.0" excludeAll(excludeProtobufJavalite)
  val opencensusLoggingExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-logging"     % "0.23.0"

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

  val googleDependencies = metricsDependencies ++ Seq(

    accessContextManager,

    akkaHttpSprayJson,
    akkaHttp,
    akkaStream,
    akkaActor,
    akkaHttpTestKit,
    scalatest,

    googleCloudBilling,
    googleGenomics,
    googleStorage,
    googleCloudResourceManager,
    googleIam,
    googleIamCredentials,
    googleCompute,
    googleAdminDirectory,
    googlePlus,
    googleOAuth2,
    googleOAuth2too,
    googlePubSub,
    googleServicemanagement,
    googleDeploymentManager,
    googleGuava
  )

  // google2 lib requires specific versions of rpc libs:
  val google2Dependencies = Seq(
    workbenchGoogle2,
    googleCloudCoreGrpc,
    googleRpc,
    googleRpcNettyShaded,
    workbenchGoogle2Tests,
    googleAutoValue // workbench-libs/google2 fails to correctly pull this as a dependency, so we do it manually
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
    httpClient,
    googleApiClient,
    scalaUri,
    workspaceManager,
    scalatest
  )

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ google2Dependencies ++ metricsDependencies ++ openCensusDependencies ++ Seq(
    typesafeConfig,
    parserCombinators,
    sentryLogback,
    slick,
    slickHikariCP,
    akkaHttp,
    akkaStream,
    swaggerUI,
    circeYAML,
    circeFS2,
    commonsJEXL,
    cromwellClient,
    cats,
    mysqlConnector,
    liquibaseCore,
    logbackClassic,
    akkaTestKit,
    akkaHttpTestKit,
    mockserverNetty,
    mockito,
    log4cats,
    workbenchModel,
    workbenchGoogle,
    googleStorageLocal,
    workbenchGoogleMocks,
    workbenchUtil,
    ficus,
    scalaCache,
    apacheCommonsIO,
    workspaceManager,
    dataRepo,
    antlrParser
  )
}
