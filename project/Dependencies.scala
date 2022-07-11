import sbt._

object Dependencies {
  val akkaV = "2.6.19"
  val akkaHttpV = "10.2.9"
  val slickV = "3.3.3"

  val googleV = "1.31.0"
  val olderGoogleV = "1.20.0"   // TODO why do we have two google versions?  GAWB-2149

  def excludeGuavaJDK5(m: ModuleID): ModuleID = m.exclude("com.google.guava", "guava-jdk5")

  val slick: ModuleID =         "com.typesafe.slick" %% "slick"           % slickV
  val slickHikariCP: ModuleID = "com.typesafe.slick" %% "slick-hikaricp"  % slickV

  val excludeAkkaActor =        ExclusionRule(organization = "com.typesafe.akka", name = "akka-actor_2.12")
  val excludeAkkaStream =       ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream_2.12")

  val excludeBouncyCastle =     ExclusionRule(organization = "org.bouncycastle", name = s"bcprov-jdk15on")
  val excludeProtobufJavalite = ExclusionRule(organization = "com.google.protobuf", name = "protobuf-javalite")

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
  val googleCloudBilling: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudbilling"          % ("v1-rev20210322-" + googleV))
  val googleGenomics: ModuleID =              excludeGuavaJDK5("com.google.apis"        % "google-api-services-genomics"              % ("v2alpha1-rev20210605-" + googleV))
  val googleLifeSciences: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-lifesciences"          % ("v2beta-rev20210527-" + googleV))
  val googleStorage: ModuleID =               excludeGuavaJDK5("com.google.apis"        % "google-api-services-storage"               % ("v1-rev20210127-" + googleV))
  val googleCloudResourceManager: ModuleID =  excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudresourcemanager"  % ("v1-rev20210613-" + googleV))
  val googleIam: ModuleID =                   excludeGuavaJDK5("com.google.apis"        % "google-api-services-iam"                   % ("v1-rev20210211-" + googleV))
  val googleIamCredentials: ModuleID =        excludeGuavaJDK5("com.google.apis"        % "google-api-services-iamcredentials"        % ("v1-rev20210326-" + googleV))

  val googleCompute: ModuleID =           "com.google.apis"   % "google-api-services-compute"           % ("v1-rev72-" + olderGoogleV)
  val googleAdminDirectory: ModuleID =    "com.google.apis"   % "google-api-services-admin-directory"   % ("directory_v1-rev53-" + olderGoogleV)
  val googlePlus: ModuleID =              "com.google.apis"   % "google-api-services-plus"              % ("v1-rev381-" + olderGoogleV)
  val googleOAuth2: ModuleID =            "com.google.apis"   % "google-api-services-oauth2"            % ("v1-rev112-" + olderGoogleV)
  val googlePubSub: ModuleID =            "com.google.apis"   % "google-api-services-pubsub"            % ("v1-rev20210322-" + googleV)
  val googleServicemanagement: ModuleID = "com.google.apis"   % "google-api-services-servicemanagement" % ("v1-rev20210604-" + googleV)
  val googleDeploymentManager: ModuleID = "com.google.apis"   % "google-api-services-deploymentmanager" % ("v2beta-rev20210311-" + googleV)
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "31.1-jre"

  val googleRpc: ModuleID =               "io.grpc" % "grpc-core" % "1.33.1"
  val googleRpcNettyShaded: ModuleID =    "io.grpc" % "grpc-netty-shaded" % "1.33.1"
  val googleCloudCoreGrpc: ModuleID =     "com.google.cloud" % "google-cloud-core-grpc" % "1.96.1"

  val googleAutoValue: ModuleID =         "com.google.auto.value" % "auto-value-annotations" % "1.7.4"

  val googleOAuth2too: ModuleID = "com.google.auth" % "google-auth-library-oauth2-http" % "0.9.1"

  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics4-scala"    % "4.1.19"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.9.5"
  val jacksonCore: ModuleID =     "com.fasterxml.jackson.core"    % "jackson-core"          % "2.13.3"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.10.14"
  val jodaConvert: ModuleID =     "org.joda"                      % "joda-convert"          % "1.9.2"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.4.2"
  val sentryLogback: ModuleID =   "io.sentry"                     % "sentry-logback"        % "1.7.30"
  val webjarsLocator: ModuleID =  "org.webjars"                   % "webjars-locator"       % "0.40"
  val commonsJEXL: ModuleID =     "org.apache.commons"            % "commons-jexl"          % "2.1.1"
  val commonsCodec: ModuleID =    "commons-codec"                 % "commons-codec"         % "1.15"   // upgrading a transitive dependency to avoid security warnings
  val httpClient: ModuleID =      "org.apache.httpcomponents"     % "httpclient"            % "4.5.13" // upgrading a transitive dependency to avoid security warnings
  val jerseyClient: ModuleID =    "org.glassfish.jersey.core"     % "jersey-client"         % "2.36"   // upgrading a transitive dependency to avoid security warnings
  val cats: ModuleID =            "org.typelevel"                 %% "cats-core"                 % "2.6.1"
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.2.11"
  val scalaUri: ModuleID =        "io.lemonlabs"                  %% "scala-uri"            % "3.0.0"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "3.2.12" % "test"
  val mockito: ModuleID =         "org.scalatestplus"             %% "mockito-4-2"          % "3.2.11.0" % Test
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "5.11.2" % "test"
  val breeze: ModuleID =          "org.scalanlp"                  %% "breeze"               % "1.2" % "test"
  val ficus: ModuleID =           "com.iheart"                    %% "ficus"                % "1.5.2"
  val apacheCommonsIO: ModuleID = "commons-io"                    % "commons-io"            % "2.11.0"
  val antlrParser: ModuleID =     "org.antlr"                     % "antlr4-runtime"        % "4.8-1"

  /* mysql-connector-java > 8.0.22 is incompatible with liquibase-core < 4.3.1. See:
      - https://github.com/liquibase/liquibase/issues/1639
      - https://dev.mysql.com/doc/relnotes/connector-j/8.0/en/news-8-0-23.html
     the end result of this incompatibility is that attempting to run Rawls' liquibase on an already-initialized database
     will throw an error "java.lang.ClassCastException: class java.time.LocalDateTime cannot be cast to class java.lang.String".
     This only occurs on already-initialized databases; it does not happen when liquibase is run the first time on an
     empty DB.

     The behavior change in mysql-connector-java between 8.0.22 and 8.0.23 needs to be assessed to see if it will cause
     any issues elsewhere in Rawls before upgrading.
   */
  val mysqlConnector: ModuleID =  "mysql"                         % "mysql-connector-java"  % "8.0.22"
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "3.10.3"

  val workbenchLibsHash = "20f9225"

  val workbenchModelV  = s"0.15-${workbenchLibsHash}"
  val workbenchGoogleV = s"0.21-${workbenchLibsHash}"
  val workbenchGoogle2V = s"0.24-${workbenchLibsHash}"
  val workbenchOauth2V = s"0.2-${workbenchLibsHash}"

  val workbenchModel: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-model"  % workbenchModelV
  val workbenchGoogle: ModuleID =       "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV
  val workbenchGoogleMocks: ModuleID =  "org.broadinstitute.dsde.workbench" %% "workbench-google" % workbenchGoogleV % "test" classifier "tests"
  val workbenchGoogle2: ModuleID =      "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V
  val workbenchGoogle2Tests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests"
  val workbenchOauth2: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V
  val workbenchOauth2Tests: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-oauth2" % workbenchOauth2V % "test" classifier "tests"
  val googleStorageLocal: ModuleID = "com.google.cloud" % "google-cloud-nio" % "0.124.7" % "test"

  val workbenchUtil: ModuleID = "org.broadinstitute.dsde.workbench" %% "workbench-util" % s"0.6-${workbenchLibsHash}"

  val circeYAML: ModuleID = "io.circe" %% "circe-yaml" % "0.14.1"

  val accessContextManager = "com.google.apis" % "google-api-services-accesscontextmanager" % "v1-rev20210319-1.31.0"

  // should we prefer jakarta over javax.xml?
  def excludeJakartaActivationApi = ExclusionRule("jakarta.activation", "jakarta.activation-api")
  def excludeJakartaXmlBindApi = ExclusionRule("jakarta.xml.bind", "jakarta.xml.bind-api")
  def excludeJakarta(m: ModuleID): ModuleID = m.excludeAll(excludeJakartaActivationApi, excludeJakartaXmlBindApi)

  val workspaceManager = excludeJakarta("bio.terra" % "workspace-manager-client" % "0.254.296-SNAPSHOT")
  val dataRepo = excludeJakarta("bio.terra" % "datarepo-client" % "1.41.0-SNAPSHOT")
  val dataRepoJersey = "org.glassfish.jersey.inject" % "jersey-hk2" % "2.32" // scala-steward:off (must match TDR)
  val resourceBufferService = excludeJakarta("bio.terra" % "terra-resource-buffer-client" % "0.4.3-SNAPSHOT")

  val opencensusScalaCode: ModuleID = "com.github.sebruck" %% "opencensus-scala-core" % "0.7.2"
  val opencensusAkkaHttp: ModuleID = "com.github.sebruck" %% "opencensus-scala-akka-http" % "0.7.2"
  val opencensusStackDriverExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-stackdriver" % "0.31.1" excludeAll(excludeProtobufJavalite)
  val opencensusLoggingExporter: ModuleID = "io.opencensus" % "opencensus-exporter-trace-logging"     % "0.31.1"

  val kindProjector = compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.2").cross(CrossVersion.full))
  val betterMonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

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
    googleLifeSciences,
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
    commonsCodec,
    httpClient,
    googleApiClient,
    scalaUri,
    workspaceManager,
    jerseyClient,
    scalatest
  )

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ google2Dependencies ++ metricsDependencies ++ openCensusDependencies ++ Seq(
    typesafeConfig,
    sentryLogback,
    slick,
    slickHikariCP,
    akkaHttp,
    akkaStream,
    webjarsLocator,
    circeYAML,
    commonsCodec,
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
    workbenchGoogle,
    googleStorageLocal,
    workbenchGoogleMocks,
    workbenchUtil,
    ficus,
    apacheCommonsIO,
    workspaceManager,
    jerseyClient,
    dataRepo,
    dataRepoJersey,
    antlrParser,
    resourceBufferService,
    kindProjector,
    betterMonadicFor,
    workbenchOauth2,
    workbenchOauth2Tests
  )
}
