import sbt._

object Dependencies {
  val akkaV = "2.3.6"
  val sprayV = "1.3.3"
  val slickV = "3.1.1"

  val googleV = "1.22.0"
  val olderGoogleV = "1.20.0"   // TODO why do we have two google versions?  GAWB-2149

  val wdl4sV = "0.13"

  def excludeGuavaJDK5(m: ModuleID): ModuleID = m.exclude("com.google.guava", "guava-jdk5")

  val sprayJson: ModuleID =     "io.spray" %% "spray-json"    % sprayV
  val sprayHttp: ModuleID =     "io.spray" %% "spray-http"    % sprayV
  val sprayHttpx: ModuleID =    "io.spray" %% "spray-httpx"   % sprayV
  val sprayCan: ModuleID =      "io.spray" %% "spray-can"     % sprayV
  val sprayRouting: ModuleID =  "io.spray" %% "spray-routing" % sprayV
  val sprayClient: ModuleID =   "io.spray" %% "spray-client"  % sprayV
  val sprayTestkit: ModuleID =  "io.spray" %% "spray-testkit" % sprayV % "test"

  val slick: ModuleID =         "com.typesafe.slick" %% "slick"           % slickV
  val slickHikariCP: ModuleID = "com.typesafe.slick" %% "slick-hikaricp"  % slickV

  val akkaActor: ModuleID =   "com.typesafe.akka" %% "akka-actor"   % akkaV
  val akkaTestkit: ModuleID = "com.typesafe.akka" %% "akka-testkit" % akkaV % "test"

  val googleApiClient: ModuleID =             excludeGuavaJDK5("com.google.api-client"  % "google-api-client"                         % googleV)
  val googleCloudBilling: ModuleID =          excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudbilling"          % ("v1-rev7-" + googleV))
  val googleGenomics: ModuleID =              excludeGuavaJDK5("com.google.apis"        % "google-api-services-genomics"              % ("v1-rev89-" + googleV))
  val googleStorage: ModuleID =               excludeGuavaJDK5("com.google.apis"        % "google-api-services-storage"               % ("v1-rev35-" + olderGoogleV))
  val googleCloudResourceManager: ModuleID =  excludeGuavaJDK5("com.google.apis"        % "google-api-services-cloudresourcemanager"  % ("v1-rev7-" + googleV))

  val googleCompute: ModuleID =           "com.google.apis"   % "google-api-services-compute"           % ("v1-rev72-" + olderGoogleV)
  val googleAdminDirectory: ModuleID =    "com.google.apis"   % "google-api-services-admin-directory"   % ("directory_v1-rev53-" + olderGoogleV)
  val googlePlus: ModuleID =              "com.google.apis"   % "google-api-services-plus"              % ("v1-rev381-" + olderGoogleV)
  val googleOAuth2: ModuleID =            "com.google.apis"   % "google-api-services-oauth2"            % ("v1-rev112-" + olderGoogleV)
  val googlePubSub: ModuleID =            "com.google.apis"   % "google-api-services-pubsub"            % ("v1-rev14-" + googleV)
  val googleServicemanagement: ModuleID = "com.google.apis"   % "google-api-services-servicemanagement" % ("v1-rev17-" + googleV)
  val googleGuava: ModuleID =             "com.google.guava"  % "guava" % "19.0"

  // metrics-scala transitively pulls in io.dropwizard.metrics:metrics-core
  val metricsScala: ModuleID =       "nl.grons"              %% "metrics-scala"    % "3.5.6"
  val metricsStatsd: ModuleID =      "com.readytalk"         %  "metrics3-statsd"  % "4.2.0"

  val scalaLogging: ModuleID =    "com.typesafe.scala-logging"    %% "scala-logging"        % "3.1.0"
  val jacksonCore: ModuleID =     "com.fasterxml.jackson.core"    % "jackson-core"          % "2.4.3"
  val jodaTime: ModuleID =        "joda-time"                     % "joda-time"             % "2.9.4"
  val jodaConvert: ModuleID =     "org.joda"                      % "joda-convert"          % "1.8"
  val typesafeConfig: ModuleID =  "com.typesafe"                  % "config"                % "1.3.0"
  val ravenLogback: ModuleID =    "com.getsentry.raven"           % "raven-logback"         % "7.8.6"
  val swaggerUI: ModuleID =       "org.webjars"                   % "swagger-ui"            % "2.1.1"
  val commonsJEXL: ModuleID =     "org.apache.commons"            % "commons-jexl"          % "2.1.1"
  val cats: ModuleID =            "org.typelevel"                 %% "cats"                 % "0.9.0"
  val mysqlConnector: ModuleID =  "mysql"                         % "mysql-connector-java"  % "5.1.38"
  val liquibaseCore: ModuleID =   "org.liquibase"                 % "liquibase-core"        % "3.5.3"
  val logbackClassic: ModuleID =  "ch.qos.logback"                % "logback-classic"       % "1.1.6"
  val scalatest: ModuleID =       "org.scalatest"                 %% "scalatest"            % "2.2.4" % "test"
  val mockito: ModuleID =         "org.mockito"                   % "mockito-core"          % "2.7.22" % "test"
  val mockserverNetty: ModuleID = "org.mock-server"               % "mockserver-netty"      % "3.9.2" % "test"

  val spraySwagger: ModuleID = ("com.gettyimages" %% "spray-swagger" % "0.5.0"
    exclude("com.typesafe.scala-logging", "scala-logging-slf4j_2.11")
    exclude("com.typesafe.scala-logging", "scala-logging-api_2.11")
    exclude("com.google.guava", "guava"))

  val wdl4s: ModuleID = ("org.broadinstitute" %% "wdl4s" % wdl4sV
    exclude("org.typelevel", "cats_2.11")
    exclude("io.spray", "spray-json_2.11"))

  val metricsDependencies = Seq(
    metricsScala,
    metricsStatsd,
    scalatest,
    mockito
  )

  val googleDependencies = metricsDependencies ++ Seq(
    sprayJson,
    sprayHttp,
    akkaActor,
    akkaTestkit,
    scalatest,

    googleCloudBilling,
    googleGenomics,
    googleStorage,
    googleCloudResourceManager,
    googleCompute,
    googleAdminDirectory,
    googlePlus,
    googleOAuth2,
    googlePubSub,
    googleServicemanagement,
    googleGuava
  )

  val utilDependencies = Seq(
    scalaLogging,
    akkaActor,
    akkaTestkit,
    scalatest,
    mockito
  )

  val modelDependencies = Seq(
    // I am not certain why I need jackson-core here but IntelliJ is confused without it and tests don't run
    jacksonCore,
    sprayJson,
    sprayHttp,
    sprayHttpx,
    jodaTime,
    jodaConvert,
    scalaLogging,
    googleApiClient,
    scalatest
  )

  val rawlsCoreDependencies: Seq[ModuleID] = modelDependencies ++ googleDependencies ++ metricsDependencies ++ Seq(
    typesafeConfig,
    spraySwagger,
    ravenLogback,
    slick,
    slickHikariCP,
    sprayCan,
    sprayRouting,
    sprayClient,
    swaggerUI,
    commonsJEXL,
    wdl4s,
    cats,
    mysqlConnector,
    liquibaseCore,
    logbackClassic,
    akkaTestkit,
    sprayTestkit,
    mockserverNetty,
    mockito
  )
}
