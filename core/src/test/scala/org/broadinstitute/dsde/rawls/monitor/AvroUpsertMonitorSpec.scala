package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity, ImportStatuses}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.ApiServiceSpec
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}

import scala.concurrent.ExecutionContext.global
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps

class AvroUpsertMonitorSpec(_system: ActorSystem) extends ApiServiceSpec with MockitoSugar with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withConstantTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  implicit val cs = IO.contextShift(global)
  def this() = this(ActorSystem("AvroUpsertMonitorSpec"))

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val workspaceName =  testData.workspace.toWorkspaceName
  val googleStorage = FakeGoogleStorageInterpreter
  val arrowPubSubTopic = "fake-arrow-pub-sub-topic"          // remove when cutting over to import service
  val arrowPubSubSubscription = "arrow-pub-sub-subscription" // remove when cutting over to import service
  val arrowBucketName = "arrow-bucket-name"                  // remove when cutting over to import service
  val importReadPubSubTopic = "request-topic"
  val importReadSubscriptionName = "request-sub"
  val importWritePubSubTopic = "status-topic"
  val importWriteSubscriptionName = "status-sub"
  val bucketName = GcsBucketName("fake-bucket")
  val blobName = GcsBlobName("fake-file")
  val entityName = "avro-entity"
  val entityType = "test-type"
  val sampleMessage = "here's a sample message"

  def testAttributes(importId: UUID) = Map(
    "workspaceName" -> workspaceName.name,
    "workspaceNamespace" -> workspaceName.namespace,
    "userEmail" -> userInfo.userEmail.toString,
    "upsertFile" ->  s"$bucketName/${blobName.value}",
    "jobId" -> importId.toString
  )

  // Create the monitor supervisor config
  val config = AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig(
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    arrowPubSubTopic,        // remove when cutting over to import service
    arrowPubSubSubscription, // remove when cutting over to import service
    arrowBucketName,         // remove when cutting over to import service
    importReadPubSubTopic,
    importReadSubscriptionName,
    importWritePubSubTopic,
    600,
    1000,
    1
  )

  def setUp(services: TestApiService) = {
    // create the two topics
    services.gpsDAO.createTopic(importReadPubSubTopic)
    services.gpsDAO.createTopic(importWritePubSubTopic)
    services.gpsDAO.createTopic(arrowPubSubTopic)      // remove when cutting over to import service

    val mockImportServiceDAO =  new MockImportServiceDAO()

    // Start the monitor
    system.actorOf(AvroUpsertMonitorSupervisor.props(
      services.workspaceServiceConstructor,
      services.gcsDAO,
      services.samDAO,
      googleStorage,
      services.gpsDAO,
      services.gpsDAO, // remove when cutting over to import service
      mockImportServiceDAO,
      config
    ))

    mockImportServiceDAO
  }


  "AvroUpsertMonitor" should "upsert entities" in withTestDataApiServices { services =>
    val timeout = 5000 milliseconds
    val interval = 250 milliseconds
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO =  setUp(services)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create compressed file
    val contents = """[{"name": "avro-entity", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}]"""

    // Store compressed file
    Await.result(googleStorage.createBlob(bucketName, blobName, contents.getBytes()).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    val blob = Await.result(googleStorage.unsafeGetBlobBody(bucketName, blobName).unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importReadPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId1))))

    // check if correct message was posted on request topic
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, sampleMessage, 1))
    }
    // Check in db if entities are there
    withWorkspaceContext(testData.workspace) { context =>
      val entity = Entity(entityName, entityType, Map(AttributeName("default", "avro-attribute") -> AttributeString("foo")))

      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assertResult(Some(entity)) { runAndWait(entityQuery.get(context, entityType, entityName)) }
      }
    }
  }


  "AvroUpsertMonitor" should "return error for imports with the wrong status" in withTestDataApiServices { services =>
    val importId2 = UUID.randomUUID()
    val importId3 = UUID.randomUUID()
    val importId4 = UUID.randomUUID()

    val mockImportServiceDAO =  setUp(services)
    mockImportServiceDAO.imports += (importId2 -> ImportStatuses.Upserting)
    mockImportServiceDAO.imports += (importId3 -> ImportStatuses.Done)
    mockImportServiceDAO.imports += (importId4 -> ImportStatuses.Error)

    services.gpsDAO.publishMessages(importReadPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId2))))
    services.gpsDAO.publishMessages(importReadPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId3))))
    services.gpsDAO.publishMessages(importReadPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId4))))

    Thread.sleep(1000)

    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, sampleMessage, 3))

  }
}