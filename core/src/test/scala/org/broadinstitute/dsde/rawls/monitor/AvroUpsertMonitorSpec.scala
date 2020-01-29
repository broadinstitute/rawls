package org.broadinstitute.dsde.rawls.monitor

import java.io.ByteArrayOutputStream
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.ApiServiceSpec
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest

import scala.concurrent.ExecutionContext.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

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
  val importRequestPubSubTopic = "request-topic"
  val importRequestSubscriptionName = "request-sub"
  val importStatusPubSubTopic = "status-topic"
  val importStatusSubscriptionName = "status-sub"
  val bucketName = GcsBucketName("fake-bucket")
  val blobName = GcsBlobName("fake-file")
  val entityName = "avro-entity"
  val entityType = "test-type"
  val sampleMessage = "here's a sample message"

  def testAttributes(importId: UUID) = Map(
    "workspaceName" -> workspaceName.name,
    "workspaceNamespace" -> workspaceName.namespace,
    "userEmail" -> userInfo.userEmail.toString,
    "upsertFile" -> blobName.value,
    "jobId" -> importId.toString
  )

  // Create the monitor supervisor config
  val config = AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig(
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    importRequestPubSubTopic,
    importRequestSubscriptionName,
    importStatusPubSubTopic,
    importStatusSubscriptionName,
    bucketName,
    1000,
    1
  )

  def setUp(services: TestApiService) = {
    // create the two topics
    services.gpsDAO.createTopic(importRequestPubSubTopic)
    services.gpsDAO.createTopic(importStatusPubSubTopic)

    val mockImportServiceDAO =  new MockImportServiceDAO()

    // Start the monitor
    system.actorOf(AvroUpsertMonitorSupervisor.props(
      services.workspaceServiceConstructor,
      services.gcsDAO,
      services.samDAO,
      googleStorage,
      services.gpsDAO,
      mockImportServiceDAO,
      config
    ))

    mockImportServiceDAO
  }


  "AvroUpsertMonitor" should "upsert entities" in withTestDataApiServices { services =>

    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO =  setUp(services)
    mockImportServiceDAO.groups += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create compressed file
    val contents = """[{"name": "avro-entity", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}]"""
    val bos = new ByteArrayOutputStream(contents.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(contents.getBytes())
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()

    // Store compressed file
    Await.result(googleStorage.createBlob(bucketName, blobName, compressed).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importRequestPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId1))))

    // check if correct message was posted on request topic
    assert(services.gpsDAO.receivedMessage(importRequestPubSubTopic, sampleMessage, 1))

    Thread.sleep(1000)

    // Check in db if entities are there
    withWorkspaceContext(testData.workspace) { context =>
      val entity = Entity(entityName, entityType, Map(AttributeName("default", "avro-attribute") -> AttributeString("foo")))
      assertResult(Some(entity)) { runAndWait(entityQuery.get(context, entityType, entityName)) }
    }
  }


  "AvroUpsertMonitor" should "return error for imports with the wrong status" in withTestDataApiServices { services =>
    val importId2 = UUID.randomUUID()
    val importId3 = UUID.randomUUID()
    val importId4 = UUID.randomUUID()

    val mockImportServiceDAO =  setUp(services)
    mockImportServiceDAO.groups += (importId2 -> ImportStatuses.Upserting)
    mockImportServiceDAO.groups += (importId3 -> ImportStatuses.Done)
    mockImportServiceDAO.groups += (importId4 -> ImportStatuses.Error("Some error"))

    services.gpsDAO.publishMessages(importRequestPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId2))))
    services.gpsDAO.publishMessages(importRequestPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId3))))
    services.gpsDAO.publishMessages(importRequestPubSubTopic, Seq(MessageRequest(sampleMessage, testAttributes(importId4))))

    Thread.sleep(1000)

    assert(services.gpsDAO.receivedMessage(importRequestPubSubTopic, sampleMessage, 3))

  }
}