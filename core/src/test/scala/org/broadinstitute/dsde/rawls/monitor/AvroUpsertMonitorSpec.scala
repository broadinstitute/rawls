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
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}

import scala.concurrent.ExecutionContext.global
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class AvroUpsertMonitorSpec(_system: ActorSystem) extends ApiServiceSpec with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually {

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
  def importReadPubSubTopic(pubsubPrefix: String) = s"$pubsubPrefix-request-topic"
  def importReadSubscriptionName(pubsubPrefix: String) = s"$pubsubPrefix-request-sub"
  def importWritePubSubTopic(pubsubPrefix: String) = s"$pubsubPrefix-status-topic"
  def importWriteSubscriptionName(pubsubPrefix: String) = s"$pubsubPrefix-status-sub"
  val bucketName = GcsBucketName("fake-bucket")
  val entityName = "avro-entity"
  val entityType = "test-type"

  def testAttributes(importId: UUID) = Map(
    "workspaceName" -> workspaceName.name,
    "workspaceNamespace" -> workspaceName.namespace,
    "userEmail" -> userInfo.userEmail.toString,
    "upsertFile" ->  s"$bucketName/${importId.toString}",
    "jobId" -> importId.toString
  )

  // Create the monitor supervisor config
  def makeConfig(pubsubPrefix: String) = {
    AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig(
      FiniteDuration.apply(1, TimeUnit.SECONDS),
      FiniteDuration.apply(1, TimeUnit.SECONDS),
      importReadPubSubTopic(pubsubPrefix),
      importReadSubscriptionName(pubsubPrefix),
      importWritePubSubTopic(pubsubPrefix),
      600,
      1000,
      1
    )
  }

  def setUp(services: TestApiService, pubsubPrefix: String) = {
    // create the two topics
    services.gpsDAO.createTopic(importReadPubSubTopic(pubsubPrefix))
    services.gpsDAO.createTopic(importWritePubSubTopic(pubsubPrefix)) map { _ =>
      services.gpsDAO.createSubscription(importWritePubSubTopic(pubsubPrefix), importWriteSubscriptionName(pubsubPrefix))
    }

    val mockImportServiceDAO =  new MockImportServiceDAO()

    // Start the monitor
    system.actorOf(AvroUpsertMonitorSupervisor.props(
      services.entityServiceConstructor,
      services.gcsDAO,
      services.samDAO,
      googleStorage,
      services.gpsDAO,
      services.gpsDAO,
      mockImportServiceDAO,
      makeConfig(pubsubPrefix),
      slickDataSource
    ))

    mockImportServiceDAO
  }

  behavior of "AvroUpsertMonitor"

  List(1,2,20,250,2345,12345) foreach { upsertQuantity =>
    it should s"upsert $upsertQuantity entities" in withTestDataApiServices { services =>
      val timeout = 120000 milliseconds
      val interval = 500 milliseconds
      val pubsubPrefix = s"upsert$upsertQuantity"
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO =  setUp(services, pubsubPrefix)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      // create indexed range of ints
      val upsertRange: List[Int] = List.range(1, upsertQuantity+1)


      // create upsert json file
      val upsertOps = upsertRange map ( idx => s"""{"name": "avro-entity-$idx", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}""" )

      val contents = s"[${upsertOps.mkString(",")}]"

      // Store upsert json file
      Await.result(googleStorage.createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes()).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId1.toString, testAttributes(importId1))))

      // check if correct message was posted on request topic
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId1.toString, 1))
      }
      // Check in db if entities are there
      withWorkspaceContext(testData.workspace) { context =>

        eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
          val entitiesOfType = runAndWait(entityQuery.listActiveEntitiesOfType(context, entityType))
          assertResult(upsertQuantity) { entitiesOfType.size }
          upsertRange foreach { idx =>
            val name = s"avro-entity-$idx"
            val entity = Entity(name, entityType, Map(AttributeName("default", "avro-attribute") -> AttributeString("foo")))
            val actual = entitiesOfType.find(_.name == name)
            assertResult(Some(entity)) { actual }
          }
        }

      }
    }
  }


  it should "return error for imports with the wrong status" in withTestDataApiServices { services =>
    val pubsubPrefix = "wrongstatus"
    val importId2 = UUID.randomUUID()
    val importId3 = UUID.randomUUID()
    val importId4 = UUID.randomUUID()

    val mockImportServiceDAO =  setUp(services, pubsubPrefix)
    mockImportServiceDAO.imports += (importId2 -> ImportStatuses.Upserting)
    mockImportServiceDAO.imports += (importId3 -> ImportStatuses.Done)
    mockImportServiceDAO.imports += (importId4 -> ImportStatuses.Error)

    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId2.toString, testAttributes(importId2))))
    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId3.toString, testAttributes(importId3))))
    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId4.toString, testAttributes(importId4))))

    Thread.sleep(1000)

    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId2.toString, 1))
    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId3.toString, 1))
    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId4.toString, 1))

  }

  it should "mark the import job as failed on malformed json file" in withTestDataApiServices { services =>
    val timeout = 120000 milliseconds
    val interval = 250 milliseconds
    val pubsubPrefix = "malformedjson"
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO =  setUp(services, pubsubPrefix)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create a -malformed- upsert json file, which will cause problems when reading
    val contents = "hey, this isn't valid json! {{{"

    // Store upsert json file
    Await.result(googleStorage.createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes()).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    val blob = Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId1.toString, testAttributes(importId1))))

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId1.toString, 1))
    }

    // upsert will fail; check that a pubsub message was published to set the import job to error.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      val statusMessages = Await.result(services.gpsDAO.pullMessages(importWriteSubscriptionName(pubsubPrefix), 1), Duration.apply(10, TimeUnit.SECONDS))

      if (statusMessages.nonEmpty) {
        statusMessages foreach { msg =>
          logger.warn(s">>>>>>> found message looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)}: ${msg.contents} | ${msg.attributes}")
        }
      }

      assert(statusMessages.exists { msg =>
        msg.attributes.get("importId").contains(importId1.toString) &&
          msg.attributes.get("newStatus").contains("Error") &&
          msg.attributes.get("action").contains("status")
      }, s"statusMessages looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)} was: ${statusMessages}")
    }
  }

  it should "mark the import job as failed on valid json that doesn't match our model" in withTestDataApiServices { services =>
    val timeout = 120000 milliseconds
    val interval = 250 milliseconds
    val pubsubPrefix = "badmodel"
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO =  setUp(services, pubsubPrefix)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create a valid json file that doesn't contain entities
    val contents = """[{"foo":"bar"},{"baz":"qux"}]"""

    // Store upsert json file
    Await.result(googleStorage.createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes()).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId1.toString, testAttributes(importId1))))

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId1.toString, 1))
    }

    // upsert will fail; check that a pubsub message was published to set the import job to error.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      val statusMessages = Await.result(services.gpsDAO.pullMessages(importWriteSubscriptionName(pubsubPrefix), 1), Duration.apply(10, TimeUnit.SECONDS))

      if (statusMessages.nonEmpty) {
        statusMessages foreach { msg =>
          logger.warn(s">>>>>>> found message looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)}: ${msg.contents} | ${msg.attributes}")
        }
      }

      assert(statusMessages.exists { msg =>
        msg.attributes.get("importId").contains(importId1.toString) &&
          msg.attributes.get("newStatus").contains("Error") &&
          msg.attributes.get("action").contains("status")
      }, s"statusMessages looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)} was: ${statusMessages}")
    }
  }

  it should "mark the import job as failed if upsert file doesn't exist" in withTestDataApiServices { services =>
    val timeout = 120000 milliseconds
    val interval = 250 milliseconds
    val pubsubPrefix = "nonexistent"
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO =  setUp(services, pubsubPrefix)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create a valid json file that doesn't contain entities
    val contents = s"""[{"name": "avro-entity", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}]"""

    // Store upsert json file, even though we expect the code to look elsewhere and miss this file
    Await.result(googleStorage.createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes()).compile.drain.unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    val blob = Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(), Duration.apply(10, TimeUnit.SECONDS))

    // Publish message on the request topic - but ensure that the gcs: location in the pubsub message is incorrect
    val badMessageAttrs = testAttributes(importId1) ++ Map("upsertFile" ->  s"$bucketName/intentionally.nonexistent.unittest")
    services.gpsDAO.publishMessages(importReadPubSubTopic(pubsubPrefix), List(MessageRequest(importId1.toString, badMessageAttrs)))

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic(pubsubPrefix), importId1.toString, 1))
    }

    // upsert will fail; check that a pubsub message was published to set the import job to error.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      val statusMessages = Await.result(services.gpsDAO.pullMessages(importWriteSubscriptionName(pubsubPrefix), 1), Duration.apply(10, TimeUnit.SECONDS))

      if (statusMessages.nonEmpty) {
        statusMessages foreach { msg =>
          logger.warn(s">>>>>>> found message looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)}: ${msg.contents} | ${msg.attributes}")
        }
      }

      assert(statusMessages.exists { msg =>
        msg.attributes.get("importId").contains(importId1.toString) &&
          msg.attributes.get("newStatus").contains("Error") &&
          msg.attributes.get("action").contains("status")
      }, s"statusMessages looking for $importId1 on ${importWriteSubscriptionName(pubsubPrefix)} was: ${statusMessages}")
    }
  }

}
