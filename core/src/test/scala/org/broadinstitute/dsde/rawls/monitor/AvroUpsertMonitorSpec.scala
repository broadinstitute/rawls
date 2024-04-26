package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddUpdateAttribute,
  AttributeUpdateOperation,
  EntityUpdateDefinition
}
import org.broadinstitute.dsde.rawls.model.ImportStatuses
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeFormat,
  AttributeName,
  AttributeString,
  DataReferenceName,
  Entity,
  GoogleProjectId,
  RawlsRequestContext,
  TypedAttributeListSerializer,
  UserInfo,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.ApiServiceSpec
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class AvroUpsertMonitorSpec(_system: ActorSystem)
    extends ApiServiceSpec
    with MockitoSugar
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withConstantTestDataApiServices[T](testCode: TestApiService => T): T =
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def this() = this(ActorSystem("AvroUpsertMonitorSpec"))

  override def beforeAll(): Unit =
    super.beforeAll()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val workspaceName = testData.workspace.toWorkspaceName
  val workspaceId = testData.workspace.workspaceIdAsUUID
  val importStatusFailingWorkspace = testData.workspaceNoSubmissions.toWorkspaceName
  val googleStorage = FakeGoogleStorageInterpreter
  val importReadPubSubTopic = "request-topic"
  val importReadSubscriptionName = "request-sub"
  val importWritePubSubTopic = "status-topic"
  val importWriteSubscriptionName = "status-sub"
  val cwdsWritePubSubTopic = "cwds-status-topic"
  val cwdsWriteSubscriptionName = "cwds-status-sub"

  val bucketName = GcsBucketName("fake-bucket")
  val entityName = "avro-entity"
  val entityType = "test-type"
  val failImportStatusUUID = UUID.randomUUID()

  def testImportServiceAttributes(importId: UUID): Map[String, String] = Map(
    "workspaceName" -> workspaceName.name,
    "workspaceNamespace" -> workspaceName.namespace,
    "userEmail" -> userInfo.userEmail.toString,
    "upsertFile" -> s"$bucketName/${importId.toString}",
    "jobId" -> importId.toString
  )

  def testAttributes(importId: UUID): Map[String, String] = Map(
    "isCWDS" -> "true",
    "workspaceId" -> workspaceId.toString,
    "userEmail" -> userInfo.userEmail.toString,
    "upsertFile" -> s"$bucketName/${importId.toString}",
    "jobId" -> importId.toString
  )

  // Create the monitor supervisor config
  val config = AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig(
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    FiniteDuration.apply(1, TimeUnit.SECONDS),
    importReadPubSubTopic,
    importReadSubscriptionName,
    importWritePubSubTopic,
    cwdsWritePubSubTopic,
    600,
    1000,
    1
  )

  private def setUpPubSub(services: TestApiService) =
    // create the two topics and the subscription. These are futures so we need to wait for them
    // to complete before allowing tests to run.
    Await.result(
      for {
        readTopicCreate <- services.gpsDAO.createTopic(importReadPubSubTopic)
        writeTopicCreate <- services.gpsDAO.createTopic(importWritePubSubTopic)
        subscriptionCreate <- services.gpsDAO.createSubscription(importWritePubSubTopic, importWriteSubscriptionName)
        cwdsWriteTopicCreate <- services.gpsDAO.createTopic(cwdsWritePubSubTopic)
        cwdsSubscriptionCreate <- services.gpsDAO.createSubscription(cwdsWritePubSubTopic, cwdsWriteSubscriptionName)
      } yield {
        assert(readTopicCreate, "did not create read topic")
        assert(writeTopicCreate, "did not create write topic")
        assert(subscriptionCreate, "did not create write subscription")
        assert(cwdsWriteTopicCreate, "did not create cWDS write topic")
        assert(cwdsSubscriptionCreate, "did not create cWDS write subscription")
        readTopicCreate && writeTopicCreate && subscriptionCreate && cwdsWriteTopicCreate && cwdsSubscriptionCreate
      },
      Duration.apply(10, TimeUnit.SECONDS)
    )

  def setUp(services: TestApiService) = {
    setUpPubSub(services)

    val mockImportServiceDAO = new MockImportServiceDAO()

    // Start the monitor
    system.actorOf(
      AvroUpsertMonitorSupervisor.props(
        services.entityServiceConstructor,
        services.gcsDAO,
        services.samDAO,
        googleStorage,
        services.gpsDAO,
        services.gpsDAO,
        mockImportServiceDAO,
        config,
        slickDataSource
      )
    )

    mockImportServiceDAO
  }

  def setUpMockImportService(services: TestApiService): ImportServiceDAO = {
    setUpPubSub(services)

    val mockImportServiceDAO = mock[ImportServiceDAO]

    // Start the monitor
    system.actorOf(
      AvroUpsertMonitorSupervisor.props(
        services.entityServiceConstructor,
        services.gcsDAO,
        services.samDAO,
        googleStorage,
        services.gpsDAO,
        services.gpsDAO,
        mockImportServiceDAO,
        config,
        slickDataSource
      )
    )

    mockImportServiceDAO
  }

  def setUpMockEntityManager(services: TestApiService): (MockImportServiceDAO, EntityService) = {
    setUpPubSub(services)

    val mockImportServiceDAO = new MockImportServiceDAO()

    val mockEntityService = mock[EntityService]

    when(
      mockEntityService.batchUpdateEntitiesInternal(
        any[WorkspaceName],
        any[Seq[EntityUpdateDefinition]],
        any[Boolean],
        any[Option[DataReferenceName]],
        any[Option[GoogleProjectId]]
      )
    ).thenReturn(Future(Seq.empty[Entity]))

    val mockEntityServiceConstructor: RawlsRequestContext => EntityService = _ => mockEntityService

    // Start the monitor
    system.actorOf(
      AvroUpsertMonitorSupervisor.props(
        mockEntityServiceConstructor,
        services.gcsDAO,
        services.samDAO,
        googleStorage,
        services.gpsDAO,
        services.gpsDAO,
        mockImportServiceDAO,
        config,
        slickDataSource
      )
    )

    (mockImportServiceDAO, mockEntityService)
  }

  def setUpMockSamDAO(services: TestApiService) = {
    setUpPubSub(services)

    val mockImportServiceDAO = new MockImportServiceDAO()
    val mockSamDAO = mock[SamDAO]

    when(mockSamDAO.getPetServiceAccountKeyForUser(testData.workspace.googleProjectId, userInfo.userEmail))
      .thenReturn(Future.failed(new Exception("USer not found")))

    // Start the monitor
    system.actorOf(
      AvroUpsertMonitorSupervisor.props(
        services.entityServiceConstructor,
        services.gcsDAO,
        mockSamDAO,
        googleStorage,
        services.gpsDAO,
        services.gpsDAO,
        mockImportServiceDAO,
        config,
        slickDataSource
      )
    )

    mockImportServiceDAO
  }

  behavior of "AvroUpsertMonitor"

  def upsertRange(upsertQuantity: Int): List[Int] = List.range(1, upsertQuantity + 1)
  def createUpsertOpsList(upsertRange: List[Int]): List[String] =
    upsertRange map (idx =>
      s"""{"name": "avro-entity-$idx", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}"""
    )
  def makeOpsJsonString(opsList: List[String]): String = s"[${opsList.mkString(",")}]"

  def makeOpsJsonString(upsertQuantity: Int): String = {
    val range = upsertRange(upsertQuantity: Int)
    val opsList = createUpsertOpsList(range)
    makeOpsJsonString(opsList)
  }

  List("Import Service", "cWDS") foreach { origin =>
    List(1, 2, 20, 250, 2345, 12345) foreach { upsertQuantity =>
      it should s"upsert $upsertQuantity entities when origin is $origin" in withTestDataApiServices { services =>
        val timeout = 120000 milliseconds
        val interval = 500 milliseconds
        val importId1 = UUID.randomUUID()

        // generate the inbound pubsub message, which depends on origin
        val originMessageAttributes = if (origin.equals("Import Service")) {
          testImportServiceAttributes(importId1)
        } else {
          testAttributes(importId1)
        }

        // add the imports and their statuses to the mock importserviceDAO
        val mockImportServiceDAO = setUp(services)
        mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

        // create upsert json file
        val contents = makeOpsJsonString(upsertQuantity)

        // Store upsert json file
        Await.result(
          googleStorage
            .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
            .compile
            .drain
            .unsafeToFuture(),
          Duration.apply(10, TimeUnit.SECONDS)
        )

        Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                     Duration.apply(10, TimeUnit.SECONDS)
        )

        // Publish message on the request topic
        services.gpsDAO.publishMessages(importReadPubSubTopic,
                                        List(MessageRequest(importId1.toString, originMessageAttributes))
        )

        // check if correct message was posted on request topic
        eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
          assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
        }
        // Check in db if entities are there
        withWorkspaceContext(testData.workspace) { context =>
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            val entitiesOfType = runAndWait(entityQuery.UnitTestHelpers.listActiveEntitiesOfType(context, entityType))
            assertResult(upsertQuantity)(entitiesOfType.size)
            upsertRange(upsertQuantity) foreach { idx =>
              val name = s"avro-entity-$idx"
              val entity =
                Entity(name, entityType, Map(AttributeName("default", "avro-attribute") -> AttributeString("foo")))
              val actual = entitiesOfType.find(_.name == name)
              assertResult(Some(entity))(actual)
            }
          }
        }
        // check that a pubsub message was published to the appropriate topic to update the job status
        val correctSub = if (origin.equals("Import Service")) importWriteSubscriptionName else cwdsWriteSubscriptionName

        eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
          val statusMessages =
            Await.result(services.gpsDAO.pullMessages(correctSub, 1), Duration.apply(10, TimeUnit.SECONDS))
          assert(statusMessages.exists { msg =>
            msg.attributes("importId").contains(importId1.toString) &&
            msg.attributes("newStatus").contains("Done") &&
            msg.attributes("action").contains("status")
          })
        }
        // and check that no message was published to the other topic
        val incorrectSub =
          if (origin.equals("Import Service")) cwdsWriteSubscriptionName else importWriteSubscriptionName
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(incorrectSub, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(!statusMessages.exists(msg => msg.attributes("importId").contains(importId1.toString)))

      }
    }
  }

  it should "return error for imports with the wrong status" in withTestDataApiServices { services =>
    val importId2 = UUID.randomUUID()
    val importId3 = UUID.randomUUID()
    val importId4 = UUID.randomUUID()

    val mockImportServiceDAO = setUp(services)
    mockImportServiceDAO.imports += (importId2 -> ImportStatuses.Upserting)
    mockImportServiceDAO.imports += (importId3 -> ImportStatuses.Done)
    mockImportServiceDAO.imports += (importId4 -> ImportStatuses.Error)

    services.gpsDAO.publishMessages(importReadPubSubTopic,
                                    List(MessageRequest(importId2.toString, testAttributes(importId2)))
    )
    services.gpsDAO.publishMessages(importReadPubSubTopic,
                                    List(MessageRequest(importId3.toString, testAttributes(importId3)))
    )
    services.gpsDAO.publishMessages(importReadPubSubTopic,
                                    List(MessageRequest(importId4.toString, testAttributes(importId4)))
    )

    Thread.sleep(1000)

    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId2.toString, 1))
    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId3.toString, 1))
    assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId4.toString, 1))

  }

  it should "mark the import job as failed on malformed json file" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO = setUp(services)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create a -malformed- upsert json file, which will cause problems when reading
    val contents = "hey, this isn't valid json! {{{"

    // Store upsert json file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    val blob =
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importReadPubSubTopic,
                                    List(MessageRequest(importId1.toString, testAttributes(importId1)))
    )

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
    }

    // upsert will fail; check that a pubsub message was published to set the import job to error.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      val statusMessages =
        Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))

      assert(statusMessages.exists { msg =>
        msg.attributes.get("importId").contains(importId1.toString) &&
        msg.attributes.get("newStatus").contains("Error") &&
        msg.attributes.get("action").contains("status")
      })
    }
  }

  it should "mark the import job as failed on valid json that doesn't match our model" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO = setUp(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      // create a valid json file that doesn't contain entities
      val contents = """[{"foo":"bar"},{"baz":"qux"}]"""

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, testAttributes(importId1)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))

        assert(statusMessages.exists { msg =>
          msg.attributes.get("importId").contains(importId1.toString) &&
          msg.attributes.get("newStatus").contains("Error") &&
          msg.attributes.get("action").contains("status")
        })
      }
  }

  it should "mark the import job as failed if upsert file doesn't exist" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO = setUp(services)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    // create a valid json file that doesn't contain entities
    val contents =
      s"""[{"name": "avro-entity", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}]"""

    // Store upsert json file, even though we expect the code to look elsewhere and miss this file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    val blob =
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

    // Publish message on the request topic - but ensure that the gcs: location in the pubsub message is incorrect
    val badMessageAttrs =
      testAttributes(importId1) ++ Map("upsertFile" -> s"$bucketName/intentionally.nonexistent.unittest")
    services.gpsDAO.publishMessages(importReadPubSubTopic, List(MessageRequest(importId1.toString, badMessageAttrs)))

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
    }

    // upsert will fail; check that a pubsub message was published to set the import job to error.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      val statusMessages =
        Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))

      assert(statusMessages.exists { msg =>
        msg.attributes.get("importId").contains(importId1.toString) &&
        msg.attributes.get("newStatus").contains("Error") &&
        msg.attributes.get("action").contains("status")
      })
    }
  }

  it should "publish pubsub message to mark import job as Error if upserts result in partial failure" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO = setUp(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      val successfulBatch = createUpsertOpsList(upsertRange(1000))
      val failureBatch =
        s"""{"name": "avro-entity-failure", "entityType": "", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}"""
      val contents = makeOpsJsonString(successfulBatch :+ failureBatch)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, testAttributes(importId1)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(importId1.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status") &&
          msg
            .attributes("errorMessage")
            .contains("Successfully updated 1000 entities; 1 updates failed. First 100 failures are: Invalid input")
        })
      }
  }

  it should "bubble up useful error message if upserts result in partial failure" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO = setUp(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)
      val successfulBatch = createUpsertOpsList(upsertRange(1000))

      // failure creates an entity that refers to a non-existent entity
      val ref = AttributeEntityReference("test-type", "this-entity-does-not-exist")
      val op: AttributeUpdateOperation =
        AddUpdateAttribute(AttributeName.withDefaultNS("intentionallyBadReference"), ref)
      import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperationFormat
      import spray.json._
      implicit val attributeFormat: AttributeFormat = new AttributeFormat with TypedAttributeListSerializer
      val opJsonString = op.toJson.compactPrint
      val failureBatch = s"""{"name": "avro-entity-failure", "entityType": "failme", "operations": [$opJsonString]}"""
      val contents = makeOpsJsonString(successfulBatch :+ failureBatch)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, testAttributes(importId1)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(importId1.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status") &&
          msg
            .attributes("errorMessage")
            .contains(
              "Successfully updated 1000 entities; 1 updates failed. First 100 failures are: test-type this-entity-does-not-exist not found"
            )
        })
      }
  }

  it should "bubble up useful error message if upserts result in complete failure" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO = setUp(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      // failure creates an entity that refers to a non-existent entity
      val ref = AttributeEntityReference("test-type", "this-entity-does-not-exist")
      val op: AttributeUpdateOperation =
        AddUpdateAttribute(AttributeName.withDefaultNS("intentionallyBadReference"), ref)
      import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperationFormat
      import spray.json._
      implicit val attributeFormat: AttributeFormat = new AttributeFormat with TypedAttributeListSerializer
      val opJsonString = op.toJson.compactPrint
      val failureBatch = s"""{"name": "avro-entity-failure", "entityType": "failme", "operations": [$opJsonString]}"""
      val contents = makeOpsJsonString(List(failureBatch))

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, testAttributes(importId1)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      val errorMsg = eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(importId1.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status")
        })
        statusMessages.head
      }

      withClue("Text in the pubsub error message was incorrect:") {
        errorMsg.attributes("errorMessage") shouldBe
          "All entities failed to update. There were 1 errors in total. Error messages: test-type this-entity-does-not-exist not found"
      }
  }

  it should "ack pubsub message if upsert published to nonexistent workspace" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds
    val importId1 = UUID.randomUUID()

    // add the imports and their statuses to the mock importserviceDAO
    val mockImportServiceDAO = setUp(services)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    val contents = makeOpsJsonString(100)

    // Store upsert json file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    // Make sure the file saved properly
    Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                 Duration.apply(10, TimeUnit.SECONDS)
    )

    // acks should be empty at this point
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks shouldBe empty
    }

    // Publish message on the request topic with a nonexistent workspace
    val messageAttributes = testAttributes(importId1) ++ Map("workspaceId" -> UUID.randomUUID().toString)
    services.gpsDAO.publishMessages(importReadPubSubTopic, List(MessageRequest(importId1.toString, messageAttributes)))

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString))
    }

    // upsert will fail; check that a pubsub message was acked.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks should not be empty
    }

  }

  it should "ack pubsub message if error occurs when finding pet account" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds
    val importId1 = UUID.randomUUID()

    // MockSamDAO should throw an error when fetching the pet service account
    val mockImportServiceDAO = setUpMockSamDAO(services)
    mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

    val contents = makeOpsJsonString(100)

    // Store upsert json file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    // Make sure the file saved properly
    Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                 Duration.apply(10, TimeUnit.SECONDS)
    )

    // acks should be empty at this point
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks shouldBe empty
    }

    // Publish message on the request topic
    services.gpsDAO.publishMessages(importReadPubSubTopic,
                                    List(MessageRequest(importId1.toString, testAttributes(importId1)))
    )

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString))
    }

    // upsert will fail; check that a pubsub message was acked.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks should not be empty
    }

  }

  it should "ack pubsub message if error occurs when fetching import status" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds

    val mockImportServiceDAO = setUpMockImportService(services)
    when(mockImportServiceDAO.getImportStatus(any[UUID], any[WorkspaceName], any[UserInfo]))
      .thenReturn(Future.failed(new Exception("User not found")))

    val contents = makeOpsJsonString(100)

    // Store upsert json file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(failImportStatusUUID.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    // Make sure the file saved properly
    Await.result(
      googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(failImportStatusUUID.toString)).unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    // acks should be empty at this point
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks shouldBe empty
    }

    // Publish message on the request topic
    services.gpsDAO.publishMessages(
      importReadPubSubTopic,
      List(MessageRequest(failImportStatusUUID.toString, testAttributes(failImportStatusUUID)))
    )

    // check if correct message was posted on request topic. This will start the upsert attempt.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, failImportStatusUUID.toString))
    }

    // upsert will fail; check that a pubsub message was acked.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks should not be empty
    }

  }

  it should "ack pubsub message if message is double delivered" in withTestDataApiServices { services =>
    val timeout = 30000 milliseconds
    val interval = 250 milliseconds
    val importUuid = UUID.randomUUID()

    val mockImportServiceDAO = setUpMockImportService(services)
    when(
      mockImportServiceDAO.getImportStatus(any[UUID], any[WorkspaceName], any[UserInfo])
    ).thenReturn(Future.successful(Some(ImportStatuses.Done)))

    val contents = makeOpsJsonString(100)

    // Store upsert json file
    Await.result(
      googleStorage
        .createBlob(bucketName, GcsBlobName(importUuid.toString), contents.getBytes())
        .compile
        .drain
        .unsafeToFuture(),
      Duration.apply(10, TimeUnit.SECONDS)
    )

    // acks should be empty at this point
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks shouldBe empty
    }

    // Publish message on the request topic
    services.gpsDAO.publishMessages(
      importReadPubSubTopic,
      List(MessageRequest(importUuid.toString, testAttributes(importUuid)))
    )

    // check that a pubsub message was acked.
    eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
      services.gpsDAO.acks should not be empty
    }

  }

  it should "update import status to error if upsert published to nonexistent workspace" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val mockImportServiceDAO = setUp(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      val contents = makeOpsJsonString(100)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic with a nonexistent workspace
      val messageAttributes = testAttributes(importId1) ++ Map("workspaceId" -> UUID.randomUUID().toString)
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, messageAttributes))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(importId1.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status")
        })
      }

  }

  it should "update import status to error if error occurs when finding pet account" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds
      val importId1 = UUID.randomUUID()

      // MockSamDAO should throw an error when fetching the pet service account
      val mockImportServiceDAO = setUpMockSamDAO(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      val contents = makeOpsJsonString(100)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic,
                                      List(MessageRequest(importId1.toString, testAttributes(importId1)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(importId1.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status")
        })
      }

  }

  it should "update import status to error if error occurs when fetching import status" in withTestDataApiServices {
    services =>
      val timeout = 30000 milliseconds
      val interval = 250 milliseconds

      {
        val mockImportServiceDAO = setUpMockImportService(services)

        when(mockImportServiceDAO.getImportStatus(any[UUID], any[WorkspaceName], any[UserInfo]))
          .thenReturn(Future.failed(new Exception("User not found")))

        mockImportServiceDAO
      }

      val contents = makeOpsJsonString(100)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(failImportStatusUUID.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Make sure the file saved properly
      Await.result(
        googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(failImportStatusUUID.toString)).unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      // Publish message on the request topic
      services.gpsDAO.publishMessages(
        importReadPubSubTopic,
        List(MessageRequest(failImportStatusUUID.toString, testAttributes(failImportStatusUUID)))
      )

      // check if correct message was posted on request topic. This will start the upsert attempt.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, failImportStatusUUID.toString))
      }

      // upsert will fail; check that a pubsub message was published to set the import job to error.
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        val statusMessages =
          Await.result(services.gpsDAO.pullMessages(cwdsWriteSubscriptionName, 1), Duration.apply(10, TimeUnit.SECONDS))
        assert(statusMessages.exists { msg =>
          msg.attributes("importId").contains(failImportStatusUUID.toString) &&
          msg.attributes("newStatus").contains("Error") &&
          msg.attributes("action").contains("status")
        })
      }
  }

  // test cases for upsert vs. update handling:
  // a map of {value in pubsub message attribute}->{expected behavior}
  case class UpsertExpectation(isUpsert: Boolean, message: String)
  val upsertCases = Map(
    None -> UpsertExpectation(isUpsert = true, "omitted"),
    Some("true") -> UpsertExpectation(isUpsert = true, "true"),
    Some("tRuE") -> UpsertExpectation(isUpsert = true, "true, case-insensitive"),
    Some("false") -> UpsertExpectation(isUpsert = false, "false"),
    Some("no thank you") -> UpsertExpectation(isUpsert = false, "some value other than case-insensitive true")
  )

  upsertCases foreach { case (inputAttribute, expectation) =>
    val methodString = if (expectation.isUpsert) "upsert" else "update"
    it should s"$methodString when isUpsert is ${expectation.message}" in withTestDataApiServices { services =>
      val timeout = 120000 milliseconds
      val interval = 500 milliseconds
      val importId1 = UUID.randomUUID()

      // add the imports and their statuses to the mock importserviceDAO
      val (mockImportServiceDAO, mockEntityService) = setUpMockEntityManager(services)
      mockImportServiceDAO.imports += (importId1 -> ImportStatuses.ReadyForUpsert)

      // create upsert json file
      val contents = makeOpsJsonString(1)

      // Store upsert json file
      Await.result(
        googleStorage
          .createBlob(bucketName, GcsBlobName(importId1.toString), contents.getBytes())
          .compile
          .drain
          .unsafeToFuture(),
        Duration.apply(10, TimeUnit.SECONDS)
      )

      Await.result(googleStorage.unsafeGetBlobBody(bucketName, GcsBlobName(importId1.toString)).unsafeToFuture(),
                   Duration.apply(10, TimeUnit.SECONDS)
      )

      // message to publish on the request topic:
      val additionalAttributes = inputAttribute match {
        case Some(input) => Map("isUpsert" -> input)
        case None        => Map.empty[String, String]
      }
      val msg = testAttributes(importId1) ++ additionalAttributes

      // Publish message on the request topic
      services.gpsDAO.publishMessages(importReadPubSubTopic, List(MessageRequest(importId1.toString, msg)))

      // check if correct message was posted on request topic
      eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
        assert(services.gpsDAO.receivedMessage(importReadPubSubTopic, importId1.toString, 1))
      }

      // check if, eventually, EntityService.batchUpdateEntitiesInternal is called with upsert={expectation.isUpsert}
      // from BucketDeletionMonitorSpec:
      // `eventually` now requires an implicit `Retrying` instance. When the statement inside returns future, it'll
      // try to use `Retrying[Future[T]]`, which gets weird when we're using mockito together with it.
      // Hence adding ascribing [Unit] explicitly here so that `eventually` will use `Retrying[Unit]`
      eventually[Unit](Timeout(timeout), Interval(interval)) {
        verify(mockEntityService, times(1)).batchUpdateEntitiesInternal(
          any[WorkspaceName],
          any[Seq[EntityUpdateDefinition]],
          ArgumentMatchers.eq(expectation.isUpsert),
          any[Option[DataReferenceName]],
          any[Option[GoogleProjectId]]
        )
      }
    }
  }

  behavior of "ImportStatuses.fromCwdsStatus"

  private val testCases = Map(
    "running" -> ImportStatuses.ReadyForUpsert,
    "RUNNING" -> ImportStatuses.ReadyForUpsert,
    "succeeded" -> ImportStatuses.Done,
    "SUCCEEDED" -> ImportStatuses.Done,
    "error" -> ImportStatuses.Error,
    "ERROR" -> ImportStatuses.Error
  )
  testCases foreach { case (input, expected) =>
    it should s"translate $input to $expected" in {
      ImportStatuses.fromCwdsStatus(input) shouldBe expected
    }
  }

  private val errorCases = List("CREATED", "QUEUED", "CANCELLED", "UNKNOWN", "something-else")
  errorCases foreach { input =>
    it should s"throw error trying to translate $input" in {
      intercept[RawlsException] {
        ImportStatuses.fromCwdsStatus(input)
      }
    }
  }

}
