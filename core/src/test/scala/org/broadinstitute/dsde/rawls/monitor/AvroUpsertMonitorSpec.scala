package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeName, AttributeString, Entity, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.ApiServiceSpec
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageInterpreterSpec}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, Future}
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


  "AvroUpsertMonitor" should "upsert entities" in withConstantTestDataApiServices { services =>
    val importId1 = UUID.randomUUID()
    val importId2 = UUID.randomUUID()
    val importId3 = UUID.randomUUID()
    val importId4 = UUID.randomUUID()
    val workspaceName = WorkspaceName("ns","ws")

    val mockImportServiceDAO = mock[ImportServiceDAO]

    when(mockImportServiceDAO.getImportStatus(importId1, workspaceName, userInfo)).thenReturn(Future.successful(Option(ImportStatuses.ReadyForUpsert)))
    when(mockImportServiceDAO.getImportStatus(importId2, workspaceName, userInfo)).thenReturn(Future.successful(Option(ImportStatuses.Upserting)))
    when(mockImportServiceDAO.getImportStatus(importId3, workspaceName, userInfo)).thenReturn(Future.successful(Option(ImportStatuses.Done)))

    val workspaceService =  services.workspaceServiceConstructor //workspaceServiceConstructor(userInfo1)
    val googleServicesDAO = services.gcsDAO
    val samDAO = services.samDAO
    val googleStorage = FakeGoogleStorageInterpreter
    val pubSubDao = services.gpsDAO
    val pubSubSubscriptionName = "fake-sub"
    val importServicePubSubTopic = "fake-topic"
    val avroUpsertBucketName = "fake-bucket"
    val batchSize = 1

    system.actorOf(AvroUpsertMonitor.props(FiniteDuration.apply(1, TimeUnit.SECONDS),
                                           FiniteDuration.apply(1, TimeUnit.SECONDS),
                                           workspaceService,
                                           googleServicesDAO,
                                           samDAO,
                                           googleStorage,
                                           pubSubDao,
                                           pubSubSubscriptionName,
                                           importServicePubSubTopic,
                                           mockImportServiceDAO,
                                           avroUpsertBucketName,
                                           batchSize
                                          ))


    val notificationDAO = new PubSubNotificationDAO(pubSubDao, "test-notification-topic")

    implicit val patienceConfig = PatienceConfig(timeout = 1 second)

    // create file in google storage
    val contents = """[{"name": "avro-entity", "entityType": "test-type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "avro-attribute", "addUpdateAttribute": "foo"}]}]"""
    googleStorage.createBlob(GcsBucketName("fake-bucket"), GcsBlobName("fake-file"), contents.getBytes()) //change bucket name - test config

    // create the two topics
    pubSubDao.createTopic("request-topic")
    pubSubDao.createTopic("status-topic")

    // post a notification on the request topic
    pubSubDao.publishMessages("request-topic", Seq("here's a sample message"))

    // check in db if entities are there
    val entity = Entity("avro-entity", "test-type", Map(AttributeName.withDefaultNS("avro-attribute") -> AttributeString("foo")))
    assertResult(Some(entity)) { runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), "test-type", "avro-entity")) }

    // check in status topic if correct message was posted
    assert(pubSubDao.receivedMessage("status-topic", "here's a sample message", 1))

  }
}