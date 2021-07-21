package org.broadinstitute.dsde.rawls.webservice

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.workspace.model.DataReferenceDescription
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{Blob, BlobInfo, Storage, StorageException}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.deltalayer.GcsStorageTestSupport
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

// This spec tests Delta Layer functionality from the entity APIs down.
// See also EntityApiServiceSpec for other coverage of entity APIs.
class EntityApiServiceDeltaLayerSpec extends ApiServiceSpec with GcsStorageTestSupport {

  implicit val actorSystem: ActorSystem = ActorSystem("EntityApiServiceDeltaLayerSpec")

  val bucket = GcsBucketName("unittest-bucket-name")

  // in-memory GCS Storage mock
  val storage = LocalStorageHelper.getOptions.getService

  // GCS Storage mock that will throw an exception
  val mockedException = new StorageException(418, "intentional unit test failure")
  val throwingStorage = mock[Storage]
  when(throwingStorage.writer(any[BlobInfo], any[BlobWriteOption]))
    .thenThrow(mockedException)

  case class TestApiServiceLocalGcsStorage(dataSource: SlickDataSource, storageImpl: Storage, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    // create in-memory local GCS storage instead of real cloud-based GCS storage
    val deltaLayerWriter = getGcsWriter(bucket, Some(storageImpl))

    // create EntityManager that will return a real GcsDeltaLayerWriter pointing at the in-memory local storage
    override val entityManager = EntityManager.defaultEntityManager(dataSource, workspaceManagerDAO, dataRepoDAO, samDAO, bigQueryServiceFactory, deltaLayerWriter, DataRepoEntityProviderConfig(100, 10, 0), testConf.getBoolean("entityStatisticsCache.enabled"))

    override val entityServiceConstructor: UserInfo => EntityService = EntityService.constructor(
      dataSource,
      samDAO,
      workbenchMetricBaseName = workbenchMetricBaseName,
      entityManager
    )
  }

  def withApiServices[T](dataSource: SlickDataSource, storageImpl: Storage)(testCode: TestApiServiceLocalGcsStorage => T): T = {
    val apiService = new TestApiServiceLocalGcsStorage(dataSource, storageImpl: Storage, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](storageImpl: Storage)(testCode: TestApiServiceLocalGcsStorage => T): T = {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource, storageImpl)(testCode)
    }
  }


  behavior of "Entity APIs for Delta Layer"

  it should "return 204 when batch upserting into Delta Layer succeeds" in withTestDataApiServices(storage) { services =>
    // at the start of the test, confirm that the storage impl does NOT contain a file that starts with our expected path.
    // since we can't predict the insertid, we can't verify the full path.
    val storageList = storage.list(bucket.value)
    assert(!storageList.getValues.asScala.toList.map(_.getName).exists(_.startsWith(s"workspace/${testData.workspace.workspaceId}/reference")))

    // add snapshot to workspace
    val defaultNamedSnapshotJson = httpJson(NamedDataRepoSnapshot(
      name = DataReferenceName("testsnap"),
      description = Some(DataReferenceDescriptionField("desc")),
      snapshotId = UUID.randomUUID()
    ))
    Post(s"${testData.workspace.path}/snapshots", defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        // assert the snapshot reference was created correctly
        assertResult(StatusCodes.Created) {
          status
        }
        // get the id of the reference we just created
        val refId = responseAs[DataReferenceDescription].getReferenceId
        // issue batchUpsert against snapshot reference
        val update1 = EntityUpdateDefinition(UUID.randomUUID().toString, "Sample", Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo"))))
        Post(s"${testData.workspace.path}/entities/batchUpsert?dataReference=testsnap", httpJson(Seq(update1))) ~>
          sealRoute(services.entityRoutes) ~>
          // assert 204 returned from batchUpsert
          check {
            assertResult(StatusCodes.NoContent) {
              status
            }
            // confirm that the storage impl contains a file that starts with our expected path.
            // since we can't predict the insertid, we can't verify the full path.
            // The LocalStorageHelper in-memory storage engine
            // seems to have some delays, so use eventually to allow for delays and retries
            eventually(timeout(Span(30, Seconds)), interval(Span(500, Millis))) {
              val storageList: List[Blob] = storage.list(bucket.value).getValues.asScala.toList
              storageList.map(_.getName).exists(_.startsWith(s"workspace/${testData.workspace.workspaceId}/reference/$refId/insert/"))
            }
          }
      }
  }

  it should "return 500 when batch upserting into Delta Layer encounters an error"  in withTestDataApiServices(throwingStorage) { services =>
    // add snapshot to workspace
    val defaultNamedSnapshotJson = httpJson(NamedDataRepoSnapshot(
      name = DataReferenceName("testsnap"),
      description = Some(DataReferenceDescriptionField("desc")),
      snapshotId = UUID.randomUUID()
    ))
    Post(s"${testData.workspace.path}/snapshots", defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        // assert the snapshot reference was created correctly
        assertResult(StatusCodes.Created) {
          status
        }
        // issue batchUpsert against snapshot reference
        val update1 = EntityUpdateDefinition(UUID.randomUUID().toString, "Sample", Seq(AddUpdateAttribute(AttributeName.withDefaultNS("newAttribute"), AttributeString("foo"))))
        Post(s"${testData.workspace.path}/entities/batchUpsert?dataReference=testsnap", httpJson(Seq(update1))) ~>
          sealRoute(services.entityRoutes) ~>
          // the underlying throwingStorage storage impl will throw an error; that error should be propagated up
          // to us here.
          check {
            assertResult(StatusCodes.ImATeapot) {
              status
            }
          }
      }
  }

}
