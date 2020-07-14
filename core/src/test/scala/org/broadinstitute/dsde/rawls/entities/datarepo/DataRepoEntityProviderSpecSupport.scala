package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.{ColumnModel, SnapshotModel, TableModel}
import bio.terra.workspace.model.DataReferenceDescription
import bio.terra.workspace.model.DataReferenceDescription.{CloningInstructionsEnum, ReferenceTypeEnum}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, RawlsUserEmail, UserInfo, Workspace}
import org.joda.time.DateTime
import spray.json.{JsObject, JsString}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

trait DataRepoEntityProviderSpecSupport {

  val slickDataSource: SlickDataSource
  val userInfo: UserInfo
  implicit val executionContext: ExecutionContext

  // default values for some important attributes
  val wsId: UUID = UUID.randomUUID()
  val refId: UUID = UUID.randomUUID()
  val dataRepoInstanceName: String = "mock-instance-name"
  val snapshot: String = UUID.randomUUID().toString

  // default Workspace object, mostly irrelevant for DataRepoEntityProviderSpec but necessary to exist
  val workspace = new Workspace("namespace", "name", wsId.toString, "bucketName", None,
    DateTime.now(), DateTime.now(), "createdBy", Map.empty, false)

  /* A "factory" method to create a DataRepoEntityProvider, with defaults.
   * Individual unit tests should call this to reduce boilerplate.
   */
  def createTestProvider(workspaceManagerDAO: SpecWorkspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription())),
                         dataRepoDAO: SpecDataRepoDAO = new SpecDataRepoDAO(Right(createSnapshotModel())),
                         samDAO: SamDAO = new MockSamDAO(slickDataSource),
                         bqFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory(),
                         entityRequestArguments: EntityRequestArguments = EntityRequestArguments(workspace, userInfo, Some(DataReferenceName("referenceName")))
                        ): DataRepoEntityProvider = {
    new DataRepoEntityProvider(entityRequestArguments, workspaceManagerDAO, dataRepoDAO, samDAO, bqFactory)
  }



  /* A "factory" method to create DataReferenceDescription objects, with defaults.
   * Allows callers to only specify the arguments they want to override.
   */
  def createDataRefDescription(name: String = "refName",
                               referenceType: ReferenceTypeEnum = ReferenceTypeEnum.DATAREPOSNAPSHOT,
                               referenceId: UUID = refId,
                               cloningInstructionsEnum: CloningInstructionsEnum = CloningInstructionsEnum.NOTHING,
                               credentialId: String = "",
                               workspaceId: UUID = wsId,
                               refInstanceName: String = dataRepoInstanceName,
                               refSnapshot: String = snapshot,
                               refString: Option[String] = None
                              ): DataReferenceDescription = {

    val dataRepoReference = refString match {
      case Some(s) => s
      case None =>  JsObject.apply(("instanceName", JsString(refInstanceName)), ("snapshot", JsString(refSnapshot))).compactPrint
    }

    new DataReferenceDescription()
      .name(name)
      .referenceType(referenceType)
      .referenceId(referenceId)
      .cloningInstructions(cloningInstructionsEnum)
      .credentialId(credentialId)
      .reference(dataRepoReference)
      .workspaceId(workspaceId)
  }

  val defaultTables: List[TableModel] = List(
    new TableModel().name("table1").primaryKey(null).rowCount(0)
      .columns(List().map(new ColumnModel().name(_)).asJava),
    new TableModel().name("table2").primaryKey(List("table2PK").asJava).rowCount(123)
      .columns(List("col2.1", "col2.2").map(new ColumnModel().name(_)).asJava),
    new TableModel().name("table3").primaryKey(List("compound","pk").asJava).rowCount(456)
      .columns(List("col3.1", "col3.2").map(new ColumnModel().name(_)).asJava)
  )

  /* A "factory" method to create SnapshotModel objects, with default.
   */
  def createSnapshotModel( tables: List[TableModel] = defaultTables): SnapshotModel =
    new SnapshotModel().tables(tables.asJava)

  /**
   * Mock for WorkspaceManagerDAO that allows the caller to specify behavior for the getDataReferenceByName method.
   */
  class SpecWorkspaceManagerDAO(refByNameResponse:Either[Throwable, DataReferenceDescription]) extends MockWorkspaceManagerDAO {
    override def getDataReferenceByName(workspaceId: UUID, refType: String, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription =
      refByNameResponse match {
        case Left(t) => throw t
        case Right(ref) =>
          // N.B. we don't attempt to replicate the workspace manager's functionality of comparing the workspace id,
          // refName, refType, and so on. We just return what the unit test asked us to return. It is workspace manager's
          // responsibility to ensure its lookup logic is correct.
          ref
      }
  }

  /**
   * Mock for DataRepoDAO that allows the caller to specify behavior for the getSnapshot and getBaseURL methods.
   *  method.
   */
  class SpecDataRepoDAO(getSnapshotResponse:Either[Throwable, SnapshotModel], baseURL: String = dataRepoInstanceName) extends MockDataRepoDAO {

    override def getInstanceName: String = baseURL

    override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = getSnapshotResponse match {
      case Left(t) => throw t
      case Right(snap) => snap
    }
  }

  /**
   * Mock for DataRepoDAO that allows the caller to specify behavior for the getSnapshot and getBaseURL methods.
   *  method.
   */
  class SpecSamDAO(dataSource: SlickDataSource = slickDataSource,
                   petKeyForUserResponse: Either[Throwable, String]) extends MockSamDAO(dataSource) {
    override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = {
      petKeyForUserResponse match {
        case Left(t) => Future.failed(t)
        case Right(key) => Future.successful(key)
      }
    }
  }


}
