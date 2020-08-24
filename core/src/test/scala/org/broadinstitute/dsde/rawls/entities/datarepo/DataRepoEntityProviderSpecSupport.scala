package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.{ColumnModel, RelationshipModel, SnapshotModel, TableModel}
import bio.terra.workspace.model.{CloningInstructionsEnum, DataReferenceDescription, DataRepoSnapshot, ReferenceTypeEnum}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.mock.{MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, RawlsUserEmail, UserInfo, Workspace}
import org.joda.time.DateTime

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

  // defaults for DataRepoEntityProviderConfig
  val maxInputsPerSubmission: Int = 1000
  val maxBigQueryResponseSizeBytes: Int = 500000

  /* A "factory" method to create a DataRepoEntityProvider, with defaults.
   * Individual unit tests should call this to reduce boilerplate.
   */
  def createTestProvider(snapshotModel: SnapshotModel = createSnapshotModel(),
                         samDAO: SamDAO = new MockSamDAO(slickDataSource),
                         bqFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory(),
                         entityRequestArguments: EntityRequestArguments = EntityRequestArguments(workspace, userInfo, Some(DataReferenceName("referenceName"))),
                         config: DataRepoEntityProviderConfig = DataRepoEntityProviderConfig(maxInputsPerSubmission, maxBigQueryResponseSizeBytes, 0)
                        ): DataRepoEntityProvider = {
    new DataRepoEntityProvider(snapshotModel, entityRequestArguments, samDAO, bqFactory, config)
  }

  def createTestBuilder(workspaceManagerDAO: WorkspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription())),
                        dataRepoDAO: SpecDataRepoDAO = new SpecDataRepoDAO(Right(createSnapshotModel())),
                        samDAO: SamDAO = new MockSamDAO(slickDataSource),
                        bqServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory(),
                        config: DataRepoEntityProviderConfig = DataRepoEntityProviderConfig(maxInputsPerSubmission, maxBigQueryResponseSizeBytes, 0)
                       ): DataRepoEntityProviderBuilder = {
    new DataRepoEntityProviderBuilder(workspaceManagerDAO, dataRepoDAO, samDAO, bqServiceFactory, config)
  }


  /* A "factory" method to create DataReferenceDescription objects, with defaults.
   * Allows callers to only specify the arguments they want to override.
   */
  def createDataRefDescription(name: String = "refName",
                               referenceType: ReferenceTypeEnum = ReferenceTypeEnum.DATA_REPO_SNAPSHOT,
                               referenceId: UUID = refId,
                               cloningInstructionsEnum: CloningInstructionsEnum = CloningInstructionsEnum.NOTHING,
                               credentialId: String = "",
                               workspaceId: UUID = wsId,
                               refInstanceName: String = dataRepoInstanceName,
                               refSnapshot: String = snapshot,
                               reference: Option[DataRepoSnapshot] = None
                              ): DataReferenceDescription = {

    val dataRepoReference = reference match {
      case Some(s) => s
      case None => new DataRepoSnapshot().instanceName(refInstanceName).snapshot(refSnapshot)
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
      .columns(List("datarepo_row_id", "integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava),
    new TableModel().name("table2").primaryKey(List("table2PK").asJava).rowCount(123)
      .columns(List("col2a", "col2b").map(new ColumnModel().name(_)).asJava),
    new TableModel().name("table3").primaryKey(List("compound","pk").asJava).rowCount(456)
      .columns(List("col3.1", "col3.2").map(new ColumnModel().name(_)).asJava)
  )

  /* A "factory" method to create SnapshotModel objects, with default.
   */
  def createSnapshotModel( tables: List[TableModel] = defaultTables, relationships: List[RelationshipModel] = List.empty): SnapshotModel =
    new SnapshotModel()
      .tables(tables.asJava)
      .relationships(relationships.asJava)
      .dataProject("unittest-dataproject")
      .name("unittest-name")
      .relationships(relationships.asJava)

  /**
   * Mock for WorkspaceManagerDAO that allows the caller to specify behavior for the getDataReferenceByName method.
   */
  class SpecWorkspaceManagerDAO(refByNameResponse:Either[Throwable, DataReferenceDescription]) extends MockWorkspaceManagerDAO {
    override def getDataReferenceByName(workspaceId: UUID, refType: ReferenceTypeEnum, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription =
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
