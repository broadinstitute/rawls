package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.ReferenceTypeEnum
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.DataReferenceName
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Failure

class DataRepoEntityProviderBuilderSpec extends FlatSpec with DataRepoEntityProviderSpecSupport with TestDriverComponent with Matchers {
  override implicit val executionContext = TestExecutionContext.testExecutionContext
  val defaultEntityRequestArguments = EntityRequestArguments(workspace, userInfo, Some(DataReferenceName("referenceName")))

  behavior of "DataRepoEntityProviderBuilder.build()"

  it should "build successfully with a valid workspace, userInfo, and DataReferenceName" in {
    val builder = createTestBuilder()
    val result = builder.build(defaultEntityRequestArguments)
    assert(result.isSuccess, result)
  }

  it should "error if data reference not defined" in {
    val builder = createTestBuilder()

    val ex = intercept[DataEntityException] { builder.build(defaultEntityRequestArguments.copy(dataReference = None)).get }
    assertResult( "data reference must be defined for this provider" ) { ex.getMessage }
  }

  it should "bubble up error if workspace manager errors" in {
    val expectedException = new bio.terra.workspace.client.ApiException("whoops 1")
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Left(expectedException)))

    builder.build(defaultEntityRequestArguments) shouldBe Failure(expectedException)
  }

  it should "bubble up error if data repo errors" in {
    val expectedException = new bio.terra.datarepo.client.ApiException("whoops 2")
    val builder = createTestBuilder(
      dataRepoDAO = new SpecDataRepoDAO(Left(expectedException)))

    builder.build(defaultEntityRequestArguments) shouldBe Failure(expectedException)
  }

  it should "bubble up snapshot does not exist error as DataEntityException" in {
    val builder = createTestBuilder(
      dataRepoDAO = new SpecDataRepoDAO(Left(new bio.terra.datarepo.client.ApiException(StatusCodes.NotFound.intValue, "not found"))))

    intercept[DataEntityException] { builder.build(defaultEntityRequestArguments).get }
  }

  it should "bubble up lookupSnapshotForName error" in {
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(referenceType = null)))
    )

    val ex = intercept[DataEntityException] { builder.build(defaultEntityRequestArguments).get }
    assertResult( s"Reference type value for referenceName is not of type ${ReferenceTypeEnum.DATA_REPO_SNAPSHOT.getValue}" ) { ex.getMessage }
  }

  behavior of "DataRepoEntityProviderBuilder.lookupSnapshotForName()"

  it should "return snapshot id in the golden path" in {
    val builder = createTestBuilder()
    val actual = builder.lookupSnapshotForName(DataReferenceName("foo"), defaultEntityRequestArguments)
    assertResult(UUID.fromString(snapshot)) { actual }
  }

  it should "bubble up error if workspace manager errors" in  {
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Left(new bio.terra.workspace.client.ApiException("whoops 1"))))

    val ex = intercept[bio.terra.workspace.client.ApiException] { builder.lookupSnapshotForName(DataReferenceName("foo"), defaultEntityRequestArguments) }
    assertResult("whoops 1") { ex.getMessage }
  }

  it should "error if workspace manager returns a non-snapshot reference type" in {
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(referenceType = null)))
    )

    val ex = intercept[DataEntityException] { builder.lookupSnapshotForName(DataReferenceName("foo"), defaultEntityRequestArguments) }
    assertResult( s"Reference type value for foo is not of type ${ReferenceTypeEnum.DATA_REPO_SNAPSHOT.getValue}" ) { ex.getMessage }
  }

  it should "error if workspace manager reference json `instanceName` value does not match DataRepoDAO's base url" in {
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refInstanceName = "this is wrong")))
    )

    val ex = intercept[DataEntityException] { builder.lookupSnapshotForName(DataReferenceName("foo"), defaultEntityRequestArguments) }
    assertResult("Reference value for foo contains an unexpected instance name value") { ex.getMessage }
  }

  it should "error if workspace manager reference json `snapshot` value is not a valid UUID" in {
    val builder = createTestBuilder(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refSnapshot = "this is not a uuid")))
    )

    val ex = intercept[DataEntityException] { builder.lookupSnapshotForName(DataReferenceName("foo"), defaultEntityRequestArguments) }
    assertResult("Reference value for foo contains an unexpected snapshot value") { ex.getMessage }
  }
}
