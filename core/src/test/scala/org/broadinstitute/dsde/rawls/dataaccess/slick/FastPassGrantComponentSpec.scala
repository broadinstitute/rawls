package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, GcpResourceTypes, IamRoles, RawlsUserSubjectId}
import org.joda.time.DateTime

import java.sql.Timestamp
import java.util.UUID

class FastPassGrantComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {

  val id = UUID.randomUUID()
  val workpaceId = UUID.randomUUID()
  val expiration = DateTime.now().plusHours(2)

  val model = FastPassGrant(
    id.toString,
    workpaceId.toString,
    RawlsUserSubjectId("12345678"),
    GcpResourceTypes.Bucket,
    "my-bucket",
    IamRoles.TerraBucketReader,
    new Timestamp(expiration.getMillis)
  )

  val record = FastPassGrantRecord(
    id,
    workpaceId,
    "12345678",
    "bucket",
    "my-bucket",
    "terraBucketReader",
    new Timestamp(expiration.getMillis)
  )

  "FastPassGrantRecord" should "translate between FastPastGrants and FastPassGrantRecords" in {
    Seq(GcpResourceTypes.Bucket, GcpResourceTypes.Project).foreach { gcpResourceType =>
      val resourceTypeModel = model.copy(resourceType = gcpResourceType)
      val resourceTypeRecord = record.copy(resourceType = GcpResourceTypes.toName(gcpResourceType))
      Seq(
        IamRoles.RequesterPays,
        IamRoles.TerraBillingProjectOwner,
        IamRoles.TerraWorkspaceCanCompute,
        IamRoles.TerraWorkspaceNextflow,
        IamRoles.TerraBucketReader,
        IamRoles.TerraBucketWriter
      ).foreach { iamRole =>
        val testModel = resourceTypeModel.copy(roleName = iamRole)
        val testRecord = resourceTypeRecord.copy(roleName = IamRoles.toName(iamRole))
        FastPassGrantRecord.fromFastPassGrant(testModel) shouldBe testRecord
        FastPassGrantRecord.toFastPassGrant(testRecord) shouldBe testModel

      }
    }
  }

}
