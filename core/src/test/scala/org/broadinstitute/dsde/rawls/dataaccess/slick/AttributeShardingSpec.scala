package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils

import java.util.UUID

class AttributeShardingSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with AttributeComponent
    with RawlsTestUtils {

  behavior of "AttributeComponent sharding functionality"

  // map of input -> expected
  // assumes 32 shards
  val uuidCases = Map(
    "00000000-0000-0000-0000-000000000000" -> "00_03",
    "01000000-0000-0000-0000-000000000000" -> "00_03",
    "02000000-0000-0000-0000-000000000000" -> "00_03",
    "03000000-0000-0000-0000-000000000000" -> "00_03",
    "04000000-0000-0000-0000-000000000000" -> "04_07",
    "05000000-0000-0000-0000-000000000000" -> "04_07",
    "06000000-0000-0000-0000-000000000000" -> "04_07",
    "07000000-0000-0000-0000-000000000000" -> "04_07",
    "08000000-0000-0000-0000-000000000000" -> "08_0b",
    "09000000-0000-0000-0000-000000000000" -> "08_0b",
    "0a000000-0000-0000-0000-000000000000" -> "08_0b",
    "0b000000-0000-0000-0000-000000000000" -> "08_0b",
    "0c000000-0000-0000-0000-000000000000" -> "0c_0f",
    "0d000000-0000-0000-0000-000000000000" -> "0c_0f",
    "0e000000-0000-0000-0000-000000000000" -> "0c_0f",
    "0f000000-0000-0000-0000-000000000000" -> "0c_0f",
    "12010101-0000-0000-0000-000000000000" -> "10_13",
    "15010101-0000-0000-0000-000000000000" -> "14_17",
    "19010101-0000-0000-0000-000000000000" -> "18_1b",
    "1d010101-0000-0000-0000-000000000000" -> "1c_1f",
    "9a010101-0000-0000-0000-000000000000" -> "98_9b",
    "9afefefe-fefe-fefe-fefe-fefefefefefe" -> "98_9b",
    "fffefefe-fefe-fefe-fefe-fefefefefefe" -> "fc_ff"
  )

  uuidCases foreach { case (uuidString, expectedShardId) =>
    it should s"calculate shardId for UUID('$uuidString') correctly" in {
      assertResult(expectedShardId)(determineShard(UUID.fromString(uuidString)))
    }
  }

}
