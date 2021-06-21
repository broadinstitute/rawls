package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils

import java.util.UUID

class AttributeShardingSpec extends TestDriverComponentWithFlatSpecAndMatchers with AttributeComponent with RawlsTestUtils {

  behavior of "AttributeComponent sharding functionality"

    // map of input -> expected
    // assumes 32 shards
    val uuidCases = Map(
      "00000000-0000-0000-0000-000000000000" -> "0_07",
      "01000000-0000-0000-0000-000000000000" -> "0_07",
      "06000000-0000-0000-0000-000000000000" -> "0_07",
      "07000000-0000-0000-0000-000000000000" -> "0_07",
      "08000000-0000-0000-0000-000000000000" -> "0_8f",
      "09000000-0000-0000-0000-000000000000" -> "0_8f",
      "0e000000-0000-0000-0000-000000000000" -> "0_8f",
      "0f000000-0000-0000-0000-000000000000" -> "0_8f",
      "12010101-0000-0000-0000-000000000000" -> "1_07",
      "15010101-0000-0000-0000-000000000000" -> "1_07",
      "19010101-0000-0000-0000-000000000000" -> "1_8f",
      "1d010101-0000-0000-0000-000000000000" -> "1_8f",
      "9a010101-0000-0000-0000-000000000000" -> "9_8f",
      "9afefefe-fefe-fefe-fefe-fefefefefefe" -> "9_8f",
      "fffefefe-fefe-fefe-fefe-fefefefefefe" -> "f_8f"
    )

  uuidCases foreach {
    case (uuidString, expectedShardId) =>
      it should s"calculate shardId for UUID('$uuidString') correctly" in {
        assertResult(expectedShardId) { determineShard(UUID.fromString(uuidString)) }
      }
  }




}
