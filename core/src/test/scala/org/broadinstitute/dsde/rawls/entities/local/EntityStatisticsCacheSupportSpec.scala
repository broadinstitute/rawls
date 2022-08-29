package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  EntityRecordWithInlineAttributes,
  TestDriverComponentWithFlatSpecAndMatchers
}
import org.broadinstitute.dsde.rawls.model.{EntityTypeMetadata, RawlsRequestContext, Workspace}

import scala.concurrent.ExecutionContext

class CacheSupport(val dataSource: SlickDataSource, val workspaceContext: Workspace)
    extends EntityStatisticsCacheSupport {
  implicit override protected val executionContext: ExecutionContext = ExecutionContext.global
  val workbenchMetricBaseName: String = "test_metric_base_name"
}

class EntityStatisticsCacheSupportSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {

  import driver.api._

  behavior of "EntityStatisticsCacheSupport"

  it should "list all entity type metadata" in withDefaultTestDatabase {
    withWorkspaceContext(testData.workspace) { context =>
      val cacheSupport = new CacheSupport(slickDataSource, context)

      val desiredTypeMetadata = Map[String, EntityTypeMetadata](
        "Sample" -> EntityTypeMetadata(
          8,
          "sample_id",
          Seq("type", "whatsit", "thingies", "quot", "somefoo", "tumortype", "confused", "cycle", "foo_id")
        ),
        "Aliquot" -> EntityTypeMetadata(2, "aliquot_id", Seq()),
        "Pair" -> EntityTypeMetadata(2, "pair_id", Seq("case", "control", "whatsit")),
        "SampleSet" -> EntityTypeMetadata(5, "sampleset_id", Seq("samples", "hasSamples")),
        "PairSet" -> EntityTypeMetadata(1, "pairset_id", Seq("pairs")),
        "Individual" -> EntityTypeMetadata(2, "individual_id", Seq("sset"))
      )

      // assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
      // so we test the existence of all keys correctly here...
      val testTypeMetadata = runAndWait(
        cacheSupport.calculateMetadataResponse(slickDataSource.dataAccess,
                                               countsFromCache = false,
                                               attributesFromCache = false,
                                               parentContext = RawlsRequestContext(userInfo)
        )
      )
      assertSameElements(testTypeMetadata.keys, desiredTypeMetadata.keys)

      testTypeMetadata foreach { case (eType, testMetadata) =>
        val desiredMetadata = desiredTypeMetadata(eType)

        // ...and test that count and the list of attribute names are correct here.
        assert(testMetadata.count == desiredMetadata.count)
        assertSameElements(testMetadata.attributeNames, desiredMetadata.attributeNames)
      }
    }
  }

  // GAWB-870
  val testWorkspace = new EmptyWorkspace
  it should "list all entity type metadata when all_attribute_values is null" in withCustomTestDatabase(testWorkspace) {
    dataSource =>
      withWorkspaceContext(testWorkspace.workspace) { context =>
        val cacheSupport = new CacheSupport(slickDataSource, context)

        val id1 = 1
        val id2 = 2 // arbitrary

        // count distinct misses rows with null columns, like this one
        runAndWait(
          entityQueryWithInlineAttributes += EntityRecordWithInlineAttributes(id1,
                                                                              "test1",
                                                                              "null_attrs_type",
                                                                              context.workspaceIdAsUUID,
                                                                              0,
                                                                              None,
                                                                              deleted = false,
                                                                              None
          )
        )

        runAndWait(
          entityQueryWithInlineAttributes += EntityRecordWithInlineAttributes(id2,
                                                                              "test2",
                                                                              "blank_attrs_type",
                                                                              context.workspaceIdAsUUID,
                                                                              0,
                                                                              Some(""),
                                                                              deleted = false,
                                                                              None
          )
        )

        val desiredTypeMetadata = Map[String, EntityTypeMetadata](
          "null_attrs_type" -> EntityTypeMetadata(1, "null_attrs_type_id", Seq()),
          "blank_attrs_type" -> EntityTypeMetadata(1, "blank_attrs_type", Seq())
        )

        // assertSameElements is fine with out-of-order keys but isn't find with out-of-order interable-type values
        // so we test the existence of all keys correctly here...
        val testTypeMetadata = runAndWait(
          cacheSupport.calculateMetadataResponse(dataSource.dataAccess,
                                                 countsFromCache = false,
                                                 attributesFromCache = false,
                                                 parentContext = RawlsRequestContext(userInfo)
          )
        )
        assertSameElements(testTypeMetadata.keys, desiredTypeMetadata.keys)

        testTypeMetadata foreach { case (eType, testMetadata) =>
          val desiredMetadata = desiredTypeMetadata(eType)

          // ...and test that count and the list of attribute names are correct here.
          assert(testMetadata.count == desiredMetadata.count)
          assertSameElements(testMetadata.attributeNames, desiredMetadata.attributeNames)
        }
      }
  }

}
