package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity, EntityQuery, SortDirections, WorkspaceFieldSpecs}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CaseSensitivitySpec extends AnyFreeSpec with Matchers with TestDriverComponent with ScalaFutures {

  val exemplarTypes = Set("cat", "Cat", "CAT", "dog", "rat")

  // create three entities for each type in our list.
  // Note that we intentionally reuse entity names, to allow tests against type+name to distinguish only by type
  val exemplarData = exemplarTypes.toSeq.zipWithIndex flatMap {
    case (typeName, index) =>
      Seq(
        Entity("001", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity("002", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity("003", typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
      )
  }

  "LocalEntityProvider case-sensitivity" - {
    "for entity types" - {
      "should return all types in uncached metadata requests" in withMinimalTestDatabase { dataSource: SlickDataSource =>
        // save exemplar data
        runAndWait(entityQuery.save(minimalTestData.workspace, exemplarData))
        // get provider
        val provider = new LocalEntityProvider(minimalTestData.workspace, slickDataSource, false, "metricsBaseName")
        // get metadata
        val metadata = provider.entityTypeMetadata(false).futureValue
        metadata.keySet shouldBe exemplarTypes
      }

      "should return all types in cached metadata requests" is (pending)

      "should list all entities only for target type" is (pending)

      "should respect case for get-entity" is (pending)

      "should respect case for update-entity" is (pending)

      "should respect case for expression evaluation" is (pending)

      "should respect case for rename entity" is (pending)

      "should only delete-all target type" is (pending)

      "should only delete all columns from target type" is (pending)

      "should respect case for delete specified entities" is (pending)

      "should only rename target type" is (pending)

      "should only rename attribute within target type" is (pending)

      "creating an entity reference respects case" is (pending)

      "batchUpsert respects case" is (pending)

      "batchUpdate respects case" is (pending)

      exemplarTypes foreach { typeUnderTest =>
        s"should only query target type [$typeUnderTest]"  in withMinimalTestDatabase { dataSource: SlickDataSource =>
          // save exemplar data
          runAndWait(entityQuery.save(minimalTestData.workspace, exemplarData))
          // get provider
          val provider = new LocalEntityProvider(minimalTestData.workspace, slickDataSource, false, "metricsBaseName")
          // get results for one specific type
          val queryCriteria = EntityQuery(1, exemplarData.size, "name", SortDirections.Ascending, None, WorkspaceFieldSpecs(None))
          val metadata = provider.queryEntities(typeUnderTest, queryCriteria).futureValue
          // extract distinct entity types from results
          val typesFromResults = metadata.results.map(_.entityType).distinct
          typesFromResults.toSet shouldBe Set(typeUnderTest)
        }
      }

    }
  }
}
