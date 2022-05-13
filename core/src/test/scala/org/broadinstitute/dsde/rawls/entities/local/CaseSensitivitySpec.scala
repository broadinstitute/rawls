package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CaseSensitivitySpec extends AnyFreeSpec with Matchers with TestDriverComponent with ScalaFutures {

  val exemplarTypes = Set("cat", "Cat", "CAT", "dog", "rat")

  val exemplarData = exemplarTypes.toSeq.zipWithIndex flatMap {
    case (typeName, index) =>
      Seq(
        Entity((index * 10).toString, typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity((index * 10 + 1).toString, typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
        Entity((index * 10 + 2).toString, typeName, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
      )
  }

  "LocalEntityProvider case-sensitivity" - {
    "for entity types" - {
      "should return all types in metadata requests" in withMinimalTestDatabase { dataSource: SlickDataSource =>
        // save exemplar data
        runAndWait(entityQuery.save(minimalTestData.workspace, exemplarData))
        // get provider
        val provider = new LocalEntityProvider(minimalTestData.workspace, slickDataSource, false, "metricsBaseName")
        // get metadata
        val metadata = provider.entityTypeMetadata(false).futureValue
        metadata.keySet shouldBe exemplarTypes
      }
      "should only delete target type" is (pending)
      "should only rename target type" is (pending)
      "should only query target type" is (pending)
    }
  }
}
