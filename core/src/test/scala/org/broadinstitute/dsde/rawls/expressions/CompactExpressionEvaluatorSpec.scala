package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityQuery
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar.mock

class CompactExpressionEvaluatorSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  val compactEntityRepository = mock[CompactEntityRepository]
  val mockQueries = mock[CompactEntityQuery]
  when(compactEntityRepository.queries).thenReturn(mockQueries)

  val compactExpressionEvaluator = new CompactExpressionEvaluator(compactEntityRepository)

  // Note: this is also essentially a test for CompactEvaluateVisitor
  "parseLookups" should "generate correct lookups" in {
    val straightForwardTests =
      Table(
        ("input", "result"),
        ("this.type", List(AttributeLookup(List(), "type"))),
        ("blah", List()),
        ("\"blah\"", List()),
        ("workspace.string", List(AttributeLookup(List(), "string")))
      )

    forAll(straightForwardTests) { (input, result) =>
      compactExpressionEvaluator.parseLookups(input) shouldBe result
    }

    val relationTests =
      Table(
        ("input", "getText", "attributeName"),
        ("this.samples.type", "samples.", "type"),
        ("workspace.sample1ref.type", "sample1ref.", "type") // TODO implement
      )

    forAll(relationTests) { (input, getText, attributeName) =>
      val result: Seq[AttributeLookup] = compactExpressionEvaluator.parseLookups(input)
      result.size shouldBe 1
      result(0).relations.size shouldBe 1
      result(0).relations(0).getText shouldBe getText
      result(0).attributeName shouldBe attributeName
    }
  }

}
