package org.broadinstitute.dsde.rawls.expressions

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, ReadWriteAction, TestDriverComponent, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickWorkspaceContext, RawlsTransaction, WorkspaceContext}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.FunSuite
import org.scalatest.TryValues
import slick.dbio

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.Seq
import scala.util.{Random, Failure, Success}

/**
 * Created by abaumann on 5/21/15.
 */
class SlickSimpleExpressionParserTest extends FunSuite with TestDriverComponent {
  import driver.api._

  def withTestWorkspace(testCode: (SlickWorkspaceContext) => Any): Unit = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { workspaceContext =>
        testCode(workspaceContext)
      }
    }
  }

  test("simple attribute expression") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator
      assertResult(Seq(AttributeString("normal"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type").get)
      }

      assertResult(Seq(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.type").get)
      }

      assertResult(Seq()) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset_empty", "this.samples").get)
      }

      assertResult(Seq(AttributeString("a"), AttributeString("b"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.thingies").get)
      }
    }
  }

  test("consistent sorting") {
    withTestWorkspace { workspaceContext =>
      // tests that expressions return consistently sorted results for expressions that differ only in the last position
      // note that the attribute tested has the same value of the name.

      createEntityStructure(workspaceContext)

      val evaluator = expressionEvaluator

      val nameResults = runAndWait(evaluator.evalFinalAttribute(workspaceContext, "a", "2", "this.bs.cs.name").get)
      val attrResults = runAndWait(evaluator.evalFinalAttribute(workspaceContext, "a", "2", "this.bs.cs.attr").get)
      val entityResults = runAndWait(evaluator.evalFinalEntity(workspaceContext, "a", "2", "this.bs.cs").get)

      assert(nameResults.nonEmpty, "name expression returns empty")
      assert(attrResults.nonEmpty, "attribute expression returns empty")
      assert(entityResults.nonEmpty, "entity expression returns empty")

      assertResult(nameResults) { attrResults }
      assertResult(nameResults) { entityResults.map(e => AttributeString(e.name)) }

      val wsNameResults = runAndWait(evaluator.evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.name").get)
      val wsAttrResults = runAndWait(evaluator.evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.attr").get)
      val wsEntityResults = runAndWait(evaluator.evalFinalEntity(workspaceContext, "a", "2", "workspace.as.bs.cs").get)

      assert(wsNameResults.nonEmpty, "name expression returns empty")
      assert(wsAttrResults.nonEmpty, "attribute expression returns empty")
      assert(wsEntityResults.nonEmpty, "entity expression returns empty")

      assertResult(wsNameResults) { wsAttrResults }
      assertResult(wsNameResults) { wsEntityResults.map(e => AttributeString(e.name)) }
    }

    def createEntities(entityType: String, entitiesNameAndAttributes: IndexedSeq[(String, Map[String, Attribute])], wsc: SlickWorkspaceContext): IndexedSeq[AttributeEntityReference] = {
      val saveActions = for ((nameAndAttributes, index) <- entitiesNameAndAttributes.zipWithIndex) yield {
        entityQuery.save(wsc, Entity(nameAndAttributes._1, entityType, nameAndAttributes._2))
      }

      // save in random order
      runAndWait(DBIO.sequence(Random.shuffle(saveActions)))

      entitiesNameAndAttributes.map { case (name, _) => AttributeEntityReference(entityType, name)}
    }

    def createEntityStructure(wsc: SlickWorkspaceContext) = {
      val aAttributes = for (a <- 0 until 5) yield {
        val bAttributes = for (b <- 0 until 5) yield {
          val cAttributes = for (c <- (0 until 20)) yield {

            (s"${a}_${b}_${c}", Map("attr" -> AttributeString(s"${a}_${b}_${c}")))
          }

          (s"${a}_${b}", Map("cs" -> AttributeEntityReferenceList(createEntities("c", cAttributes, wsc))))
        }

        (s"$a", Map("bs" -> AttributeEntityReferenceList(createEntities("b", bAttributes, wsc))))
      }

      val as = createEntities("a", aAttributes, wsc)

      runAndWait(workspaceQuery.save(wsc.workspace.copy(attributes = wsc.workspace.attributes + ("as" -> AttributeEntityReferenceList(as)))))
    }
  }

  test("string literals") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator
      assertResult(Seq(AttributeString("str")), "(simple string failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """"str"""").get)
      }

      assertResult(Seq(AttributeString(" str")), "(string with space failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """" str"""").get)
      }

      assertResult(Seq(AttributeString("\"str")), "(string with quote in it failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """""str"""").get)
      }

      assertResult(Seq(AttributeString("\"st\"r")), "(string with two quotes in it failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """""st"r"""").get)
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """this."foo"""").isFailure
      }
      assert {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """that."foo"""").isFailure
      }

      assertResult(Seq(AttributeString("42")), "(quoted numeral should have parsed as string)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """"42"""").get)
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """unquoted""").isFailure
      }
      assert {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """foo"bar"""").isFailure
      }
    }
  }

  def expressionEvaluator = {
    new SlickExpressionEvaluator(this)
  }

  test("numeric literals") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator
      assertResult(Seq(AttributeNumber(42)), "(integer failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """42""").get)
      }

      assertResult(Seq(AttributeNumber(-4.2)), "(decimal failed)") {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", """-4.2""").get)
      }
    }
  }

  test("reserved attribute expression") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator
      assertResult(Seq(AttributeString("sample1"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.name").get)
      }

      assertResult(Seq(AttributeString("Sample"), AttributeString("Sample"), AttributeString("Sample"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.entityType").get)
      }

      assertResult(Seq(AttributeString(workspaceContext.workspace.name))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.name").get)
      }

      intercept[RawlsException] {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.entityType").get)
      }
    }
  }

  test("workspace attribute expression") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator

      assertResult(Seq(testData.wsAttrs.get("string").get)) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.string").get)
      }

      assertResult(Seq(testData.wsAttrs.get("number").get)) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.number").get)
      }

      assertResult(Seq(testData.sample1.attributes.get("type").get)) {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref.type").get)
      }

      assertResult(Seq()) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.empty").get)
      }

      assertResult(Seq(AttributeString("another string"), AttributeString("true"))) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.values").get)
      }

      assertResult(Seq()) {
        runAndWait(evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.this_attribute_is_not_present").get)
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace").isFailure
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.").isFailure
      }

// https://broadinstitute.atlassian.net/browse/GAWB-349 figure out how to add this back in one day
//      assert {
//        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.missing.also_missing").isFailure
//      }

      assert {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref.").isFailure
      }
    }
  }

  test("simple entity expression") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator

      assertResult(Seq(testData.sample2)) {
        runAndWait(evaluator.evalFinalEntity(workspaceContext, "Pair", "pair1", "this.case").get)
      }

      assertResult(Seq(testData.sample1, testData.sample2, testData.sample3)) {
        runAndWait(evaluator.evalFinalEntity(workspaceContext, "Individual", "indiv1", "this.sset.samples").get)
      }

      assertResult(Seq(testData.pair1)) {
        runAndWait(evaluator.evalFinalEntity(workspaceContext, "Pair", "pair1", "this").get)
      }
    }
  }

  test("workspace entity expression") {
    withTestWorkspace { workspaceContext =>
      val evaluator = expressionEvaluator

      assertResult(Seq(testData.sample1)) {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evaluator.evalFinalEntity(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref").get)
      }

      assertResult(Seq(testData.sample2)) {
        val reflist = AttributeEntityReferenceList(Seq(AttributeEntityReference("Sample", "sample2")))
        val attributesPlusReference = testData.workspace.attributes + ("samplerefs" -> reflist)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evaluator.evalFinalEntity(workspaceContext, "dummy text", "dummy text", "workspace.samplerefs").get)
      }
    }
  }

  test("output expressions") {
    withTestWorkspace { workspaceContext =>
      val parser = new ExpressionParser
      assert(parser.parseOutputExpr("this.attribute").isSuccess, "this.attribute should parse correctly" )
      assert(parser.parseOutputExpr("this..attribute").isFailure, "this..attribute should not parse correctly" )
      assert(parser.parseOutputExpr("this.chained.expression").isFailure, "this.chained.expression should not parse correctly" )

      assert(parser.parseOutputExpr("workspace.attribute").isSuccess, "workspace.attribute should parse correctly" )
      assert(parser.parseOutputExpr("workspace..attribute").isFailure, "workspace..attribute should not parse correctly" )
      assert(parser.parseOutputExpr("workspace.chained.expression").isFailure, "workspace.chained.expression should not parse correctly" )
      
      assert(parser.parseOutputExpr("bonk.attribute").isFailure, "bonk.attribute should not parse correctly" )
    }
  }
}
