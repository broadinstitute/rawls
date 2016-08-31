package org.broadinstitute.dsde.rawls.expressions


import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.Seq
import scala.util.{Random, Success => TrySuccess}


/**
 * Created by abaumann on 5/21/15.
 */
class SlickSimpleExpressionParserTest extends FunSuite with TestDriverComponent {
  import driver.api._

  def withTestWorkspace[T](testCode: (SlickWorkspaceContext) => T): T = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { workspaceContext =>
        testCode(workspaceContext)
      }
    }
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, expression: String) = {
    SlickExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, entityType, entityName) { evaluator =>
      evaluator.evalFinalEntity(workspaceContext, expression)
    }
  }

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, expression: String) = {
    entityQuery.findEntityByName(workspaceContext.workspaceId, entityType, entityName).result flatMap { entityRec =>
      SlickExpressionEvaluator.withNewExpressionEvaluator(this, entityRec) { evaluator =>
        evaluator.evalFinalAttribute(workspaceContext, expression)
      }
    }
  }

  test("withNewExpressionEvaluator lookups") {
    withTestWorkspace { workspaceContext =>

      //Single sample lookup works
      assertResult("sample1") {
        runAndWait(
          SlickExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "sample1") { evaluator =>
            DBIO.successful(evaluator.rootEntities.head.name)
        })
      }

      //Nonexistent sample lookup fails
      intercept[RawlsException] {
        runAndWait(
          SlickExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "nonexistent") { evaluator =>
            DBIO.successful(evaluator.rootEntities.head.name)
          })
      }
    }
  }

  test("ensure root entity temp table was dropped") {
    withTestWorkspace { workspaceContext =>
      intercept[MySQLSyntaxErrorException] {
        runAndWait({
          evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type")
          exprEvalQuery.result
        })
      }
    }
  }

  test("single attribute expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("normal"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.type"))
      }

      assertResult(Map("sset_empty" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset_empty", "this.samples"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("a"), AttributeString("b"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.thingies"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.nonexistent"))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "nonsensical_expression"))
      }
    }
  }

  test("the same expression on multiple inputs") {

    withTestWorkspace { workspaceContext =>
      val allTheTypes = Map(
        "sample1" -> TrySuccess(Seq(AttributeString("normal"))),
        "sample2" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample3" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample4" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample5" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample6" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample7" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample8" -> TrySuccess(Seq(AttributeString("tumor"))))

      assertResult(allTheTypes) { runAndWait(
        entityQuery.findEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap { ents =>
          SlickExpressionEvaluator.withNewExpressionEvaluator(this, ents) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, "this.type")
          }
        })
      }

      val allTheTumorTypes = Map(
        "sample1" -> TrySuccess(Seq()),
        "sample2" -> TrySuccess(Seq(AttributeString("LUSC"))),
        "sample3" -> TrySuccess(Seq(AttributeString("LUSC"))),
        "sample4" -> TrySuccess(Seq()),
        "sample5" -> TrySuccess(Seq()),
        "sample6" -> TrySuccess(Seq()),
        "sample7" -> TrySuccess(Seq()),
        "sample8" -> TrySuccess(Seq()))

      assertResult(allTheTumorTypes) { runAndWait(
        entityQuery.findEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap { ents =>
          SlickExpressionEvaluator.withNewExpressionEvaluator(this, ents) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, "this.tumortype")
          }
        })
      }
    }
  }

  test("consistent sorting") {
    withTestWorkspace { workspaceContext =>
      // tests that expressions return consistently sorted results for expressions that differ only in the last position
      // note that the attribute tested has the same value of the name.

      createEntityStructure(workspaceContext)

      val nameResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "this.bs.cs.name"))
      val attrResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "this.bs.cs.attr"))
      val entityResults = runAndWait(evalFinalEntity(workspaceContext, "a", "2", "this.bs.cs"))

      assert(nameResults.nonEmpty, "name expression returns empty")
      assert(attrResults.nonEmpty, "attribute expression returns empty")
      assert(entityResults.nonEmpty, "entity expression returns empty")

      assertResult(nameResults) { attrResults }
      assertResult(nameResults) { Map( "2" -> TrySuccess(entityResults.map(e => AttributeString(e.name)))) }

      val wsNameResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.name"))
      val wsAttrResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.attr"))
      val wsEntityResults = runAndWait(evalFinalEntity(workspaceContext, "a", "2", "workspace.as.bs.cs"))

      assert(wsNameResults.nonEmpty, "name expression returns empty")
      assert(wsAttrResults.nonEmpty, "attribute expression returns empty")
      assert(wsEntityResults.nonEmpty, "entity expression returns empty")

      assertResult(wsNameResults) { wsAttrResults }
      assertResult(wsNameResults) { Map( "2" -> TrySuccess( wsEntityResults.map(e => AttributeString(e.name)))) }
    }

    def createEntities(entityType: String, entitiesNameAndAttributes: IndexedSeq[(String, AttributeMap)], wsc: SlickWorkspaceContext): IndexedSeq[AttributeEntityReference] = {
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

            (s"${a}_${b}_${c}", Map(DefaultAttributeName("attr") -> AttributeString(s"${a}_${b}_${c}")))
          }

          (s"${a}_${b}", Map(DefaultAttributeName("cs") -> AttributeEntityReferenceList(createEntities("c", cAttributes, wsc))))
        }

        (s"$a", Map(DefaultAttributeName("bs") -> AttributeEntityReferenceList(createEntities("b", bAttributes, wsc))))
      }

      val as = createEntities("a", aAttributes, wsc)

      runAndWait(workspaceQuery.save(wsc.workspace.copy(attributes = wsc.workspace.attributes + (DefaultAttributeName("as") -> AttributeEntityReferenceList(as)))))
    }
  }

  test("string literals") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("str")))), "(simple string failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """"str""""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString(" str")))), "(string with space failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """" str""""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("\"str")))), "(string with quote in it failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """""str""""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("\"st\"r")))), "(string with two quotes in it failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """""st"r""""))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """this."foo""""))
      }
      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """that."foo""""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("42")))), "(quoted numeral should have parsed as string)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """"42""""))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """unquoted"""))
      }
      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """foo"bar""""))
      }
    }
  }

  test("numeric literals") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" ->TrySuccess(Seq(AttributeNumber(42)))), "(integer failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """42"""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeNumber(-4.2)))), "(decimal failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """-4.2"""))
      }
    }
  }

  test("reserved attribute expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("sample1"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.name"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString("Sample"), AttributeString("Sample"), AttributeString("Sample"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.entityType"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString(workspaceContext.workspace.name))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.name"))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.entityType"))
      }
    }
  }

  test("workspace attribute expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(Seq(testData.wsAttrs.get(DefaultAttributeName("string")).get)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.string"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(testData.wsAttrs.get(DefaultAttributeName("number")).get)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.number"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(testData.sample1.attributes.get(DefaultAttributeName("type")).get)))) {
        val attributesPlusReference = testData.workspace.attributes + (DefaultAttributeName("sample1ref") -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.sample1ref.type"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.empty"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("another string"), AttributeString("true"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.values"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.this_attribute_is_not_present"))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace"))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace."))
      }

// https://broadinstitute.atlassian.net/browse/GAWB-349 figure out how to add this back in one day
//      assert {
//        evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.missing.also_missing").isFailure
//      }

      intercept[RawlsException] {
        val attributesPlusReference = testData.workspace.attributes + (DefaultAttributeName("sample1ref") -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.sample1ref."))
      }
    }
  }

  test("simple entity expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Set("sample2")) {
        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "this.case")).map(_.name).toSet
      }

      assertResult(Set("sample1", "sample2", "sample3")) {
        runAndWait(evalFinalEntity(workspaceContext, "Individual", "indiv1", "this.sset.samples")).map(_.name).toSet
      }

      assertResult(Set("pair1")) {
        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "this")).map(_.name).toSet
      }
    }
  }

  test("workspace entity expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Set("sample1")) {
        val attributesPlusReference = testData.workspace.attributes + (DefaultAttributeName("sample1ref") -> AttributeEntityReference("Sample", "sample1"))
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.sample1ref")).map(_.name).toSet
      }

      assertResult(Set("sample2")) {
        val reflist = AttributeEntityReferenceList(Seq(AttributeEntityReference("Sample", "sample2")))
        val attributesPlusReference = testData.workspace.attributes + (DefaultAttributeName("samplerefs") -> reflist)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.samplerefs")).map(_.name).toSet
      }
    }
  }

  test("output expressions") {
    withTestWorkspace { workspaceContext =>
      assert(parseOutputExpr("this.attribute").isSuccess, "this.attribute should parse correctly" )
      assert(parseOutputExpr("this..attribute").isFailure, "this..attribute should not parse correctly" )
      assert(parseOutputExpr("this.chained.expression").isFailure, "this.chained.expression should not parse correctly" )

      assert(parseOutputExpr("workspace.attribute").isSuccess, "workspace.attribute should parse correctly" )
      assert(parseOutputExpr("workspace..attribute").isFailure, "workspace..attribute should not parse correctly" )
      assert(parseOutputExpr("workspace.chained.expression").isFailure, "workspace.chained.expression should not parse correctly" )
      
      assert(parseOutputExpr("bonk.attribute").isFailure, "bonk.attribute should not parse correctly" )
    }
  }
}
