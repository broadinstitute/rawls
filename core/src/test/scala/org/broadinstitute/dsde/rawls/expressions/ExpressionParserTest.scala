package org.broadinstitute.dsde.rawls.expressions


import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ExprEvalRecord, TestDriverComponent}
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
class ExpressionParserTest extends FunSuite with TestDriverComponent {
  import driver.api._

  def withTestWorkspace[T](testCode: (SlickWorkspaceContext) => T): T = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { workspaceContext =>
        testCode(workspaceContext)
      }
    }
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, expression: String) = {
    ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, entityType, entityName) { evaluator =>
      evaluator.evalFinalEntity(workspaceContext, expression)
    }
  }

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, expression: String) = {
    entityQuery.findEntityByName(workspaceContext.workspaceId, entityType, entityName).result flatMap { entityRec =>
      ExpressionEvaluator.withNewExpressionEvaluator(this, entityRec) { evaluator =>
        evaluator.evalFinalAttribute(workspaceContext, expression)
      }
    }
  }

  test("withNewExpressionEvaluator lookups") {
    withTestWorkspace { workspaceContext =>

      //Single sample lookup works
      assertResult("sample1") {
        runAndWait(
          ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "sample1") { evaluator =>
            DBIO.successful(evaluator.rootEntities.head.name)
        })
      }

      //Nonexistent sample lookup fails
      intercept[RawlsException] {
        runAndWait(
          ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "nonexistent") { evaluator =>
            DBIO.successful(evaluator.rootEntities.head.name)
          })
      }
    }
  }

  test("ensure root entity temp table was cleared") {
    withTestWorkspace { workspaceContext =>
      assertResult(0) {
        runAndWait({
          evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type") andThen
            sql"select count(*) from EXPREVAL_SCRATCH".as[Int].head
        })
      }
    }
  }

  test("single attribute expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(AttributeString("normal")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type"))
      }

      assertResult(Map("sample8" -> TrySuccess(AttributeString("1029384756")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample8", "this.foo_id"))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeValueList(Seq(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor")))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.type"))
      }

      assertResult(Map("sset_empty" -> TrySuccess(AttributeValueEmptyList))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset_empty", "this.empty_list"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueList(Seq(AttributeString("a"), AttributeString("b")))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.thingies"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeNull))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.nonexistent"))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "nonsensical_expression"))
      }
    }
  }

  test("attribute expression that refers to an entity should except") {
    withTestWorkspace { workspaceContext =>
      val exprResult = runAndWait(evalFinalAttribute(workspaceContext, "Pair", "pair1", "this.case"))
      assert(exprResult("pair1").isFailure)
    }
  }

  test("library entity attribute expression") {
    withTestWorkspace { workspaceContext =>
      val libraryAttribute = Map(AttributeName("library", "book") -> AttributeString("arbitrary"))
      runAndWait(entityQuery.save(workspaceContext, Entity("sampleWithLibraryNamespaceAttribute", "Sample", libraryAttribute)))

      assertResult(Map("sampleWithLibraryNamespaceAttribute" -> TrySuccess(AttributeString("arbitrary")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sampleWithLibraryNamespaceAttribute", "this.library:book"))
      }

      assertResult(Map("sampleWithLibraryNamespaceAttribute" -> TrySuccess(AttributeNull))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sampleWithLibraryNamespaceAttribute", "this.library:checked_out_book"))
      }

      (1 to 3).foreach { num =>
        val ent = runAndWait(entityQuery.get(workspaceContext, "Sample", s"sample$num")).get
        val libraryAttribute = Map(AttributeName("library", "chapter") -> AttributeNumber(num))
        runAndWait(entityQuery.save(workspaceContext, ent.copy(attributes = libraryAttribute)))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3)))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.library:chapter"))
      }

      val resultsByType = runAndWait(entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap { ents =>
        ExpressionEvaluator.withNewExpressionEvaluator(this, ents) { evaluator =>
          evaluator.evalFinalAttribute(workspaceContext, "this.library:chapter")
        }
      })

      (1 to 3).foreach { num =>
        assertResult(TrySuccess(AttributeNumber(num))) {
          resultsByType(s"sample$num")
        }
      }

      // reserved attribute "name" works the same as it does in the default namespace

      assertResult(Map("sample1" -> TrySuccess(AttributeString("sample1")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.library:name"))
      }
    }
  }

  test("attribute names with two colons") {
    withTestWorkspace { workspaceContext =>
      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.a:b:c"))
      }
    }
  }

  test("the same expression on multiple inputs") {

    withTestWorkspace { workspaceContext =>
      val allTheTypes = Map(
        "sample1" -> TrySuccess(AttributeString("normal")),
        "sample2" -> TrySuccess(AttributeString("tumor")),
        "sample3" -> TrySuccess(AttributeString("tumor")),
        "sample4" -> TrySuccess(AttributeString("tumor")),
        "sample5" -> TrySuccess(AttributeString("tumor")),
        "sample6" -> TrySuccess(AttributeString("tumor")),
        "sample7" -> TrySuccess(AttributeString("tumor")),
        "sample8" -> TrySuccess(AttributeString("tumor")))

      assertResult(allTheTypes) { runAndWait(
        entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap { ents =>
          ExpressionEvaluator.withNewExpressionEvaluator(this, ents) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, "this.type")
          }
        })
      }

      val allTheTumorTypes = Map(
        "sample1" -> TrySuccess(AttributeNull),
        "sample2" -> TrySuccess(AttributeString("LUSC")),
        "sample3" -> TrySuccess(AttributeString("LUSC")),
        "sample4" -> TrySuccess(AttributeNull),
        "sample5" -> TrySuccess(AttributeNull),
        "sample6" -> TrySuccess(AttributeNull),
        "sample7" -> TrySuccess(AttributeNull),
        "sample8" -> TrySuccess(AttributeNull))

      assertResult(allTheTumorTypes) { runAndWait(
        entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap { ents =>
          ExpressionEvaluator.withNewExpressionEvaluator(this, ents) { evaluator =>
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
      assertResult(nameResults) { Map( "2" -> TrySuccess(AttributeValueList(entityResults.map(e => AttributeString(e.name)).toSeq))) }

      val wsNameResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.name"))
      val wsAttrResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.attr"))
      val wsEntityResults = runAndWait(evalFinalEntity(workspaceContext, "a", "2", "workspace.as.bs.cs"))

      assert(wsNameResults.nonEmpty, "name expression returns empty")
      assert(wsAttrResults.nonEmpty, "attribute expression returns empty")
      assert(wsEntityResults.nonEmpty, "entity expression returns empty")

      assertResult(wsNameResults) { wsAttrResults }
      assertResult(wsNameResults) { Map( "2" -> TrySuccess( AttributeValueList(wsEntityResults.map(e => AttributeString(e.name)).toSeq))) }
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

            (s"${a}_${b}_${c}", Map(AttributeName.withDefaultNS("attr") -> AttributeString(s"${a}_${b}_${c}")))
          }

          (s"${a}_${b}", Map(AttributeName.withDefaultNS("cs") -> AttributeEntityReferenceList(createEntities("c", cAttributes, wsc))))
        }

        (s"$a", Map(AttributeName.withDefaultNS("bs") -> AttributeEntityReferenceList(createEntities("b", bAttributes, wsc))))
      }

      val as = createEntities("a", aAttributes, wsc)

      runAndWait(workspaceQuery.save(wsc.workspace.copy(attributes = wsc.workspace.attributes + (AttributeName.withDefaultNS("as") -> AttributeEntityReferenceList(as)))))
    }
  }

  test("string literals") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(AttributeString("str"))), "(simple string failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """"str""""))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeString(" str"))), "(string with space failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """" str""""))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeString("\"str"))), "(string with escaped quote in it failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """"\"str""""))
      }

      intercept[RawlsException] {
        //the string ""str" is not valid JSON as internal strings should be quoted.
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """""str""""))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """this."foo""""))
      }
      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """that."foo""""))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeString("42"))), "(quoted numeral should have parsed as string)") {
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

      assertResult(Map("sample1" ->TrySuccess(AttributeNumber(42))), "(integer failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """42"""))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeNumber(-4.2))), "(decimal failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """-4.2"""))
      }
    }
  }

  test("null literal") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" ->TrySuccess(AttributeNull)), "(null failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """null"""))
      }
    }
  }

  test("array literals") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" ->TrySuccess(AttributeValueEmptyList)), "(empty array failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "[]"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2))))), "(numeric array failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "[1,2]"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueList(Seq(AttributeNumber(1), AttributeBoolean(true), AttributeString("three"), AttributeNull)))), "(heterogeneous value typed array failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """[1,true,"three", null]"""))
      }
    }
  }

  test("raw json literals") {
    withTestWorkspace { workspaceContext =>
      // - json for entity references should be parsed as RawJson
      assertResult(Map("sset1" -> TrySuccess(AttributeValueRawJson("{\"entityType\":\"sample\",\"entityName\":\"sample2\"}")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", """{"entityType":"sample","entityName":"sample2"}"""))
      }
      // - json for lists of entity references should be parsed as RawJson
      assertResult(Map("sset1" -> TrySuccess(AttributeValueRawJson("[{\"entityType\":\"sample\",\"entityName\":\"sample2\"}]")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", """[{"entityType":"sample","entityName":"sample2"}]"""))
      }
      // - json for lists of numbers and entity references (i.e. a mix of attribute val and ref) should be parsed as RawJson
      assertResult(Map("sset1" -> TrySuccess(AttributeValueRawJson("[{\"entityType\":\"sample\",\"entityName\":\"sample2\"},9]")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", """[{"entityType":"sample","entityName":"sample2"},9]"""))
      }
      // - json for objects of any kind should be parsed as RawJson
      assertResult(Map("sset1" -> TrySuccess(AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(0)))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "[9,0]"))
      }
      assertResult(Map("sset1" -> TrySuccess(AttributeValueRawJson("{\"foo\":\"bar\"}")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "{\"foo\":\"bar\"}"))
      }
    }
  }

  test("reserved attribute expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(AttributeString("sample1")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.name"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeString("sample1")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.sample_id"))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeValueList(Seq(AttributeString("Sample"), AttributeString("Sample"), AttributeString("Sample")))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.entityType"))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeString(workspaceContext.workspace.name)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.name"))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeString(workspaceContext.workspace.name)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.workspace_id"))
      }

      assertResult(Map("sset1" -> TrySuccess(AttributeString("workspace")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.entityType"))
      }
    }
  }

  test("workspace attribute expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Map("sample1" -> TrySuccess(testData.wsAttrs.get(AttributeName.withDefaultNS("string")).get))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.string"))
      }

      assertResult(Map("sample1" -> TrySuccess(testData.wsAttrs.get(AttributeName.withDefaultNS("number")).get))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.number"))
      }

      assertResult(Map("sample1" -> TrySuccess(testData.sample1.attributes.get(AttributeName.withDefaultNS("type")).get))) {
        val attributesPlusReference = testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.sample1ref.type"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueEmptyList))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.emptyList"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueList(Seq(AttributeString("another string"), AttributeString("true")))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.values"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeNull))) {
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
        val attributesPlusReference = testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.sample1ref."))
      }
    }
  }

  test("workspace library attribute expression") {
    withTestWorkspace { workspaceContext =>

      val series = Seq(
        AttributeString("The Fellowship of the Ring"),
        AttributeString("The Two Towers"),
        AttributeString("The Return of the King")
      )

      val libraryAttributes = Map(
        AttributeName("library", "author") -> AttributeString("L. Ron Hubbard"),
        AttributeName("library", "emptyList") -> AttributeValueEmptyList,
        AttributeName("library", "series") -> AttributeValueList(series)
      )

      runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = testData.workspace.attributes ++ libraryAttributes)))

      assertResult(Map("sample1" -> TrySuccess(AttributeString("L. Ron Hubbard")))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:author"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueEmptyList))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:emptyList"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeValueList(series)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:series"))
      }

      assertResult(Map("sample1" -> TrySuccess(AttributeNull))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:not_here"))
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

  test("library entity expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Set("sample2")) {
        val ent = runAndWait(entityQuery.get(workspaceContext, "Pair", "pair1")).get
        val libraryAttribute = AttributeName("library", "lib_case") -> testData.sample2.toReference
        runAndWait(entityQuery.save(workspaceContext, ent.copy(attributes = ent.attributes + libraryAttribute)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "this.library:lib_case")).map(_.name).toSet
      }

      assertResult(Set("sample1", "sample2", "sample3")) {
        val ent = runAndWait(entityQuery.get(workspaceContext, "Individual", "indiv1")).get
        val libraryAttribute = AttributeName("library", "lib_set") -> testData.sset1.toReference
        runAndWait(entityQuery.save(workspaceContext, ent.copy(attributes = ent.attributes + libraryAttribute)))

        runAndWait(evalFinalEntity(workspaceContext, "Individual", "indiv1", "this.library:lib_set.samples")).map(_.name).toSet
      }
    }
  }

  test("workspace entity expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Set("sample1")) {
        val attributesPlusReference = testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.sample1ref")).map(_.name).toSet
      }

      assertResult(Set("sample2")) {
        val reflist = AttributeEntityReferenceList(Seq(testData.sample2.toReference))
        val attributesPlusReference = testData.workspace.attributes + (AttributeName.withDefaultNS("samplerefs") -> reflist)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.samplerefs")).map(_.name).toSet
      }
    }
  }

  test("library workspace entity expression") {
    withTestWorkspace { workspaceContext =>

      assertResult(Set("sample1")) {
        val attributesPlusReference = testData.workspace.attributes + (AttributeName("library", "s1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.library:s1ref")).map(_.name).toSet
      }

      assertResult(Set("sample2")) {
        val reflist = AttributeEntityReferenceList(Seq(testData.sample2.toReference))
        val attributesPlusReference = testData.workspace.attributes + (AttributeName("library", "srefs") -> reflist)
        runAndWait(workspaceQuery.save(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.library:srefs")).map(_.name).toSet
      }
    }
  }

  test("output expressions") {
    withTestWorkspace { workspaceContext =>
      assert(parseOutputAttributeExpr("this.attribute").isSuccess, "this.attribute should parse correctly" )
      assert(parseOutputAttributeExpr("this..attribute").isFailure, "this..attribute should not parse correctly" )
      assert(parseOutputAttributeExpr("this.chained.expression").isFailure, "this.chained.expression should not parse correctly" )

      assert(parseOutputAttributeExpr("workspace.attribute").isSuccess, "workspace.attribute should parse correctly" )
      assert(parseOutputAttributeExpr("workspace..attribute").isFailure, "workspace..attribute should not parse correctly" )
      assert(parseOutputAttributeExpr("workspace.chained.expression").isFailure, "workspace.chained.expression should not parse correctly" )

      assert(parseOutputAttributeExpr("bonk.attribute").isFailure, "bonk.attribute should not parse correctly" )
    }
  }

  test("library output expressions") {
    withTestWorkspace { workspaceContext =>
      assert(parseOutputAttributeExpr("this.library:attribute").isSuccess, "this.library:attribute should parse correctly" )
      assert(parseOutputAttributeExpr("this..library:attribute").isFailure, "this..library:attribute should not parse correctly" )
      assert(parseOutputAttributeExpr("this.library:chained.expression").isFailure, "this.library:chained.expression should not parse correctly" )
      assert(parseOutputAttributeExpr("this.chained.library:expression").isFailure, "this.chained.library:expression should not parse correctly" )

      assert(parseOutputAttributeExpr("workspace.library:attribute").isSuccess, "workspace.library:attribute should parse correctly" )
      assert(parseOutputAttributeExpr("workspace..library:attribute").isFailure, "workspace..library:attribute should not parse correctly" )
      assert(parseOutputAttributeExpr("workspace.library:chained.expression").isFailure, "workspace.library:chained.expression should not parse correctly" )
      assert(parseOutputAttributeExpr("workspace.chained.library:expression").isFailure, "workspace.chained.library:expression should not parse correctly" )

      assert(parseOutputAttributeExpr("bonk.library:attribute").isFailure, "bonk.library:attribute should not parse correctly" )
    }
  }

  test("extra data in entity attribute temp table should not mess things up") {
    withTestWorkspace { workspaceContext =>
      val action = for {
        entityRecs <- this.entityQuery.findActiveEntityByWorkspace(workspaceContext.workspaceId).result
        extraScratchRecord = ExprEvalRecord(entityRecs.tail.head.id, entityRecs.tail.head.name, "not a transaction id")
        _ <- this.exprEvalQuery += extraScratchRecord
        result <- evalFinalAttribute(workspaceContext, entityRecs.head.entityType, entityRecs.head.name, "this.name").transactionally
        residual <- this.exprEvalQuery.result
      } yield {
        (extraScratchRecord, result, residual)
      }

      val (extraScratchRecord, result, residual) = runAndWait(action.withPinnedSession)
      assertResult(Seq(extraScratchRecord)) { residual }
      assert(result.size == 1 && result.get(extraScratchRecord.name).isEmpty)
    }
  }
}
