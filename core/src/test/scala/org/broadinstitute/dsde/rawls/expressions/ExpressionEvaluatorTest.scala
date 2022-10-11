package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ExprEvalRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.Seq
import scala.util.{Random, Success => TrySuccess}

/**
 * Created by abaumann on 5/21/15.
 */
class ExpressionEvaluatorTest extends AnyFunSuite with TestDriverComponent {
  import driver.api._

  def withTestWorkspace[T](testCode: (Workspace) => T): T =
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { workspaceContext =>
        testCode(workspaceContext)
      }
    }

  def evalFinalEntity(workspaceContext: Workspace, entityType: String, entityName: String, expression: String) =
    ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, entityType, entityName) { evaluator =>
      evaluator.evalFinalEntity(workspaceContext, expression)
    }

  def evalFinalAttribute(workspaceContext: Workspace, entityType: String, entityName: String, expression: String) =
    entityQuery.findEntityByName(workspaceContext.workspaceIdAsUUID, entityType, entityName).result flatMap {
      entityRec =>
        ExpressionEvaluator.withNewExpressionEvaluator(this, Some(entityRec)) { evaluator =>
          evaluator.evalFinalAttribute(workspaceContext, expression)
        }
    }

  test("withNewExpressionEvaluator lookups") {
    withTestWorkspace { workspaceContext =>
      // Single sample lookup works
      assertResult("sample1") {
        runAndWait(ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "sample1") {
          evaluator =>
            DBIO.successful(evaluator.rootEntities.get.head.name)
        })
      }

      // Nonexistent sample lookup fails
      intercept[RawlsException] {
        runAndWait(ExpressionEvaluator.withNewExpressionEvaluator(this, workspaceContext, "Sample", "nonexistent") {
          evaluator =>
            DBIO.successful(evaluator.rootEntities.get.head.name)
        })
      }
    }
  }

  test("ensure root entity temp table was cleared") {
    withTestWorkspace { workspaceContext =>
      assertResult(0) {
        runAndWait {
          evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type") andThen
            sql"select count(*) from EXPREVAL_SCRATCH".as[Int].head
        }
      }
    }
  }

  test("single attribute expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("normal"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type"))
      }

      assertResult(Map("sample8" -> TrySuccess(Seq(AttributeString("1029384756"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample8", "this.foo_id"))
      }

      assertResult(
        Map("sset1" -> TrySuccess(Seq(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor"))))
      ) {
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

  test("attribute expression that refers to an entity should except") {
    withTestWorkspace { workspaceContext =>
      val exprResult = runAndWait(evalFinalAttribute(workspaceContext, "Pair", "pair1", "this.case"))
      assert(exprResult("pair1").isFailure)
    }
  }

  test("library entity attribute expression") {
    withTestWorkspace { workspaceContext =>
      val libraryAttribute = Map(AttributeName("library", "book") -> AttributeString("arbitrary"))
      runAndWait(
        entityQuery.save(workspaceContext, Entity("sampleWithLibraryNamespaceAttribute", "Sample", libraryAttribute))
      )

      assertResult(Map("sampleWithLibraryNamespaceAttribute" -> TrySuccess(Seq(AttributeString("arbitrary"))))) {
        runAndWait(
          evalFinalAttribute(workspaceContext, "Sample", "sampleWithLibraryNamespaceAttribute", "this.library:book")
        )
      }

      assertResult(Map("sampleWithLibraryNamespaceAttribute" -> TrySuccess(Seq()))) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "Sample",
                             "sampleWithLibraryNamespaceAttribute",
                             "this.library:checked_out_book"
          )
        )
      }

      (1 to 3).foreach { num =>
        val ent = runAndWait(entityQuery.get(workspaceContext, "Sample", s"sample$num")).get
        val libraryAttribute = Map(AttributeName("library", "chapter") -> AttributeNumber(num))
        runAndWait(entityQuery.save(workspaceContext, ent.copy(attributes = libraryAttribute)))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.library:chapter"))
      }

      val resultsByType = runAndWait(
        entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap {
          ents =>
            ExpressionEvaluator.withNewExpressionEvaluator(this, Some(ents)) { evaluator =>
              evaluator.evalFinalAttribute(workspaceContext, "this.library:chapter")
            }
        }
      )

      (1 to 3).foreach { num =>
        assertResult(TrySuccess(Seq(AttributeNumber(num)))) {
          resultsByType(s"sample$num")
        }
      }

      // workspace library attribute references inside JSON

      assertResult(
        Map(
          "sampleWithLibraryNamespaceAttribute" -> TrySuccess(Seq(AttributeValueRawJson("""{"book": "arbitrary"}""")))
        ),
        "JSON with single library entity attribute reference failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "Sample",
                             "sampleWithLibraryNamespaceAttribute",
                             """{"book": this.library:book}"""
          )
        )
      }

      assertResult(
        Map(
          "sset1" -> TrySuccess(
            Seq(AttributeValueRawJson("""{"book": "sset1", "chapters": ["few", [1, 2, 3], [[1, 2, 3]]]}"""))
          )
        ),
        "JSON with multiple library entity attribute reference failed"
      ) {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "SampleSet",
            "sset1",
            """{"book": this.default:name, "chapters": ["few", this.samples.library:chapter, [this.samples.library:chapter]]}"""
          )
        )
      }

      // workspace library attribute references inside Array

      assertResult(
        Map(
          "sampleWithLibraryNamespaceAttribute" -> TrySuccess(
            Seq(AttributeString("string"), AttributeString("another string"), AttributeString("arbitrary"))
          )
        ),
        "array with single library entity attribute reference failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "Sample",
                             "sampleWithLibraryNamespaceAttribute",
                             """["string", "another string", this.library:book]"""
          )
        )
      }

      val nestedArrayOutput1 = Seq(AttributeValueRawJson("""["string", "sset1", "number", [1, 2, 3]]"""))
      assertResult(Map("sset1" -> TrySuccess(nestedArrayOutput1)),
                   "nested array with multiple library entity attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "SampleSet",
                             "sset1",
                             """["string", this.default:name, "number", this.samples.library:chapter]"""
          )
        )
      }

      val nestedArrayOutput2 =
        Seq(AttributeString("string"), AttributeString("sset1"), AttributeValueRawJson("""["chapters", [1, 2, 3]]"""))
      assertResult(Map("sset1" -> TrySuccess(nestedArrayOutput2)),
                   "nested array with multiple library entity attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "SampleSet",
                             "sset1",
                             """["string", this.default:name, ["chapters", this.samples.library:chapter]]"""
          )
        )
      }

      val nestedArrayOutput3 = Seq(AttributeString("string"),
                                   AttributeString("sset1"),
                                   AttributeValueRawJson("""["chapters", ["numbers", 1, 2], [1, 2, 3]]""")
      )
      assertResult(Map("sset1" -> TrySuccess(nestedArrayOutput3)),
                   "nested array with multiple library entity attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "SampleSet",
            "sset1",
            """["string", this.default:name, ["chapters", ["numbers", 1, 2], this.samples.library:chapter]]"""
          )
        )
      }

      val nestedArrayOutput4 = Seq(
        AttributeString("string"),
        AttributeString("sset1"),
        AttributeValueRawJson("""["chapters", ["numbers", 1, 2], [1, 2, 3]]"""),
        AttributeString("bool"),
        AttributeBoolean(false)
      )
      assertResult(Map("sset1" -> TrySuccess(nestedArrayOutput4)),
                   "nested array with multiple library entity attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "SampleSet",
            "sset1",
            """["string", this.default:name, ["chapters", ["numbers", 1, 2], this.samples.library:chapter], "bool", false]"""
          )
        )
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
        "sample1" -> TrySuccess(Seq(AttributeString("normal"))),
        "sample2" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample3" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample4" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample5" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample6" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample7" -> TrySuccess(Seq(AttributeString("tumor"))),
        "sample8" -> TrySuccess(Seq(AttributeString("tumor")))
      )

      assertResult(allTheTypes) {
        runAndWait(
          entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap {
            ents =>
              ExpressionEvaluator.withNewExpressionEvaluator(this, Some(ents)) { evaluator =>
                evaluator.evalFinalAttribute(workspaceContext, "this.type")
              }
          }
        )
      }

      val allTheTumorTypes = Map(
        "sample1" -> TrySuccess(Seq()),
        "sample2" -> TrySuccess(Seq(AttributeString("LUSC"))),
        "sample3" -> TrySuccess(Seq(AttributeString("LUSC"))),
        "sample4" -> TrySuccess(Seq()),
        "sample5" -> TrySuccess(Seq()),
        "sample6" -> TrySuccess(Seq()),
        "sample7" -> TrySuccess(Seq()),
        "sample8" -> TrySuccess(Seq())
      )

      assertResult(allTheTumorTypes) {
        runAndWait(
          entityQuery.findActiveEntityByType(UUID.fromString(testData.workspace.workspaceId), "Sample").result flatMap {
            ents =>
              ExpressionEvaluator.withNewExpressionEvaluator(this, Some(ents)) { evaluator =>
                evaluator.evalFinalAttribute(workspaceContext, "this.tumortype")
              }
          }
        )
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

      assertResult(nameResults)(attrResults)
      assertResult(nameResults)(Map("2" -> TrySuccess(entityResults.map(e => AttributeString(e.name)))))

      val wsNameResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.name"))
      val wsAttrResults = runAndWait(evalFinalAttribute(workspaceContext, "a", "2", "workspace.as.bs.cs.attr"))
      val wsEntityResults = runAndWait(evalFinalEntity(workspaceContext, "a", "2", "workspace.as.bs.cs"))

      assert(wsNameResults.nonEmpty, "name expression returns empty")
      assert(wsAttrResults.nonEmpty, "attribute expression returns empty")
      assert(wsEntityResults.nonEmpty, "entity expression returns empty")

      assertResult(wsNameResults)(wsAttrResults)
      assertResult(wsNameResults)(Map("2" -> TrySuccess(wsEntityResults.map(e => AttributeString(e.name)))))
    }

    def createEntities(entityType: String,
                       entitiesNameAndAttributes: IndexedSeq[(String, AttributeMap)],
                       wsc: Workspace
    ): IndexedSeq[AttributeEntityReference] = {
      val saveActions =
        for ((nameAndAttributes, index) <- entitiesNameAndAttributes.zipWithIndex)
          yield entityQuery.save(wsc, Entity(nameAndAttributes._1, entityType, nameAndAttributes._2))

      // save in random order
      runAndWait(DBIO.sequence(Random.shuffle(saveActions)))

      entitiesNameAndAttributes.map { case (name, _) => AttributeEntityReference(entityType, name) }
    }

    def createEntityStructure(wsc: Workspace) = {
      val aAttributes = for (a <- 0 until 5) yield {
        val bAttributes = for (b <- 0 until 5) yield {
          val cAttributes =
            for (c <- 0 until 20)
              yield (s"${a}_${b}_${c}", Map(AttributeName.withDefaultNS("attr") -> AttributeString(s"${a}_${b}_${c}")))

          (s"${a}_${b}",
           Map(AttributeName.withDefaultNS("cs") -> AttributeEntityReferenceList(createEntities("c", cAttributes, wsc)))
          )
        }

        (s"$a",
         Map(AttributeName.withDefaultNS("bs") -> AttributeEntityReferenceList(createEntities("b", bAttributes, wsc)))
        )
      }

      val as = createEntities("a", aAttributes, wsc)

      runAndWait(
        workspaceQuery.createOrUpdate(
          wsc.copy(attributes =
            wsc.attributes + (AttributeName.withDefaultNS("as") -> AttributeEntityReferenceList(as))
          )
        )
      )
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

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("\"str")))),
                   "(string with escaped quote in it failed)"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """"\"str""""))
      }

      intercept[RawlsException] {
        // the string ""str" is not valid JSON as internal strings should be quoted.
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """""str""""))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """this."foo""""))
      }

      intercept[RawlsException] {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """that."foo""""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("42")))),
                   "(quoted numeral should have parsed as string)"
      ) {
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
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeNumber(42)))), "(integer failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """42"""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeNumber(-4.2)))), "(decimal failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """-4.2"""))
      }
    }
  }

  test("null literal") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq())), "(null failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """null"""))
      }
    }
  }

  test("raw array literals") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq())), "(empty array failed)") {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "[]"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeNumber(1), AttributeNumber(2)))),
                   "(numeric array failed)"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "[1,2]"))
      }

      assertResult(
        Map(
          "sample1" -> TrySuccess(
            Seq(AttributeNumber(1), AttributeBoolean(true), AttributeString("three"), AttributeNull)
          )
        ),
        "(heterogeneous value typed array failed)"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """[1,true,"three", null]"""))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson("""[1,true,["three", null]]""")))),
                   "(nested heterogeneous typed array failed)"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """[1,true,["three", null]]"""))
      }
    }
  }

  test("raw json literals") {
    withTestWorkspace { workspaceContext =>
      // - json for entity references should be parsed as RawJson
      assertResult(
        Map(
          "sset1" -> TrySuccess(List(AttributeValueRawJson("{\"entityType\":\"sample\",\"entityName\":\"sample2\"}")))
        )
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "SampleSet",
                             "sset1",
                             """{"entityType":"sample","entityName":"sample2"}"""
          )
        )
      }
      // - json for lists of entity references should be parsed as RawJson
      assertResult(
        Map(
          "sset1" -> TrySuccess(List(AttributeValueRawJson("[{\"entityType\":\"sample\",\"entityName\":\"sample2\"}]")))
        )
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "SampleSet",
                             "sset1",
                             """[{"entityType":"sample","entityName":"sample2"}]"""
          )
        )
      }
      // - json for lists of numbers and entity references (i.e. a mix of attribute val and ref) should be parsed as RawJson
      assertResult(
        Map(
          "sset1" -> TrySuccess(
            List(AttributeValueRawJson("[{\"entityType\":\"sample\",\"entityName\":\"sample2\"},9]"))
          )
        )
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "SampleSet",
                             "sset1",
                             """[{"entityType":"sample","entityName":"sample2"},9]"""
          )
        )
      }
      // - json for objects of any kind should be parsed as RawJson
      assertResult(Map("sset1" -> TrySuccess(Vector(AttributeNumber(9), AttributeNumber(0))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "[9,0]"))
      }
      assertResult(Map("sset1" -> TrySuccess(List(AttributeValueRawJson("{\"foo\":\"bar\"}"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "{\"foo\":\"bar\"}"))
      }
    }
  }

  test("reserved attribute expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("sample1"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.name"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("sample1"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.Sample_id"))
      }

      assertResult(
        Map("sset1" -> TrySuccess(Seq(AttributeString("Sample"), AttributeString("Sample"), AttributeString("Sample"))))
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.entityType"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString(workspaceContext.name))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.name"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString(workspaceContext.name))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.workspace_id"))
      }

      assertResult(Map("sset1" -> TrySuccess(Seq(AttributeString("workspace"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.entityType"))
      }
    }
  }

  test("workspace attribute expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Map("sample1" -> TrySuccess(Seq(testData.wsAttrs.get(AttributeName.withDefaultNS("string")).get)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.string"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq(testData.wsAttrs.get(AttributeName.withDefaultNS("number")).get)))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.number"))
      }

      assertResult(
        Map("sample1" -> TrySuccess(Seq(testData.sample1.attributes.get(AttributeName.withDefaultNS("type")).get)))
      ) {
        val attributesPlusReference =
          testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

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
        val attributesPlusReference =
          testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.sample1ref."))
      }

      // workspace library attribute references inside JSON

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson("""{"workspaceAttr": 10}""")))),
                   "JSON with single workspace attribute reference failed"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", """{"workspaceAttr": workspace.number}"""))
      }

      val nestedJsonExpr =
        """
          |{
          |   "numberAttr": workspace.number,
          |   "other" : {
          |       "stringAttr": workspace.string,
          |       "arrayOutputs": {
          |           "values": workspace.values
          |       }
          |   }
          |}
        """.stripMargin
      val nestedJsonEvaluatedOutput =
        s"""
           |{
           |   "numberAttr": 10,
           |   "other" : {
           |       "stringAttr": "yep, it's a string",
           |       "arrayOutputs": {
           |           "values": ["another string", "true"]
           |       }
           |   }
           |}
        """.stripMargin
      assertResult(
        Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson(s"""$nestedJsonEvaluatedOutput""")))),
        "nested JSON with multiple workspace attribute references failed"
      ) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", s"""$nestedJsonExpr"""))
      }

      // workspace library attribute references inside Array

      assertResult(
        Map(
          "sample1" -> TrySuccess(
            Seq(AttributeString("string"), AttributeString("another string"), AttributeNumber(10))
          )
        ),
        "array with single workspace attribute reference failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "Sample",
                             "sample1",
                             """["string", "another string", workspace.number]"""
          )
        )
      }

      val nestedArrayOutput1 = Seq(
        AttributeValueRawJson(
          """["string", "yep, it's a string", "number", 10, true, ["values"], ["another string", "true"], false]"""
        )
      )
      assertResult(Map("sample1" -> TrySuccess(nestedArrayOutput1)),
                   "nested array with multiple workspace attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "Sample",
            "sample1",
            """["string", workspace.string, "number", workspace.number, true, ["values"], workspace.values, false]"""
          )
        )
      }

      val nestedArrayOutput2 = Seq(
        AttributeString("string"),
        AttributeString("yep, it's a string"),
        AttributeString("number"),
        AttributeNumber(10),
        AttributeBoolean(true),
        AttributeValueRawJson("""["values", ["another string", "true"]]""")
      )
      assertResult(Map("sample1" -> TrySuccess(nestedArrayOutput2)),
                   "nested array with multiple workspace attribute references failed"
      ) {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "Sample",
            "sample1",
            """["string", workspace.string, "number", workspace.number, true, ["values", workspace.values]]"""
          )
        )
      }
    }
  }

  test("workspace library attribute expression") {
    withTestWorkspace { workspaceContext =>
      val series = List(
        AttributeString("The Fellowship of the Ring"),
        AttributeString("The Two Towers"),
        AttributeString("The Return of the King")
      )

      val libraryAttributes = Map(
        AttributeName("library", "author") -> AttributeString("L. Ron Hubbard"),
        AttributeName("library", "nothing") -> AttributeValueEmptyList,
        AttributeName("library", "series") -> AttributeValueList(series)
      )

      runAndWait(
        workspaceQuery.createOrUpdate(
          testData.workspace.copy(attributes = testData.workspace.attributes ++ libraryAttributes)
        )
      )

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeString("L. Ron Hubbard"))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:author"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:nothing"))
      }

      assertResult(Map("sample1" -> TrySuccess(series))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:series"))
      }

      assertResult(Map("sample1" -> TrySuccess(Seq()))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", "workspace.library:not_here"))
      }

      // workspace library attribute references inside JSON

      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson("""{"author": "L. Ron Hubbard"}"""))))) {
        runAndWait(
          evalFinalAttribute(workspaceContext, "Sample", "sample1", """{"author": workspace.library:author}""")
        )
      }

      val jsonExpr =
        """
          |{
          |   "author": workspace.library:author,
          |   "bookSeries": workspace.library:series
          |}
        """.stripMargin
      val jsonEvaluatedOutput =
        s"""
           |{
           |   "author": "L. Ron Hubbard",
           |   "bookSeries":["The Fellowship of the Ring", "The Two Towers", "The Return of the King"]
           |}
        """.stripMargin
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson(s"""$jsonEvaluatedOutput"""))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", s"""$jsonExpr"""))
      }

      val nestedJsonExpr =
        """
          |{
          |   "author": workspace.library:author,
          |   "details" : {
          |       "publication": "ABC Publication House",
          |       "bookSeries": workspace.library:series
          |   }
          |}
        """.stripMargin
      val nestedJsonEvaluatedOutput =
        s"""
           |{
           |   "author": "L. Ron Hubbard",
           |   "details" : {
           |       "publication": "ABC Publication House",
           |       "bookSeries":["The Fellowship of the Ring", "The Two Towers", "The Return of the King"]
           |   }
           |}
        """.stripMargin
      assertResult(Map("sample1" -> TrySuccess(Seq(AttributeValueRawJson(s"""$nestedJsonEvaluatedOutput"""))))) {
        runAndWait(evalFinalAttribute(workspaceContext, "Sample", "sample1", s"""$nestedJsonExpr"""))
      }

      // workspace library attribute references inside Array

      assertResult(
        Map(
          "sample1" -> TrySuccess(
            Seq(AttributeString("book"), AttributeString("authorName"), AttributeString("L. Ron Hubbard"))
          )
        ),
        "array with single attribute reference failed"
      ) {
        runAndWait(
          evalFinalAttribute(workspaceContext,
                             "Sample",
                             "sample1",
                             """["book", "authorName", workspace.library:author]"""
          )
        )
      }

      val nestedArrayOutput = Seq(
        AttributeValueRawJson(
          """["books", 3, "authorName", "L. Ron Hubbard", "series", ["The Fellowship of the Ring", "The Two Towers", "The Return of the King"]]"""
        )
      )
      assertResult(Map("sample1" -> TrySuccess(nestedArrayOutput)), "nested array with attribute references failed") {
        runAndWait(
          evalFinalAttribute(
            workspaceContext,
            "Sample",
            "sample1",
            """["books", 3, "authorName", workspace.library:author, "series", workspace.library:series]"""
          )
        )
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

        runAndWait(evalFinalEntity(workspaceContext, "Individual", "indiv1", "this.library:lib_set.samples"))
          .map(_.name)
          .toSet
      }
    }
  }

  test("workspace entity expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Set("sample1")) {
        val attributesPlusReference =
          testData.workspace.attributes + (AttributeName.withDefaultNS("sample1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.sample1ref")).map(_.name).toSet
      }

      assertResult(Set("sample2")) {
        val reflist = AttributeEntityReferenceList(List(testData.sample2.toReference))
        val attributesPlusReference =
          testData.workspace.attributes + (AttributeName.withDefaultNS("samplerefs") -> reflist)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.samplerefs")).map(_.name).toSet
      }
    }
  }

  test("library workspace entity expression") {
    withTestWorkspace { workspaceContext =>
      assertResult(Set("sample1")) {
        val attributesPlusReference =
          testData.workspace.attributes + (AttributeName("library", "s1ref") -> testData.sample1.toReference)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.library:s1ref")).map(_.name).toSet
      }

      assertResult(Set("sample2")) {
        val reflist = AttributeEntityReferenceList(List(testData.sample2.toReference))
        val attributesPlusReference = testData.workspace.attributes + (AttributeName("library", "srefs") -> reflist)
        runAndWait(workspaceQuery.createOrUpdate(testData.workspace.copy(attributes = attributesPlusReference)))

        runAndWait(evalFinalEntity(workspaceContext, "Pair", "pair1", "workspace.library:srefs")).map(_.name).toSet
      }
    }
  }

  test("extra data in entity attribute temp table should not mess things up") {
    withTestWorkspace { workspaceContext =>
      val action = for {
        entityRecs <- this.entityQuery.findActiveEntityByWorkspace(workspaceContext.workspaceIdAsUUID).result
        extraScratchRecord = ExprEvalRecord(entityRecs.tail.head.id, entityRecs.tail.head.name, "not a transaction id")
        _ <- this.exprEvalQuery += extraScratchRecord
        result <- evalFinalAttribute(workspaceContext,
                                     entityRecs.head.entityType,
                                     entityRecs.head.name,
                                     "this.name"
        ).transactionally
        residual <- this.exprEvalQuery.result
      } yield (extraScratchRecord, result, residual)

      val (extraScratchRecord, result, residual) = runAndWait(action.withPinnedSession)
      assertResult(Seq(extraScratchRecord))(residual)
      assert(result.size == 1 && result.get(extraScratchRecord.name).isEmpty)
    }
  }
}
