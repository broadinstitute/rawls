package org.broadinstitute.dsde.rawls.util

import org.scalatest.FreeSpec
import spray.json._

class JsonFilterUtilsSpec extends FreeSpec with JsonFilterUtils {

  // these fixtures are used in multiple tests below
  val thirdLevel = JsObject(
    "thirdLevel" -> JsString("is me; I am the third level of nesting."),
    "str" -> JsString("foo"),
    "obj" -> JsObject(),
    "arr" -> JsArray(),
    "num" -> JsNumber(3)
  )

  val secondLevel = JsObject(
    "secondLevel" -> JsString("is me; I am the second level of nesting."),
    "str" -> JsString("foo"),
    "obj" -> JsObject(),
    "arr" -> JsArray(),
    "num" -> JsNumber(2),
    "thirdLevelObject" -> thirdLevel
  )

  val firstLevel = JsObject(
    "firstLevel" -> JsString("is me; I am the first level of nesting."),
    "str" -> JsString("foo"),
    "obj" -> JsObject(),
    "arr" -> JsArray(),
    "num" -> JsNumber(1),
    "secondLevelObject" -> secondLevel
  )

  val in = JsObject(
    "topLevelBoolean" -> JsBoolean(true),
    "firstLevelObject" -> firstLevel,
    "topLevelString" -> JsString("hello world"),
    "anotherObject" -> JsObject("one" -> JsNumber(111))
  )


  "JsonFilterUtils" - {

    "shallow filter" - {

      "should return unchanged with no filters" in {
        val actual = shallowFilterJsObject(in, Set.empty)
        assertResult(in) { actual }
      }

      "should return an empty object with only unrecognized filters" in {
        val actual = shallowFilterJsObject(in, Set("these", "keys", "are", "not", "in", "the", "input"))
        val expected = JsObject()
        assertResult(expected) { actual }
      }

      List("topLevelBoolean", "firstLevelObject", "topLevelString", "anotherObject") foreach { key =>
        s"should filter to any single key ('$key')" in {
          val actual = shallowFilterJsObject(in, Set(key))
          val expected = JsObject(key -> in.fields(key))
          assertResult(expected) { actual }
        }
      }

      List("topLevelBoolean", "firstLevelObject", "topLevelString", "anotherObject") foreach { key =>
        s"should filter to any single key ('$key'), even when additional unrecogized keys exist" in {
          val actual = shallowFilterJsObject(in, Set(key, "something", "else"))
          val expected = JsObject(key -> in.fields(key))
          assertResult(expected) { actual }
        }
      }

      "should filter to multiple keys" in  {
        val actual = shallowFilterJsObject(in, Set("firstLevelObject", "topLevelBoolean"))
        val expected = JsObject(
          "firstLevelObject" -> firstLevel,
          "topLevelBoolean" -> JsBoolean(true)
        )
        assertResult(expected) { actual }
      }

    }

    "deep filter" - {

      "should return unchanged with no filters" in {
        val actual = deepFilterJsObject(in, Set.empty)
        assertResult(in) { actual }
      }

      "should return an empty object with only unrecognized filters" in {
        val actual = deepFilterJsObject(in, Set("huh", "what", "firstLevelObject.unknown",
          "firstLevelObject.secondLevelObject.alsoUnknown", "something.else"))
        val expected = JsObject()
        assertResult(expected) { actual }
      }

      "should filter to only top-level keys" in {
        val actual = deepFilterJsObject(in, Set("topLevelBoolean", "anotherObject"))
        val expected = JsObject(
          "topLevelBoolean" -> JsBoolean(true),
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected) { actual }
      }

      "should filter to top-level keys in presence of unrecognized filters" in {
        val actual = deepFilterJsObject(in, Set("topLevelBoolean", "anotherObject", "huh", "what", "firstLevelObject.unknown",
          "firstLevelObject.secondLevelObject.alsoUnknown", "something.else"))
        val expected = JsObject(
          "topLevelBoolean" -> JsBoolean(true),
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected) { actual }
      }

      "should filter to nested keys one level deep" in {
        val actual = deepFilterJsObject(in, Set("anotherObject.one"))
        val expected = JsObject(
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected) { actual }
      }

      "should be lenient about filtering to nested keys whose parent is not an object" in {
        val actual = deepFilterJsObject(in, Set("anotherObject.one.oneIsNotAnObject"))
        val expected = JsObject(
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected) { actual }
      }

      "should filter to entire trees when root is specified" in {
        val actual = deepFilterJsObject(in, Set("firstLevelObject"))
        val expected = JsObject(
                    "firstLevelObject" -> firstLevel
        )
        assertResult(expected) { actual }
      }

      "should filter to entire *nested* trees when *nested* root is specified" in {
        val actual = deepFilterJsObject(in, Set("firstLevelObject.secondLevelObject"))
        val expected = JsObject(
          "firstLevelObject" -> JsObject("secondLevelObject" -> secondLevel)
        )
        assertResult(expected) { actual }
      }

      "should filter to combinations of top-level, nested roots, and nested keys" in {
        val actual = deepFilterJsObject(in, Set(
          "unknown",
          "topLevelBoolean",
          "anotherObject.one",
          "firstLevelObject.num",
          "firstLevelObject.obj",
          "firstLevelObject.unknown",
          "firstLevelObject.secondLevelObject.thirdLevelObject",
          "firstLevelObject.secondLevelObject.secondLevel",
          "firstLevelObject.secondLevelObject.arr",
          "firstLevelObject.secondLevelObject.num",
          "firstLevelObject.secondLevelObject.unknown"
        ))

        val expected = JsObject(
          "topLevelBoolean" -> JsBoolean(true),
          "anotherObject" -> JsObject("one" -> JsNumber(111)),
          "firstLevelObject" -> JsObject(
            "obj" -> JsObject(),
            "num" -> JsNumber(1),
            "secondLevelObject" -> JsObject(
              "secondLevel" -> JsString("is me; I am the second level of nesting."),
              "arr" -> JsArray(),
              "num" -> JsNumber(2),
              "thirdLevelObject" -> thirdLevel
            )
          ),
        )

        assertResult(expected) { actual }

      }

    }

  }

}
