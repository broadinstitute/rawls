package org.broadinstitute.dsde.rawls.util

import org.scalatest.freespec.AnyFreeSpec
import spray.json._

class JsonFilterUtilsSpec extends AnyFreeSpec with JsonFilterUtils {

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

    "shallow object filter" - {

      "should return unchanged with no filters" in {
        val actual = shallowFilterJsObject(in, Set.empty)
        assertResult(in)(actual)
      }

      "should return an empty object with only unrecognized filters" in {
        val actual = shallowFilterJsObject(in, Set("these", "keys", "are", "not", "in", "the", "input"))
        val expected = JsObject()
        assertResult(expected)(actual)
      }

      List("topLevelBoolean", "firstLevelObject", "topLevelString", "anotherObject") foreach { key =>
        s"should filter to any single key ('$key')" in {
          val actual = shallowFilterJsObject(in, Set(key))
          val expected = JsObject(key -> in.fields(key))
          assertResult(expected)(actual)
        }
      }

      List("topLevelBoolean", "firstLevelObject", "topLevelString", "anotherObject") foreach { key =>
        s"should filter to any single key ('$key'), even when additional unrecognized keys exist" in {
          val actual = shallowFilterJsObject(in, Set(key, "something", "else"))
          val expected = JsObject(key -> in.fields(key))
          assertResult(expected)(actual)
        }
      }

      "should filter to multiple keys" in {
        val actual = shallowFilterJsObject(in, Set("firstLevelObject", "topLevelBoolean"))
        val expected = JsObject(
          "firstLevelObject" -> firstLevel,
          "topLevelBoolean" -> JsBoolean(true)
        )
        assertResult(expected)(actual)
      }

    }

    "deep object filter" - {

      "should return unchanged with no filters" in {
        val actual = deepFilterJsObject(in, Set.empty)
        assertResult(in)(actual)
      }

      "should return an empty object with only unrecognized filters" in {
        val actual = deepFilterJsObject(in,
                                        Set("huh",
                                            "what",
                                            "firstLevelObject.unknown",
                                            "firstLevelObject.secondLevelObject.alsoUnknown",
                                            "something.else"
                                        )
        )
        val expected = JsObject()
        assertResult(expected)(actual)
      }

      "should filter to only top-level keys" in {
        val actual = deepFilterJsObject(in, Set("topLevelBoolean", "anotherObject"))
        val expected = JsObject(
          "topLevelBoolean" -> JsBoolean(true),
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected)(actual)
      }

      "should filter to top-level keys in presence of unrecognized filters" in {
        val actual = deepFilterJsObject(in,
                                        Set("topLevelBoolean",
                                            "anotherObject",
                                            "huh",
                                            "what",
                                            "firstLevelObject.unknown",
                                            "firstLevelObject.secondLevelObject.alsoUnknown",
                                            "something.else"
                                        )
        )
        val expected = JsObject(
          "topLevelBoolean" -> JsBoolean(true),
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected)(actual)
      }

      "should filter to nested keys one level deep" in {
        val actual = deepFilterJsObject(in, Set("anotherObject.one"))
        val expected = JsObject(
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected)(actual)
      }

      "should be lenient about filtering to nested keys whose parent is not an object" in {
        val actual = deepFilterJsObject(in, Set("anotherObject.one.oneIsNotAnObject"))
        val expected = JsObject(
          "anotherObject" -> JsObject("one" -> JsNumber(111))
        )
        assertResult(expected)(actual)
      }

      "should filter to entire trees when root is specified" in {
        val actual = deepFilterJsObject(in, Set("firstLevelObject"))
        val expected = JsObject(
          "firstLevelObject" -> firstLevel
        )
        assertResult(expected)(actual)
      }

      "should filter to entire *nested* trees when *nested* root is specified" in {
        val actual = deepFilterJsObject(in, Set("firstLevelObject.secondLevelObject"))
        val expected = JsObject(
          "firstLevelObject" -> JsObject("secondLevelObject" -> secondLevel)
        )
        assertResult(expected)(actual)
      }

      "should filter to combinations of top-level, nested roots, and nested keys" in {
        val actual = deepFilterJsObject(
          in,
          Set(
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
          )
        )

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
          )
        )

        assertResult(expected)(actual)

      }

    }

    "deep JsValue filter" - {

      "should handle a single object transparently" in {
        // since we validated behavior of deepFilterJsObject above, we can use it to calculate expected value
        val actual = deepFilterJsValue(in, Set("firstLevelObject.secondLevelObject"))
        val expected = deepFilterJsObject(in, Set("firstLevelObject.secondLevelObject"))
        assertResult(expected)(actual)
      }

      "should filter objects in array" in {
        val input = JsArray(firstLevel, secondLevel, thirdLevel)
        val actual = deepFilterJsValue(input, Set("str", "num"))
        val expected = JsArray(
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(1)
          ),
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(2)
          ),
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(3)
          )
        )
        assertResult(expected)(actual)
      }

      "should filter objects in a mixed array" in {
        val input = JsArray(firstLevel, JsTrue, secondLevel, JsString("extra"), thirdLevel)
        val actual = deepFilterJsValue(input, Set("str", "num"))
        val expected = JsArray(
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(1)
          ),
          JsTrue,
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(2)
          ),
          JsString("extra"),
          JsObject(
            "str" -> JsString("foo"),
            "num" -> JsNumber(3)
          )
        )
        assertResult(expected)(actual)
      }

      "should filter nested arrays" in {
        val input = JsArray(
          JsObject(
            "str" -> JsString("top.one"),
            "num" -> JsNumber(1),
            "nested" -> JsArray(
              JsObject(
                "str" -> JsString("nested.one"),
                "num" -> JsNumber(1.1)
              ),
              JsObject(
                "str" -> JsString("nested.two"),
                "num" -> JsNumber(1.2)
              )
            )
          ),
          JsObject(
            "str" -> JsString("top.two"),
            "num" -> JsNumber(2),
            "nested" -> JsArray(
              JsObject(
                "str" -> JsString("nested.three"),
                "num" -> JsNumber(2.1)
              ),
              JsObject(
                "str" -> JsString("nested.four"),
                "num" -> JsNumber(2.2)
              )
            )
          )
        )
        val actual = deepFilterJsValue(input, Set("num", "nested.num"))
        val expected = JsArray(
          JsObject(
            "num" -> JsNumber(1),
            "nested" -> JsArray(
              JsObject(
                "num" -> JsNumber(1.1)
              ),
              JsObject(
                "num" -> JsNumber(1.2)
              )
            )
          ),
          JsObject(
            "num" -> JsNumber(2),
            "nested" -> JsArray(
              JsObject(
                "num" -> JsNumber(2.1)
              ),
              JsObject(
                "num" -> JsNumber(2.2)
              )
            )
          )
        )
        assertResult(expected)(actual)
      }

    }

  }

}
