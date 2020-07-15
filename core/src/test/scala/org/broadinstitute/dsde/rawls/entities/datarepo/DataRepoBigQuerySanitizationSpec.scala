package org.broadinstitute.dsde.rawls.entities.datarepo

import java.nio.charset.Charset

import com.google.common.io.Resources
import org.scalatest.{BeforeAndAfterAll, FreeSpec}
import spray.json._

class DataRepoBigQuerySanitizationSpec extends FreeSpec with DataRepoBigQuerySupport with BeforeAndAfterAll {

  // read the list of naughty strings from https://github.com/minimaxir/big-list-of-naughty-strings.
  // We expect a json array, containing base64-encoded strings.
  val blnsData = Resources.toString(
    new java.net.URL("https://raw.githubusercontent.com/minimaxir/big-list-of-naughty-strings/master/blns.base64.json"),
    Charset.defaultCharset())

  val naughtyStrings = blnsData.parseJson match {
    case blns:JsArray =>
      blns.elements.map {
        case s:JsString => new String(java.util.Base64.getDecoder.decode(s.value))
        case _ => fail("found something not a string in BLNS")
      }.toList
    case _ => fail("BLNS was not a json array")
  }

  override protected def beforeAll(): Unit = {
    // create a new variable to hold the size; this prevents Scalatest from attempting to output
    // anything about the contents of the list itself, if the assertion fails
    val nSize = naughtyStrings.size
    assert(nSize > 0, "fixture data was empty; failing test. Size: " + nSize)

    super.beforeAll()
  }


  "DataRepoBigQuerySupport, when sanitizing SQL strings, should" - {
    "pass legal strings untouched" in {
      val input = "thisIsALegalString"
      val expected = "thisIsALegalString"
      assertResult(expected) { sanitizeSql(input) }
    }

    "remove illegal characters in strings" in {
      val input = "this has bad characters; it's no good!"
      val expected = "thishasbadcharactersitsnogood"
      assertResult(expected) { sanitizeSql(input) }
    }

    s"do the right things across ${naughtyStrings.size} inputs from github.com/minimaxir/big-list-of-naughty-strings" in {
      // use this instead of regex to ensure the test has a different implementation than the runtime implementation
      def isLegalSqlChar(c: Char): Boolean = {
        val i = c.toInt
        (i >= 48 && i <= 57) || // 0-9
        (i >= 65 && i <= 90) || // A-Z
        (i >= 97 && i <= 122) || // a-z
         i == 95 || // underscore
         i == 45 // hyphen
      }

      def isLegalSqlString(s: String): Boolean = s.toCharArray.forall(isLegalSqlChar)

      naughtyStrings.foreach { input =>
        val actual = sanitizeSql(input)
        assert(isLegalSqlString(actual), s"found an illegal character in '$input' which sanitized to '$actual'")
      }
    }

  }

}
