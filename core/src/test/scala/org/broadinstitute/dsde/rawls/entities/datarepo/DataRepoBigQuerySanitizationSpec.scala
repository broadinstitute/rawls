package org.broadinstitute.dsde.rawls.entities.datarepo

import com.google.common.io.Resources
import org.broadinstitute.dsde.rawls.entities.exceptions.IllegalIdentifierException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import spray.json._

import java.nio.charset.Charset

class DataRepoBigQuerySanitizationSpec extends AnyFreeSpec with DataRepoBigQuerySupport with BeforeAndAfterAll {

  // read the list of naughty strings from https://github.com/minimaxir/big-list-of-naughty-strings.
  // We expect a json array, containing base64-encoded strings.
  val blnsData = Resources.toString(
    new java.net.URL("https://raw.githubusercontent.com/minimaxir/big-list-of-naughty-strings/master/blns.base64.json"),
    Charset.defaultCharset()
  )

  val naughtyStrings = blnsData.parseJson match {
    case blns: JsArray =>
      blns.elements.map {
        case s: JsString => new String(java.util.Base64.getDecoder.decode(s.value))
        case _           => fail("found something not a string in BLNS")
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

  "DataRepoBigQuerySupport, when validating SQL strings, should" - {
    import DataRepoBigQuerySupport.validateSql

    "pass legal strings untouched" in {
      val input = "thisIsALegalString"
      val expected = "thisIsALegalString"
      assertResult(expected)(validateSql(input))
    }

    "throw error on illegal characters in strings" in {
      val input = "this has bad characters; it's no good!"
      val expected = "thishasbadcharactersitsnogood"
      val ex = intercept[IllegalIdentifierException] {
        validateSql(input)
      }
      assertResult(
        "Illegal identifier used in BigQuery SQL. Original input was like [this_has_bad_characters__it_s_no_good_]"
      ) {
        ex.getMessage
      }
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
        val isLegal = isLegalSqlString(input)

        if (isLegal) {
          // input is legal; validate function should return it unchanged
          assertResult(input)(validateSql(input))
        } else {
          val ex = intercept[IllegalIdentifierException](validateSql(input))
          assert(ex.getMessage.startsWith("Illegal identifier used in BigQuery SQL."))
          // also assert that our output message does not echo any unsafe chars
          val outputMessage = ex.getMessage
            .replace("Illegal identifier used in BigQuery SQL. Original input was like [", "")
            .replace("]", "")
          assert(isLegalSqlString(outputMessage), "output message echoed an illegal character")
        }

      }
    }

  }

}
