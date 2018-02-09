package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.scalatest.{FreeSpec, Matchers}

class WorkspaceModelSpec extends FreeSpec with Matchers {

  val trickyBit        = "/+:?&~!@#$^*()[]{}∞€\\"
  val trickyBitEncoded = java.net.URLEncoder.encode(trickyBit, java.nio.charset.StandardCharsets.UTF_8.name)

  val nameNeedsEncoding = s"${trickyBit}test${trickyBit}name$trickyBit"
  val nameEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}name$trickyBitEncoded"

  val namespaceNeedsEncoding = s"${trickyBit}test${trickyBit}namespace$trickyBit"
  val namespaceEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}namespace$trickyBitEncoded"

  "MethodRepoMethod" - {

    val goodMethod = AgoraMethod("test-namespace", "test-name", 555)
    val goodMethodWithCharsToEncode =
      AgoraMethod(namespaceNeedsEncoding, nameNeedsEncoding, 555)
    val badMethod1 = AgoraMethod("a", "", 1)
    val badMethod2 = AgoraMethod("a", "b", 0)

    "Validation works as one would expect" - {
      "for good methods" in {
        assertResult(goodMethod.validate) {
          Some(goodMethod)
        }

        assertResult(goodMethodWithCharsToEncode.validate) {
          Some(goodMethodWithCharsToEncode)
        }
      }

      "for bad methods" in {
        assertResult(badMethod1.validate) {
          None
        }
        assertResult(badMethod2.validate) {
          None
        }
      }
    }

    "Can convert to a method repo URI" - {
      "For valid methods" - {

        "for Agora" in {
          assertResult("agora://test-namespace/test-name/555") {
            goodMethod.methodUri
          }

          assertResult(s"agora://$namespaceEncoded/$nameEncoded/555") {
            goodMethodWithCharsToEncode.methodUri
          }
        }
      }

      "for nasty bad methodses" in {

        intercept[RawlsException] {
          badMethod1.methodUri
        }
        intercept[RawlsException] {
          badMethod2.methodUri
        }
      }

    }

    "Can be created from a method URI" - {
      val methodUri = "agora://test-namespace/test-name/555"
      val methodUriWithEncodedChars = s"agora://$namespaceEncoded/$nameEncoded/555"

      "from good URIs" in {
        assertResult(AgoraMethod("test-namespace", "test-name", 555)) {
          MethodRepoMethod.fromUri(methodUri)
        }

        assertResult(AgoraMethod(namespaceNeedsEncoding, nameNeedsEncoding, 555)) {
          MethodRepoMethod.fromUri(methodUriWithEncodedChars)
        }
      }

      val badUri1 = null
      val badUri2 = ""
      val badUri3 = "agoraphobia"
      val badUri4 = "agora://"
      val badUri5 = "agora://test-namespace/test-name"
      val badUri6 = "agora://test-namespace/1"
      val badUri7 = "agora://test-namespace/test-name/bad/path"
      val badUri8 = "marks-methods-mart://test-namespace/test-name/555"
      val badUri9 = "marks-methods-mart://test-namespace/test-name/0" // proves that validation works in apply()

      "catches bad URIs" in {
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri1)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri2)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri3)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri4)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri5)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri6)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri7)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri8)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri9)
        }
      }
    }
  }

}
