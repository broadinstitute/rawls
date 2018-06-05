package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.scalatest.{FreeSpec, Matchers}

import spray.json._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.{MethodRepoMethodFormat, MethodConfigurationFormat}

class WorkspaceModelSpec extends FreeSpec with Matchers {

  val trickyBit        = "/+:?&~!@#$^*()[]{}∞€\\"
  val trickyBitEncoded = java.net.URLEncoder.encode(trickyBit, java.nio.charset.StandardCharsets.UTF_8.name)

  "MethodRepoMethod" - {

    "apply() does the right thing" in {
      assertResult(MethodRepoMethod.apply("path", "version")) {
        DockstoreMethod("path", "version")
      }
      assertResult(MethodRepoMethod.apply("namespace", "name", 555)) {
        AgoraMethod("namespace", "name", 555)
      }
    }

    "JSON logic" - {

      "Serialize" - {

        "Agora" in {
          assertResult {
            """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"agora"}""".parseJson
          } {
            MethodRepoMethodFormat.write(AgoraMethod("test-namespace", "test-name", 555))
          }
        }

        "Dockstore" in {
          assertResult {
            """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
          } {
            MethodRepoMethodFormat.write(DockstoreMethod("test-path", "test-version"))
          }
        }
      }

      "Deserialize" - {

        "Agora" in {

          // Round-trip
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"agora"}""".parseJson)
          }

          // URI takes precedence
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"not-the-name","methodVersion":777,"methodNamespace":"not-the-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"fake-repo"}""".parseJson)
          }

          // URI alone is OK
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"agora://test-namespace/test-name/555"}""".parseJson)
          }

          // Fall back to fields
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","sourceRepo":"agora"}""".parseJson)
          }

          // No "sourceRepo" -> default to Agora
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace"}""".parseJson)
          }
        }

        "Dockstore" in {

          // Round-trip
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
          }

          // URI takes precedence
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"fake-repo","methodPath":"not-the-path","methodVersion":"not-the-version"}""".parseJson)
          }

          // URI alone is OK
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"dockstore://test-path/test-version"}""".parseJson)
          }

          // Fall back to fields
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
          }

          // No "sourceRepo" -> Dockstore throws an exception because we default to Agora
          intercept[spray.json.DeserializationException] {
            MethodRepoMethodFormat.read(
              """{"methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
          }
        }

        // Bad "sourceRepo"
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read(
            """{"sourceRepo":"marks-methods-mart","methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
        }

        // Bad "sourceRepo"
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read(
            """{"sourceRepo":777,"methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
        }

        // Bad repo in URI
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read(
            """{"methodUri":"marks-methods-mart://test-path/test-version"}""".parseJson)
        }
      }
    }

  }

  "AgoraMethod" - {

    val nameNeedsEncoding = s"${trickyBit}test${trickyBit}name$trickyBit"
    val nameEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}name$trickyBitEncoded"

    val namespaceNeedsEncoding = s"${trickyBit}test${trickyBit}namespace$trickyBit"
    val namespaceEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}namespace$trickyBitEncoded"


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

  "DockstoreMethod" - {

    val pathNeedsEncoding = s"${trickyBit}test${trickyBit}path$trickyBit"
    val pathEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}path$trickyBitEncoded"

    val versionNeedsEncoding = s"${trickyBit}test${trickyBit}version$trickyBit"
    val versionEncoded       = s"${trickyBitEncoded}test${trickyBitEncoded}version$trickyBitEncoded"


    val goodMethod = DockstoreMethod("test-path", "test-version")
    val goodMethodWithCharsToEncode =
      DockstoreMethod(pathNeedsEncoding, versionNeedsEncoding)
    val badMethod1 = DockstoreMethod("a", "")
    val badMethod2 = DockstoreMethod("", "b")

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

        "for Dockstore" in {
          assertResult("dockstore://test-path/test-version") {
            goodMethod.methodUri
          }

          assertResult(s"dockstore://$pathEncoded/$versionEncoded") {
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
      val methodUri = "dockstore://test-path/test-version"
      val methodUriWithEncodedChars = s"dockstore://$pathEncoded/$versionEncoded"

      "from good URIs" in {
        assertResult(DockstoreMethod("test-path", "test-version")) {
          MethodRepoMethod.fromUri(methodUri)
        }

        assertResult(DockstoreMethod(pathNeedsEncoding, versionNeedsEncoding)) {
          MethodRepoMethod.fromUri(methodUriWithEncodedChars)
        }
      }

      val badUri1 = null
      val badUri2 = ""
      val badUri3 = "dockstoreblah"
      val badUri4 = "dockstore://"
      val badUri5 = "dockstore://path-but-no-version"
      val badUri6 = "marks-methods-mart://test-path/test-version"
      val badUri7 = "marks-methods-mart://test-path"

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
      }
    }
  }

  "MethodConfigurations" - {
    "should treat empty strings in inputs and outputs as unspecified" in {
      val mcBefore = MethodConfiguration("ns", "name", Some("sample"), Map(),
        Map("optional" -> AttributeString(""), "real" -> AttributeString("this.blah")),
        Map("optional" -> AttributeString(""), "real" -> AttributeString("this.blah")),
        MethodRepoMethod("path", "version")
      )

      val mcJson = mcBefore.toJson
      val mcAfter = MethodConfigurationFormat.read(mcJson)

      mcAfter shouldEqual MethodConfiguration("ns", "name", Some("sample"), Map(),
        Map("real" -> AttributeString("this.blah")),
        Map("real" -> AttributeString("this.blah")),
        MethodRepoMethod("path", "version")
      )
    }
  }

}
