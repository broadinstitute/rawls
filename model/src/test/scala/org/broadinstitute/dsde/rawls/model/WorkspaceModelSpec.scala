package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.scalatest.{FreeSpec, Matchers}

class WorkspaceModelSpec extends FreeSpec with Matchers {

  "MethodRepoMethod" - {

    val goodMethod = MethodRepoMethod("test-namespace", "test-name", 555)
    val badMethod1 = MethodRepoMethod("a", "", 1)
    val badMethod2 = MethodRepoMethod(null, "b", 1)
    val badMethod3 = MethodRepoMethod("a", "b", 0)

    "Validation works as one would expect" - {
      "for a good method" in {
        assertResult(goodMethod.validate) {
          Some(goodMethod)
        }
      }

      "for bad methods" in {
        assertResult(badMethod1.validate) {
          None
        }
        assertResult(badMethod2.validate) {
          None
        }
        assertResult(badMethod3.validate) {
          None
        }
      }
    }

    "Can convert to a method repo URI" - {
      "For valid methods" - {

        "for Agora" in {
          assertResult("agora://test-namespace/test-name/555") {
            goodMethod.asAgoraMethodUrl
          }
        }
      }

      "for nasty bad methodses" in {

        intercept[RawlsException] {
          badMethod1.asAgoraMethodUrl
        }
        intercept[RawlsException] {
          badMethod2.asAgoraMethodUrl
        }
        intercept[RawlsException] {
          badMethod3.asAgoraMethodUrl
        }
        intercept[RawlsException] {
          goodMethod.asMethodUrlForRepo("")
        }
        intercept[RawlsException] {
          goodMethod.asMethodUrlForRepo(null)
        }
        intercept[RawlsException] {
          goodMethod.asMethodUrlForRepo("marks-methods-mart")
        }
      }

    }

    "Can be created from a method URI" - {
      val methodUri = "agora://test-namespace/test-name/555"

      "from a good URI" in {
        assertResult(MethodRepoMethod("test-namespace", "test-name", 555)) {
          MethodRepoMethod.apply(methodUri)
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
          MethodRepoMethod.apply(badUri1)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri2)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri3)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri4)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri5)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri6)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri7)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri8)
        }
        intercept[RawlsException] {
          MethodRepoMethod.apply(badUri9)
        }
      }
    }
  }

}
