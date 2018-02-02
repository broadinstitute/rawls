package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.scalatest.{FreeSpec, Matchers}

class WorkspaceModelSpec extends FreeSpec with Matchers {

  "MethodRepoMethod" - {
    "Can convert to a method repo URI" - {
      "For valid methods" - {
        val method = MethodRepoMethod("test-namespace", "test-name", 555)

        "for Agora" in {
          assertResult("agora://test-namespace/test-name/555") {
            method.asAgoraMethodUrl
          }
        }

        "for some other method repo" in {
          assertResult("marks-methods-mart://test-namespace/test-name/555") {
            method.asMethodUrlForRepo("marks-methods-mart")
          }
        }
      }

      "for nasty bad methodses" in {
        val goodMethod = MethodRepoMethod("test-namespace", "test-name", 555)
        val badMethod1 = MethodRepoMethod("a", "", 1)
        val badMethod2 = MethodRepoMethod(null, "b", 1)
        val badMethod3 = MethodRepoMethod("a", "b", 0)

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
      }
    }
  }

}
