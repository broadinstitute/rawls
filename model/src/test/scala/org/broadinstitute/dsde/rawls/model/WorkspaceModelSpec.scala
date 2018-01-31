package org.broadinstitute.dsde.rawls.model

import org.scalatest.{FreeSpec, Matchers}

class WorkspaceModelSpec extends FreeSpec with Matchers {

  "MethodRepoMethod" - {
    "Can convert to a method repo URI" - {
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

    "Can be created from a method URI" - {
      val methodUri = "agora://test-namespace/test-name/555"

      "from a good URI" in {
        assertResult(MethodRepoMethod("test-namespace", "test-name", 555)) {
          MethodRepoMethod.apply(methodUri)
        }
      }

      // TODO: test error handling once it's implemented
    }
  }

}
