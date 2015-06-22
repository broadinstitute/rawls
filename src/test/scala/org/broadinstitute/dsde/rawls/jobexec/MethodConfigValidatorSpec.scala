package org.broadinstitute.dsde.rawls.jobexec

import org.scalatest.{WordSpecLike, Matchers, FlatSpec}

class MethodConfigValidatorSpec extends WordSpecLike with Matchers {

  val littleWdl =
    """
      |task t1 {
      |  command {
      |    echo ${Int int_arg}
      |  }
      |}
      |
      |workflow w1 {
      |  call t1
      |}
    """.stripMargin

  val intArgName = "w1.t1.int_arg"

  "MethodConfigValidator" should {
    "validate inputs against WDL" in {
      MethodConfigValidator.getValidationErrors(Map(intArgName -> 1), littleWdl).get(intArgName) should be (None) // Valid input
      MethodConfigValidator.getValidationErrors(Map("notAnArg" -> 1), littleWdl).get(intArgName) shouldNot be (None) // Missing argument
      MethodConfigValidator.getValidationErrors(Map(intArgName -> "foo"), littleWdl).get(intArgName) shouldNot be (None) // Wrong type
    }
  }

  // TODO more test cases
}
