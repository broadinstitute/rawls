package org.broadinstitute.dsde.rawls.model.expressions

trait ExpressionFixture {
  val validInputExpressions = Seq(
    "this.gvcf",
    "workspace.gvcf",
    "workspace.library:cohort",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "workspace.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    "workspace.yes.we.can",
    "this.hyphen-is-allowed",
    "this.underscores_are_ok",

    // compact-printed JSON
    """"a string literal"""",
    "9000",
    "-3.77",
    "true",
    """["foo","bar","horsefish"]""",
    "[1,2,3]",
    """{"key":"value"}""",
    """["a",{"more":{"elaborate":"example"}}]"""
  )

  val invalidInputExpressions = Seq(
    "this.",
    "this.bad|character",
    "this..wont.parse",
    "workspace.",
    "workspace........",
    "where_does_this_even_go",
    "gs://buckets-arent-expressions/nope",
    "*",

    // empty input expressions are not allowed
    ""
  )

  val validOutputExpressions = Seq(
    "this.gvcf",
    "workspace.gvcf",
    "workspace.library:cohort",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "workspace.arbitrary:whatever",
    "this.hyphen-is-allowed",
    "this.underscores_are_ok",

    // empty output expressions are OK
    ""
  )

  val invalidOutputExpressions = Seq(
    "this.",
    "this.bad|character",
    "this..wont.parse",
    "this.case_sample.attribute",
    "workspace.",
    "workspace........",
    "workspace.nope.nope.nope",
    "where_does_this_even_go",
    "gs://buckets-arent-expressions/nope",
    "*",

    // JSON output expressions are not allowed
    """"a string literal"""",
    "9000",
    "-3.77",
    "true",
    """["foo","bar","horsefish"]""",
    "[1,2,3]",
    """{"key":"value"}""",
    """["a",{"more":{"elaborate":"example"}}]"""
  )
}
