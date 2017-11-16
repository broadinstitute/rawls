package org.broadinstitute.dsde.rawls.expressions

// test data for Expression Validation and Parsing

trait ExpressionFixture {
  // NOTE: empty strings are handled specially by the validator, so they don't need to be handled by the parser

  val parseableInputExpressions = Seq(
    "this.gvcf",
    "workspace.gvcf",
    "workspace.library:cohort",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "workspace.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    "workspace.yes.we.can",
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

  val unparseableInputExpressions = Seq(
    "this.",
    "this.bad|character",
    "this..wont.parse",
    "workspace.",
    "workspace........",
    "where_does_this_even_go",
    "gs://buckets-arent-expressions/nope",
    "*",

    // oops, it isn't.  GAWB-2598
    "this.hyphen-is-allowed"
  )

  val parseableOutputExpressions = Seq(
    "this.gvcf",
    "workspace.gvcf",
    "workspace.library:cohort",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "workspace.arbitrary:whatever",
    "this.underscores_are_ok"
  )

  val unparseableOutputExpressions = Seq(
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

    """"a string literal"""",
    "9000",
    "-3.77",
    "true",
    """["foo","bar","horsefish"]""",
    "[1,2,3]",
    """{"key":"value"}""",
    """["a",{"more":{"elaborate":"example"}}]""",

    // oops, it isn't.  GAWB-2598
    "this.hyphen-is-allowed"
  )
}
