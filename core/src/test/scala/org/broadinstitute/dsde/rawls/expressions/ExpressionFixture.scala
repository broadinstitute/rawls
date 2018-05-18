package org.broadinstitute.dsde.rawls.expressions

// test data for Expression Validation and Parsing

trait ExpressionFixture {
  // NOTE: empty strings are handled specially by the validator, so they don't need to be handled by the parser

  val parseableInputExpressionsWithNoRoot = Seq(
    "workspace.gvcf",
    "workspace.library:cohort",
    "workspace.arbitrary:whatever",
    "workspace.yes.we.can",
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

  val parseableInputExpressionsWithRoot = Seq(
    "this.gvcf",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    "this.underscores_are_ok"
  )

  val parseableInputExpressions = parseableInputExpressionsWithNoRoot ++ parseableInputExpressionsWithRoot

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

  val unparseableInputExpressionsWithNoRoot = unparseableInputExpressions ++ parseableInputExpressionsWithRoot

  val parseableOutputExpressionsWithNoRoot = Seq(
    "workspace.gvcf",
    "workspace.library:cohort",
    "workspace.arbitrary:whatever"
  )

  val parseableOutputExpressionsWithRoot = Seq(
    "this.gvcf",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "this.underscores_are_ok"
  )

  val parseableOutputExpressions = parseableOutputExpressionsWithNoRoot ++ parseableOutputExpressionsWithRoot

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

  val unparseableOutputExpressionsWithNoRoot = unparseableOutputExpressions ++ parseableOutputExpressionsWithRoot
}
