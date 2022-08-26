package org.broadinstitute.dsde.rawls.expressions

// test data for Expression Validation and Parsing

trait LocalExpressionFixture {
  // NOTE: empty strings are handled specially by the validator, so they don't need to be handled by the parser

  val parseableInputExpressionsWithNoRoot: Seq[String] = Seq(
    "workspace.gvcf",
    "workspace.with-dash",
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
    """["a",{"more":{"elaborate":"example"}}]""",
    """{"more":{"elaborate":{"reference1": "val1", "reference2": "val2", "path":"gs://abc/123"}}}"""
  )

  val parseableInputExpressionsWithRoot: Seq[String] = Seq(
    "this.gvcf",
    "this.with-dash",
    "this.library:cohort",
    "this.library:cohort1",
    "this.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    "this.underscores_are_ok",
    "this._",
    """["foo","bar", this.valid]""",
    """["a",{"more":{"elaborate":this.example}}]""",
    """{"more":{"elaborate":{"reference1": this.val1, "path":"gs://abc/123"}}}""",
    """{"level1": "easy", "other-levels": {"level2": this.library, "level3": [this.library:cohort, "extremely difficult", this.library:cohort.entity]}}""",
    """["foo", "bar", 123, ["array", this.values, this.library:cohort], false]""",
    """["foo", "bar", 123, ["array", this.values, [this.library:cohort]], false, ["abc", this.foo]]"""
  )

  val parseableInputExpressions: Seq[String] = parseableInputExpressionsWithNoRoot ++ parseableInputExpressionsWithRoot

  val unparseableInputExpressions: Seq[String] = Seq(
    "this.",
    "this.bad|character",
    "this..wont.parse",
    "workspace.",
    "workspace........",
    "where_does_this_even_go",
    "gs://buckets-arent-expressions/nope",
    "*",
    """["foo","bar", notValid]""",
    """{"city": Boston}"""
  )

  val unparseableInputExpressionsWithNoRoot: Seq[String] =
    unparseableInputExpressions ++ parseableInputExpressionsWithRoot

  val parseableOutputExpressionsWithNoRoot: Seq[String] = Seq(
    "workspace.gvcf",
    "workspace.library:cohort",
    "workspace.arbitrary:whatever"
  )

  val parseableOutputExpressionsWithRoot: Seq[String] = Seq(
    "this.gvcf",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "this.underscores_are_ok",
    "this.with-dash"
  )

  val parseableOutputExpressions: Seq[String] =
    parseableOutputExpressionsWithNoRoot ++ parseableOutputExpressionsWithRoot

  val unparseableOutputExpressions: Seq[String] = Seq(
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
    """["a",{"more":{"elaborate":"example"}}]"""
  )

  val unparseableOutputExpressionsWithNoRoot: Seq[String] =
    unparseableOutputExpressions ++ parseableOutputExpressionsWithRoot
}
