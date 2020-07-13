package org.broadinstitute.dsde.rawls.expressions

// test data for Data Repo Expression Validation
trait DataRepoExpressionFixture {
  /** INPUT EXPRESSIONS */
  val validWorkspaceInputExpressions: Seq[String] = Seq(
    "workspace.gvcf",
    "workspace.with-dash",
    "workspace.library:cohort",
    "workspace.arbitrary:whatever",
    "workspace.yes.we.can",
    """["a",{"more":{"elaborate":"workspace.example"}}]"""
  )

  val validJsonInputExpressions: Seq[String] = Seq(
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


  val validEntityInputExpressions: Seq[String] = Seq(
    "this.gvcf",
    "this.with-dash",
    "this.library:cohort",
    "this.library:cohort1",
    "this.arbitrary:whatever",
    "this.underscores_are_ok",
    "this._",
    """["foo","bar", this.gvcf]""",
    """["a",{"more":{"elaborate":this.gvcf}}]""",
    """{"more":{"elaborate":{"reference1": this.gvcf, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.gvcf, this.library:cohort], false]""",
    """["foo", "bar", 123, ["array", this.gvcf, [this.library:cohort]], false, ["abc", this.with-dash]]"""
  )

  val validInputExpressionsWithNoRoot: Seq[String] = validWorkspaceInputExpressions ++ validJsonInputExpressions

  val validInputExpressions: Seq[String] = validInputExpressionsWithNoRoot ++ validEntityInputExpressions

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
    """{"city": Boston}""",
  )

  // parseable input expressions that are invalid (don't fit the schema, relations, any other reason?)
  val invalidInputExpressions: Seq[String] = Seq(
    "this.column_does_not_exist",
    "this.case_sample.foo:ref.bar:attribute", // invalid for now -- relations
    """{"level1": "easy", "other-levels": {"level2": this.gvcf, "level3": [this.library:cohort, "extremely difficult", this.library:cohort.entity]}}""", // invalid -- relations
  )

  val badInputExpressionsWithRoot: Seq[String] = invalidInputExpressions ++ unparseableInputExpressions

  val badInputExpressionsWithNoRoot: Seq[String] = badInputExpressionsWithRoot ++ validEntityInputExpressions


  /** OUTPUT EXPRESSIONS */
  val validWorkspaceOutputExpressions: Seq[String] = Seq(
    "workspace.gvcf",
    "workspace.library:cohort",
    "workspace.arbitrary:whatever"
  )

  // these are invalid now since we don't have TDR support for entity output expressions
  val invalidEntityOutputExpressions: Seq[String] = Seq(
    "this.gvcf",
    "this.library:cohort",
    "this.arbitrary:whatever",
    "this.underscores_are_ok",
    "this.with-dash"
  )

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
    """["a",{"more":{"elaborate":"example"}}]""",
  )

  val validOutputExpressions: Seq[String] = validWorkspaceOutputExpressions

  val invalidOutputExpressions: Seq[String] = unparseableOutputExpressions ++ invalidEntityOutputExpressions
}