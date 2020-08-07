package org.broadinstitute.dsde.rawls.expressions

import bio.terra.datarepo.model.{ColumnModel, TableModel}

import scala.collection.JavaConverters._

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


  // When adding values to this seq, remember to update defaultFixtureRootTableColumns to update the test table schema
  val validEntityInputExpressions: Seq[String] = Seq(
    "this.gvcf",
    "this.with-dash",
    "this.underscores_are_ok",
    "this._",
    """["foo","bar", this.gvcf]""",
    """["a",{"more":{"elaborate":this.gvcf}}]""",
    """{"more":{"elaborate":{"reference1": this.gvcf, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.gvcf], false]""",
    """["foo", "bar", 123, ["array", this.gvcf], false, ["abc", this.with-dash]]"""
  )

  val validInputExpressionsWithNoRoot: Seq[String] = validWorkspaceInputExpressions ++ validJsonInputExpressions

  val validInputExpressions: Seq[String] = validInputExpressionsWithNoRoot ++ validEntityInputExpressions

  val validEntityInputExpressionsWithRelationships: Seq[String] = Seq(
    // forward reference to the second table
    "this.rootTableToSecondTable.second_table_column",
    "this.rootTableToSecondTable.second_third",
    "this.rootTableToSecondTable.second_root",
    """["foo","bar", this.rootTableToSecondTable.second_table_column]""",
    """["a",{"more":{"elaborate":this.rootTableToSecondTable.second_table_column}}]""",
    """{"more":{"elaborate":{"reference1": this.rootTableToSecondTable.second_table_column, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.second_table_column], false]""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.second_table_column], false, ["abc", this.rootTableToSecondTable.second_third]]""",
    // backward reference to the second table
    "this.secondTableToRootTable.second_table_column",
    "this.secondTableToRootTable.second_third",
    "this.secondTableToRootTable.second_root",
    """["foo","bar", this.secondTableToRootTable.second_table_column]""",
    """["a",{"more":{"elaborate":this.secondTableToRootTable.second_table_column}}]""",
    """{"more":{"elaborate":{"reference1": this.secondTableToRootTable.second_table_column, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.secondTableToRootTable.second_table_column], false]""",
    """["foo", "bar", 123, ["array", this.secondTableToRootTable.second_table_column], false, ["abc", this.secondTableToRootTable.second_third]]""",
    // forward and backward references to the third table
    "this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column",
    "this.rootTableToSecondTable.thirdTableToSecondTable.third_second",
    """["foo","bar", this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column]""",
    """["a",{"more":{"elaborate":this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column}}]""",
    """{"more":{"elaborate":{"reference1": this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column], false]""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.thirdTableToSecondTable.third_table_column], false, ["abc", this.rootTableToSecondTable.thirdTableToSecondTable.third_second]]"""


  )
  val validInputExpressionsWithNoRootWithRelationships: Seq[String] = validWorkspaceInputExpressions ++ validJsonInputExpressions
  val validInputExpressionsWithRelationships: Seq[String] = validInputExpressionsWithNoRootWithRelationships ++ validEntityInputExpressionsWithRelationships

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
    "this.relationship_does_not_exist.second_table_column",
    "this.rootTableToSecondTable.column_does_not_exist",
    "this.library:cohort", // namespace:name is not allowed for BQ column/table names
    "this.library:cohort1",
    "this.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    """{"level1": "easy", "other-levels": {"level2": this.gvcf, "level3": [this.library:cohort, "extremely difficult", this.library:cohort.entity]}}""",
    """["foo", "bar", 123, ["array", this.gvcf, this.library:cohort], false]""",
    """["foo", "bar", 123, ["array", this.gvcf, [this.library:cohort]], false, ["abc", this.with-dash]]""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.second_root, this.rootTableToSecondTable.library:second_table_column], false]""",
    """["foo", "bar", 123, ["array", this.rootTableToSecondTable.second_root, [this.rootTableToSecondTable.library:second_table_column]], false, ["abc", this.rootTableToSecondTable.second_third]]"""
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

  /** TABLES AND COLUMNS */
  val defaultFixtureRootTableName = "rootTable"
  // These should reflect the columns used in validEntityInputExpressions above
  val defaultFixtureRootTableColumns = List("gvcf", "with-dash", "underscores_are_ok", "_", "case_sample")
  val defaultFixtureTables: List[TableModel] = List(
    new TableModel().name(defaultFixtureRootTableName).columns(defaultFixtureRootTableColumns.map(new ColumnModel().name(_)).asJava)
  )

  val rootTableName = "rootTable"
  val rootTableColumns = List("root_table_column", "gvcf", "with-dash", "underscores_are_ok", "_", "case_sample", "root_second")

  val secondTableName = "secondTable"
  val secondTableColumns = List("second_table_column", "second_third", "second_root")

  val thirdTableName = "thirdTable"
  val thirdTableColumns = List("third_table_column", "third_second")

  val multipleTables: List[TableModel] = List(
    new TableModel().name(rootTableName).columns(rootTableColumns.map(new ColumnModel().name(_)).asJava),
    new TableModel().name(secondTableName).columns(secondTableColumns.map(new ColumnModel().name(_)).asJava),
    new TableModel().name(thirdTableName).columns(thirdTableColumns.map(new ColumnModel().name(_)).asJava)
  )
}
