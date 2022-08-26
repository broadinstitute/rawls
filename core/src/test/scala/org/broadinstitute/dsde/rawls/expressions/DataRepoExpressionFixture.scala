package org.broadinstitute.dsde.rawls.expressions

import bio.terra.datarepo.model.{ColumnModel, RelationshipModel, RelationshipTermModel, TableModel}

import scala.jdk.CollectionConverters._

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
    // forward reference to the person table
    "this.authorRelationship.person_id",
    "this.authorRelationship.favorite_book",
    "this.authorRelationship.name",
    """["foo","bar", this.authorRelationship.person_id]""",
    """["a",{"more":{"elaborate":this.authorRelationship.person_id}}]""",
    """{"more":{"elaborate":{"reference1": this.authorRelationship.person_id, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.authorRelationship.person_id], false]""",
    """["foo", "bar", 123, ["array", this.authorRelationship.person_id], false, ["abc", this.authorRelationship.favorite_book]]""",
    // backward reference to the person table
    "this.favoriteBooksRelationship.person_id",
    "this.favoriteBooksRelationship.favorite_book",
    "this.favoriteBooksRelationship.name",
    """["foo","bar", this.favoriteBooksRelationship.person_id]""",
    """["a",{"more":{"elaborate":this.favoriteBooksRelationship.person_id}}]""",
    """{"more":{"elaborate":{"reference1": this.favoriteBooksRelationship.person_id, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.favoriteBooksRelationship.person_id], false]""",
    """["foo", "bar", 123, ["array", this.favoriteBooksRelationship.person_id], false, ["abc", this.favoriteBooksRelationship.favorite_book]]""",
    // forward reference to the person table, then a (backward) reference from person to publisher table
    "this.authorRelationship.publisherOwnerRelationship.publisher_id",
    "this.authorRelationship.publisherOwnerRelationship.owner",
    """["foo","bar", this.authorRelationship.publisherOwnerRelationship.publisher_id]""",
    """["a",{"more":{"elaborate":this.authorRelationship.publisherOwnerRelationship.publisher_id}}]""",
    """{"more":{"elaborate":{"reference1": this.authorRelationship.publisherOwnerRelationship.publisher_id, "path":"gs://abc/123"}}}""",
    """["foo", "bar", 123, ["array", this.authorRelationship.publisherOwnerRelationship.publisher_id], false]""",
    """["foo", "bar", 123, ["array", this.authorRelationship.publisherOwnerRelationship.publisher_id], false, ["abc", this.authorRelationship.publisherOwnerRelationship.owner]]"""
  )
  val validInputExpressionsWithNoRootWithRelationships: Seq[String] =
    validWorkspaceInputExpressions ++ validJsonInputExpressions
  val validInputExpressionsWithRelationships: Seq[String] =
    validInputExpressionsWithNoRootWithRelationships ++ validEntityInputExpressionsWithRelationships

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

  // parseable input expressions that are invalid (don't fit the schema, relations, any other reason?)
  val invalidInputExpressions: Seq[String] = Seq(
    "this.column_does_not_exist",
    "this.relationship_does_not_exist.person_id",
    "this.authorRelationship.column_does_not_exist",
    "this.library:cohort", // namespace:name is not allowed for BQ column/table names
    "this.library:cohort1",
    "this.arbitrary:whatever",
    "this.case_sample.foo:ref.bar:attribute",
    """{"level1": "easy", "other-levels": {"level2": this.gvcf, "level3": [this.library:cohort, "extremely difficult", this.library:cohort.entity]}}""",
    """["foo", "bar", 123, ["array", this.gvcf, this.library:cohort], false]""",
    """["foo", "bar", 123, ["array", this.gvcf, [this.library:cohort]], false, ["abc", this.with-dash]]""",
    """["foo", "bar", 123, ["array", this.authorRelationship.name, this.authorRelationship.library:person_id], false]""",
    """["foo", "bar", 123, ["array", this.authorRelationship.name, [this.authorRelationship.library:person_id]], false, ["abc", this.authorRelationship.favorite_book]]"""
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
    """["a",{"more":{"elaborate":"example"}}]"""
  )

  val validOutputExpressions: Seq[String] = validWorkspaceOutputExpressions

  val invalidOutputExpressions: Seq[String] = unparseableOutputExpressions ++ invalidEntityOutputExpressions

  /** TABLES AND COLUMNS */
  val defaultFixtureRootTableName = "rootTable"
  // These should reflect the columns used in validEntityInputExpressions above
  val defaultFixtureRootTableColumns = List("gvcf", "with-dash", "underscores_are_ok", "_", "case_sample")
  val defaultFixtureTables: List[TableModel] = List(
    new TableModel()
      .name(defaultFixtureRootTableName)
      .columns(defaultFixtureRootTableColumns.map(new ColumnModel().name(_)).asJava)
  )

  /**
    * Relationship tables
    *
    * Diagram for reference:
    * +----------+         +-------------+         +---------------+
    * |book table|         |person table |         |publisher table|
    * |----------|         |-------------|         |---------------|
    * |book_id   |<-------+|favorite book|         |publisher_id   |
    * |title     |         |name         |         |name           |
    * |author    |+------->|person_id    |<-------+|owner          |
    * +----------+         +-------------+         +---------------+
    */
  val bookTableName = "bookTable"
  val bookTableColumns = List("book_id", "title", "author")

  val personTableName = "personTable"
  val personTableColumns = List("person_id", "name", "favorite_book")

  val publisherTableName = "publisherTable"
  val publisherTableColumns = List("publisher_id", "name", "owner")

  val bookReference: RelationshipTermModel = new RelationshipTermModel().table(bookTableName).column("book_id")
  val personReference: RelationshipTermModel = new RelationshipTermModel().table(personTableName).column("person_id")
  val authorReference: RelationshipTermModel = new RelationshipTermModel().table(bookTableName).column("author")
  val favoriteBooksReference: RelationshipTermModel =
    new RelationshipTermModel().table(personTableName).column("favorite_book")
  val publisherOwnerReference: RelationshipTermModel =
    new RelationshipTermModel().table(publisherTableName).column("owner")

  val authorRelationship: RelationshipModel =
    new RelationshipModel().name("authorRelationship").from(authorReference).to(personReference)
  val favoriteBooksRelationship: RelationshipModel =
    new RelationshipModel().name("favoriteBooksRelationship").from(favoriteBooksReference).to(bookReference)
  val publisherOwnerRelationship: RelationshipModel =
    new RelationshipModel().name("publisherOwnerRelationship").from(publisherOwnerReference).to(personReference)

  val relationships: List[RelationshipModel] =
    List(authorRelationship, publisherOwnerRelationship, favoriteBooksRelationship)

  val relationshipTables: List[TableModel] = List(
    new TableModel().name(bookTableName).columns(bookTableColumns.map(new ColumnModel().name(_)).asJava),
    new TableModel().name(personTableName).columns(personTableColumns.map(new ColumnModel().name(_)).asJava),
    new TableModel().name(publisherTableName).columns(publisherTableColumns.map(new ColumnModel().name(_)).asJava)
  )
}
