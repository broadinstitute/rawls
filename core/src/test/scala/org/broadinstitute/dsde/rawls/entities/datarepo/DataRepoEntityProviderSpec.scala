package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{ColumnModel, RelationshipModel, RelationshipTermModel, TableModel}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.HttpDataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.ExpressionAndResult
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoBigQuerySupport._
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException, EntityTypeNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ParsedEntityLookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AttributeNumber, AttributeString, AttributeValue, AttributeValueRawJson, DataReferenceName, Entity, EntityTypeMetadata, GoogleProjectId, SubmissionValidationEntityInputs, SubmissionValidationValue}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.Header
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Random, Success}

class DataRepoEntityProviderSpec
    extends AsyncFlatSpec
    with DataRepoEntityProviderSpecSupport
    with TestDriverComponent
    with Matchers {

  implicit override val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "DataRepoEntityProvider.googleProject"

  it should "use an explicit project if one was provided" in {
    val randStr = java.util.UUID.randomUUID().toString
    val gProject = GoogleProjectId(randStr)
    // arguments include an explicit billingProject
    val args = EntityRequestArguments(workspace = workspace,
                                      ctx = testContext,
                                      dataReference = Option(DataReferenceName("referenceName")),
                                      billingProject = Option(gProject)
    )
    val provider = createTestProvider(entityRequestArguments = args)
    provider.googleProject should be(gProject)
  }

  it should "use the workspace's project if no explicit project was provided" in {
    val randStr = java.util.UUID.randomUUID().toString
    val gProject = GoogleProjectId(randStr)
    val testWorkspace = workspace.copy(googleProjectId = gProject)
    // arguments specify None for billingProject, but pass our random string inside the workspace
    val args = EntityRequestArguments(workspace = testWorkspace,
                                      ctx = testContext,
                                      dataReference = Option(DataReferenceName("referenceName")),
                                      billingProject = None
    )
    val provider = createTestProvider(entityRequestArguments = args)
    provider.googleProject should be(gProject)
  }

  behavior of "DataEntityProvider.entityTypeMetadata()"

  it should "return entity type metadata in the golden path" in {
    // N.B. due to the DataRepoEntityProviderSpecSupport.defaultTables fixture data, this test also asserts on:
    // - empty list returned for columns on a table
    // - null PK returned for table, defaults to datarepo_row_id
    // - compound PK returned for table, defaults to datarepo_row_id
    // - single PK returned for table is honored
    // - row counts returned for table are honored
    // - datarepo_row_id is added to columns if not included and not pk

    val provider = createTestProvider()

    provider.entityTypeMetadata() map { metadata: Map[String, EntityTypeMetadata] =>
      // this is the default expected value, should it move to the support trait?
      val expected = Map(
        ("table1", EntityTypeMetadata(10, "datarepo_row_id", Seq("integer-field", "boolean-field", "timestamp-field"))),
        ("table2", EntityTypeMetadata(123, "table2PK", Seq("col2a", "col2b"))),
        ("table3", EntityTypeMetadata(456, "datarepo_row_id", Seq("col3.1", "col3.2")))
      )
      assertResult(expected)(metadata)
    }
  }

  it should "return an empty Map if data repo snapshot has no tables" in {
    val provider = createTestProvider(snapshotModel = createSnapshotModel(List.empty[TableModel]))

    provider.entityTypeMetadata() map { metadata: Map[String, EntityTypeMetadata] =>
      assert(metadata.isEmpty, "expected response data to be the empty map")
    }
  }

  behavior of "DataRepoBigQuerySupport, when finding the primary key for a table"

  it should "use primary key of `datarepo_row_id` if snapshot has null primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(null)
    assertResult("datarepo_row_id")(createTestProvider().pkFromSnapshotTable(input))
  }

  it should "use primary key of `datarepo_row_id` if snapshot has empty-array primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(List.empty[String].asJava)
    assertResult("datarepo_row_id")(createTestProvider().pkFromSnapshotTable(input))
  }

  it should "use primary key of `datarepo_row_id` if snapshot has multiple primary keys" in {
    val input = new TableModel()
    input.setPrimaryKey(List("one", "two", "three").asJava)
    assertResult("datarepo_row_id")(createTestProvider().pkFromSnapshotTable(input))
  }

  it should "use primary key from snapshot if one and only one returned" in {
    val input = new TableModel()
    input.setPrimaryKey(List("singlekey").asJava)
    assertResult("singlekey")(createTestProvider().pkFromSnapshotTable(input))
  }

  // to-do: tests for entity/row counts returned by data repo, once TDR supports this (see DR-1003)

  behavior of "DataEntityProvider.getEntity()"

  it should "return exactly one entity if all OK" in {
    val tableRowCount = 1

    // set up a provider with a mock that returns exactly one BQ row
    val provider =
      createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(createTestTableResult(tableRowCount))))
    provider.getEntity("table1", "Row0") map { entity: Entity =>
      // this is the default expected value, should it move to the support trait?
      val expected = Entity(
        "Row0",
        "table1",
        Map(
          AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString("Row0"),
          AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
          AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
        )
      )
      assertResult(expected)(entity)
    }
  }

  it should "fail if pet credentials not available from Sam" in {
    val provider = createTestProvider(samDAO = new SpecSamDAO(petKeyForUserResponse = Left(new Exception("sam error"))))

    val futureEx = recoverToExceptionIf[Exception] {
      provider.getEntity("table1", "Row0")
    }
    futureEx map { ex =>
      assertResult(
        s"Error attempting to use project ${provider.googleProject}. The project does not exist or you do not have permission to use it: sam error"
      )(ex.getMessage)
    }
  }

  ignore should "fail if user is a workspace Reader but did not specify a billing project (canCompute?)" in {
    // we haven't implemented the runtime logic for this because we don't have PO input,
    // so we don't know exactly what to unit test
    fail("not implemented in runtime code yet")
  }

  it should "fail if snapshot table not found in data repo's response" in {
    val provider = createTestProvider() // default behavior returns three rows

    val ex = intercept[EntityTypeNotFoundException] {
      provider.getEntity("this_table_is_unknown", "Row0")
    }
    assertResult("this_table_is_unknown")(ex.requestedType)
  }

  it should "bubble up error if BigQuery errors" in {
    val provider = createTestProvider(
      bqFactory = MockBigQueryServiceFactory.ioFactory(Left(new BigQueryException(555, "unit test exception message")))
    )

    val futureEx = recoverToExceptionIf[BigQueryException] {
      provider.getEntity("table1", "Row0")
    }
    futureEx map { ex =>
      assertResult("unit test exception message")(ex.getMessage)
    }
  }

  it should "fail if BigQuery returns zero rows" in {
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List.empty[FieldValueList].asJava)
    val tableResult: TableResult = new TableResult(Schema.of(List.empty[Field].asJava), 0, page)

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    val futureEx = recoverToExceptionIf[EntityNotFoundException] {
      provider.getEntity("table1", "Row0")
    }
    futureEx map { ex =>
      assertResult("Entity not found.")(ex.getMessage)
    }
  }

  it should "fail if BigQuery returns more than one" in {
    val provider = createTestProvider() // default behavior returns three rows

    val futureEx = recoverToExceptionIf[DataEntityException] {
      provider.getEntity("table1", "Row0")
    }
    futureEx map { ex =>
      assertResult("Query succeeded, but returned 3 rows; expected one row.")(ex.getMessage)
    }
  }

  behavior of "DataEntityProvider.evaluateExpressions()"

  it should "do the happy path for basic expressions" in {
    val tableRowCount = 3
    val stringKeys = createKeyList(tableRowCount)

    val tableResult = createTestTableResult(tableRowCount)
    // set up a provider with a mock that returns ..
    val provider = createTestProvider(
      snapshotModel = createSnapshotModel(
        List(
          new TableModel()
            .name("table1")
            .primaryKey(null)
            .rowCount(3)
            .columns(List("integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava)
        )
      ),
      bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult))
    )
    val expressionEvaluationContext = ExpressionEvaluationContext(None, None, None, Some("table1"))
    val gatherInputsResult = GatherInputsResult(
      Set(
        MethodInput(
          new ToolInputParameter().name("name1").valueType(new ValueType().typeName(ValueType.TypeNameEnum.INT)),
          "this.integer-field"
        ),
        MethodInput(
          new ToolInputParameter().name("name2").valueType(new ValueType().typeName(ValueType.TypeNameEnum.BOOLEAN)),
          "this.boolean-field"
        ),
        MethodInput(new ToolInputParameter()
                      .name("workspace1")
                      .valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING)),
                    "workspace.string"
        ),
        MethodInput(
          new ToolInputParameter().name("name3").valueType(new ValueType().typeName(ValueType.TypeNameEnum.OBJECT)),
          """{"foo": this.boolean-field, "bar": this.timestamp-field, "workspace": workspace.string}"""
        )
      ),
      Set.empty,
      Set.empty,
      Set.empty
    )

    provider.evaluateExpressions(expressionEvaluationContext,
                                 gatherInputsResult,
                                 Map("workspace.string" -> Success(List(AttributeString("workspaceValue"))))
    ) map { submissionValidationEntityInputs =>
      val expectedResults = stringKeys map { stringKey =>
        SubmissionValidationEntityInputs(
          stringKey,
          Set(
            SubmissionValidationValue(Some(AttributeNumber(MockBigQueryServiceFactory.FV_INTEGER.getNumericValue)),
                                      None,
                                      "name1"
            ),
            SubmissionValidationValue(Some(AttributeBoolean(MockBigQueryServiceFactory.FV_BOOLEAN.getBooleanValue)),
                                      None,
                                      "name2"
            ),
            SubmissionValidationValue(Some(AttributeString("workspaceValue")), None, "workspace1"),
            SubmissionValidationValue(
              Some(
                AttributeValueRawJson(
                  s"""{"foo": ${MockBigQueryServiceFactory.FV_BOOLEAN.getBooleanValue}, "bar": "${MockBigQueryServiceFactory.FV_TIMESTAMP.getStringValue}", "workspace": "workspaceValue"}"""
                )
              ),
              None,
              "name3"
            )
          )
        )
      }
      submissionValidationEntityInputs should contain theSameElementsAs expectedResults
    }
  }

  it should "fail if the submission has more inputs than are allowed by config" in {
    val tableRowCount = 123
    val smallMaxInputsPerSubmission = 200

    val provider = createTestProvider(
      bqFactory = MockBigQueryServiceFactory.ioFactory(Right(createTestTableResult(tableRowCount))),
      config = DataRepoEntityProviderConfig(smallMaxInputsPerSubmission, maxBigQueryResponseSizeBytes, 0)
    )
    val expressionEvaluationContext = ExpressionEvaluationContext(None, None, None, Some("table2"))

    val gatherInputsResult = GatherInputsResult(
      Set(
        MethodInput(
          new ToolInputParameter().name("col2a").valueType(new ValueType().typeName(ValueType.TypeNameEnum.INT)),
          "this.col2a"
        ),
        MethodInput(
          new ToolInputParameter().name("col2b").valueType(new ValueType().typeName(ValueType.TypeNameEnum.BOOLEAN)),
          "this.col2b"
        )
      ),
      Set.empty,
      Set.empty,
      Set.empty
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        provider.evaluateExpressions(expressionEvaluationContext,
                                     gatherInputsResult,
                                     Map("workspace.string" -> Success(List(AttributeString("workspaceValue"))))
        ),
        Duration.Inf
      )
    }.errorReport.message should be(
      s"Too many results. Snapshot row count * number of entity expressions cannot exceed ${smallMaxInputsPerSubmission}."
    )
  }

  behavior of "DataEntityProvider.convertToListAndCheckSize()"

  it should "fail if the stream is too big" in {
    val size = 1000
    val provider = createTestProvider(config = DataRepoEntityProviderConfig(0, size, 0))

    intercept[DataEntityException] {
      provider.convertToListAndCheckSize(Stream.fill(size)(("", Map("" -> Success(List(AttributeNumber(2)))))), size)
    }.getMessage should be(
      s"Query returned too many results likely due to either large one-to-many relationships or arrays. The limit on the total number bytes is $size."
    )
  }

  it should "return the right items in expected order" in {
    val provider = createTestProvider()
    val size = 100
    val expectedResult = List.fill(size)(
      (Random.nextString(5), Map(Random.nextString(5) -> Success(List(AttributeNumber(Random.nextDouble())))))
    )
    provider.convertToListAndCheckSize(expectedResult.toStream,
                                       size
    ) should contain theSameElementsInOrderAs expectedResult
  }

  behavior of "DataEntityProvider.figureOutQueryStructureForExpressions()"

  it should "output a single SelectAndFrom when there are no lookup expressions" in {
    val testTables = defaultTables
    val snapshotModel = createSnapshotModel(testTables)
    val provider = createTestProvider(snapshotModel)

    val tableName = testTables.head.getName
    val alias = "blarg"

    val parsedExpressions = Set.empty[ParsedEntityLookupExpression]

    val entityTable = EntityTable(snapshotModel, tableName, alias)
    val result =
      provider.figureOutQueryStructureForExpressions(snapshotModel, entityTable, parsedExpressions, datarepoRowIdColumn)
    result should contain theSameElementsAs List(
      SelectAndFrom(entityTable, None, Seq(EntityColumn(entityTable, datarepoRowIdColumn, false)))
    )
  }

  it should "return a single value for many expressions without relationships" in {
    val testTables = defaultTables
    val snapshotModel = createSnapshotModel(testTables)
    val provider = createTestProvider(snapshotModel)

    val tableName = testTables.head.getName
    val columnNames = testTables.head.getColumns.asScala.map(_.getName).sorted
    val alias = "blarg"

    val parsedExpressions = columnNames.map { columnName =>
      ParsedEntityLookupExpression(List.empty, columnName, s"this.$columnName")
    }.toSet

    val entityTable = EntityTable(snapshotModel, tableName, alias)
    val result =
      provider.figureOutQueryStructureForExpressions(snapshotModel, entityTable, parsedExpressions, datarepoRowIdColumn)
    result should contain theSameElementsAs List(
      // figureOutQueryStructureForExpressions explicitly adds the datarepoRowIdColumn, so we add it here too
      SelectAndFrom(entityTable,
                    None,
                    (columnNames += datarepoRowIdColumn).sorted.toList.map((column: String) =>
                      EntityColumn(entityTable, column, false)
                    )
      )
    )
  }

  it should "return a single value for many expressions followed by another for a relationship" in {
    val joinColumnName = "donor_id"
    val rootTable = new TableModel()
      .name("donor")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("string-field", joinColumnName, "datarepo_row_id").map(new ColumnModel().name(_)).asJava)

    val dependentTable = new TableModel()
      .name("sample")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("another-string-field", joinColumnName, "datarepo_row_id").map(new ColumnModel().name(_)).asJava)

    val relationshipName = "my_donor"
    val relationship = new RelationshipModel()
      .from(new RelationshipTermModel().table(dependentTable.getName).column(joinColumnName))
      .to(new RelationshipTermModel().table(rootTable.getName).column(joinColumnName))
      .name(relationshipName)

    val testTables = List(rootTable, dependentTable)

    val snapshotModel = createSnapshotModel(testTables, List(relationship))
    val provider = createTestProvider(snapshotModel)

    val rootTableName = rootTable.getName
    val rootColumnNames = rootTable.getColumns.asScala.map(_.getName).sorted
    val dependentTableName = dependentTable.getName
    val dependentColumns = dependentTable.getColumns.asScala.map(_.getName).sorted

    val parsedExpressions = rootColumnNames.map { columnName =>
      ParsedEntityLookupExpression(List.empty, columnName, s"this.$columnName")
    } ++ dependentColumns.map { columnName =>
      ParsedEntityLookupExpression(List(relationshipName), columnName, s"this.$relationshipName.$columnName")
    }

    val rootEntityTable = EntityTable(snapshotModel, rootTableName, "root")
    val dependentEntityTable = EntityTable(snapshotModel, dependentTableName, "entity_1")

    val result = provider.figureOutQueryStructureForExpressions(snapshotModel,
                                                                rootEntityTable,
                                                                parsedExpressions.toSet,
                                                                datarepoRowIdColumn
    )
    result should contain theSameElementsInOrderAs Seq(
      SelectAndFrom(rootEntityTable,
                    None,
                    rootColumnNames.toList.map((column: String) => EntityColumn(rootEntityTable, column, false))
      ),
      SelectAndFrom(
        rootEntityTable,
        Option(
          EntityJoin(
            EntityColumn(rootEntityTable, joinColumnName, false),
            EntityColumn(dependentEntityTable, joinColumnName, false),
            Seq(relationshipName),
            "rel_2",
            false
          )
        ),
        dependentColumns.toList.map((column: String) => EntityColumn(dependentEntityTable, column, false))
      )
    )
  }

  it should "handle case when there are no root table lookup expressions" in {
    val joinColumnName = "donor_id"
    val rootTable = new TableModel()
      .name("donor")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("string-field", joinColumnName, "datarepo_row_id").map(new ColumnModel().name(_)).asJava)

    val dependentTable = new TableModel()
      .name("sample")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("another-string-field", joinColumnName, "datarepo_row_id").map(new ColumnModel().name(_)).asJava)

    val relationshipName = "my_donor"
    val relationship = new RelationshipModel()
      .from(new RelationshipTermModel().table(dependentTable.getName).column(joinColumnName))
      .to(new RelationshipTermModel().table(rootTable.getName).column(joinColumnName))
      .name(relationshipName)

    val testTables = List(rootTable, dependentTable)

    val snapshotModel = createSnapshotModel(testTables, List(relationship))
    val provider = createTestProvider(snapshotModel)

    val rootTableName = rootTable.getName
    val dependentTableName = dependentTable.getName
    val dependentColumns = dependentTable.getColumns.asScala.map(_.getName).sorted

    val parsedExpressions = dependentColumns.map { columnName =>
      ParsedEntityLookupExpression(List(relationshipName), columnName, s"this.$relationshipName.$columnName")
    }

    val rootEntityTable = EntityTable(snapshotModel, rootTableName, "root")
    val dependentEntityTable = EntityTable(snapshotModel, dependentTableName, "entity_1")

    val result = provider.figureOutQueryStructureForExpressions(snapshotModel,
                                                                rootEntityTable,
                                                                parsedExpressions.toSet,
                                                                datarepoRowIdColumn
    )
    result should contain theSameElementsInOrderAs Seq(
      SelectAndFrom(rootEntityTable, None, Seq(EntityColumn(rootEntityTable, datarepoRowIdColumn, false))),
      SelectAndFrom(
        rootEntityTable,
        Option(
          EntityJoin(
            EntityColumn(rootEntityTable, joinColumnName, false),
            EntityColumn(dependentEntityTable, joinColumnName, false),
            Seq(relationshipName),
            "rel_2",
            false
          )
        ),
        dependentColumns.toList.map((column: String) => EntityColumn(dependentEntityTable, column, false))
      )
    )
  }

  it should "handle relationship in the other direction" in {
    val joinColumnName = "donor_id"
    val rootTable = new TableModel()
      .name("donor")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "string-field", joinColumnName).map(new ColumnModel().name(_)).asJava)

    val dependentTable = new TableModel()
      .name("sample")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "another-string-field", joinColumnName).map(new ColumnModel().name(_)).asJava)

    val relationshipName = "my_donor"
    val relationship = new RelationshipModel()
      .from(new RelationshipTermModel().table(dependentTable.getName).column(joinColumnName))
      .to(new RelationshipTermModel().table(rootTable.getName).column(joinColumnName))
      .name(relationshipName)

    val testTables = List(rootTable, dependentTable)

    val snapshotModel = createSnapshotModel(testTables, List(relationship))
    val provider = createTestProvider(snapshotModel)

    val rootTableName = rootTable.getName
    val rootColumnNames = rootTable.getColumns.asScala.map(_.getName).sorted

    val dependentTableName = dependentTable.getName
    val dependentColumns = dependentTable.getColumns.asScala.map(_.getName).sorted

    // notice the swap of dependentColumns and rootColumnNames from the test above
    val parsedExpressions = dependentColumns.map { columnName =>
      ParsedEntityLookupExpression(List.empty, columnName, s"this.$columnName")
    }.toSet ++ rootColumnNames.map { columnName =>
      ParsedEntityLookupExpression(List(relationshipName), columnName, s"this.$relationshipName.$columnName")
    }

    val rootEntityTable = EntityTable(snapshotModel, rootTableName, "entity_1")
    val dependentEntityTable = EntityTable(snapshotModel, dependentTableName, "root")

    val result = provider.figureOutQueryStructureForExpressions(snapshotModel,
                                                                dependentEntityTable,
                                                                parsedExpressions,
                                                                datarepoRowIdColumn
    )
    result should contain theSameElementsInOrderAs Seq(
      SelectAndFrom(dependentEntityTable,
                    None,
                    dependentColumns.toList.map((column: String) => EntityColumn(dependentEntityTable, column, false))
      ),
      SelectAndFrom(
        dependentEntityTable,
        Option(
          EntityJoin(
            EntityColumn(dependentEntityTable, joinColumnName, false),
            EntityColumn(rootEntityTable, joinColumnName, false),
            List(relationshipName),
            "rel_2",
            false
          )
        ),
        rootColumnNames.toList.map((column: String) => EntityColumn(rootEntityTable, column, false))
      )
    )
  }

  it should "handle multiple hops with no columns selected in the middle" in {
    val donorIdColumn = "donor_id"
    val sampleIdColumn = "sample_id"
    val rootTable = new TableModel()
      .name("donor")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "string-field", donorIdColumn).map(new ColumnModel().name(_)).asJava)

    val middleTable = new TableModel()
      .name("sample")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", sampleIdColumn, donorIdColumn).map(new ColumnModel().name(_)).asJava)

    val finalTable = new TableModel()
      .name("file")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", sampleIdColumn, "path").map(new ColumnModel().name(_)).asJava)

    val relationshipName1 = "my_donor"
    val relationship1 = new RelationshipModel()
      .from(new RelationshipTermModel().table(middleTable.getName).column(donorIdColumn))
      .to(new RelationshipTermModel().table(rootTable.getName).column(donorIdColumn))
      .name(relationshipName1)

    val relationshipName2 = "sample"
    val relationship2 = new RelationshipModel()
      .from(new RelationshipTermModel().table(finalTable.getName).column(sampleIdColumn))
      .to(new RelationshipTermModel().table(middleTable.getName).column(sampleIdColumn))
      .name(relationshipName2)

    val testTables = List(rootTable, middleTable, finalTable)

    val snapshotModel = createSnapshotModel(testTables, List(relationship1, relationship2))
    val provider = createTestProvider(snapshotModel)

    val rootTableName = rootTable.getName
    val rootColumnNames = rootTable.getColumns.asScala.map(_.getName).sorted

    val finalTableName = finalTable.getName
    val finalColumns = finalTable.getColumns.asScala.map(_.getName).sorted

    val parsedExpressions = rootColumnNames.map { columnName =>
      ParsedEntityLookupExpression(List.empty, columnName, s"this.$columnName")
    }.toSet ++ finalColumns.map { columnName =>
      ParsedEntityLookupExpression(List(relationshipName1, relationshipName2),
                                   columnName,
                                   s"this.$relationshipName1.$relationshipName2.$columnName"
      )
    }

    val rootEntityTable = EntityTable(snapshotModel, rootTableName, "root")
    val middleEntityTable = EntityTable(snapshotModel, middleTable.getName, "entity_1")
    val finalEntityTable = EntityTable(snapshotModel, finalTableName, "entity_3")

    val result = provider.figureOutQueryStructureForExpressions(snapshotModel,
                                                                rootEntityTable,
                                                                parsedExpressions,
                                                                datarepoRowIdColumn
    )
    result should contain theSameElementsInOrderAs List(
      SelectAndFrom(rootEntityTable,
                    None,
                    rootColumnNames.toList.map((column: String) => EntityColumn(rootEntityTable, column, false))
      ),
      SelectAndFrom(
        rootEntityTable,
        Option(
          EntityJoin(
            EntityColumn(rootEntityTable, donorIdColumn, false),
            EntityColumn(middleEntityTable, donorIdColumn, false),
            Seq(relationshipName1),
            "rel_2",
            false
          )
        ),
        Seq.empty
      ),
      SelectAndFrom(
        middleEntityTable,
        Option(
          EntityJoin(
            EntityColumn(middleEntityTable, sampleIdColumn, false),
            EntityColumn(finalEntityTable, sampleIdColumn, false),
            Seq(relationshipName1, relationshipName2),
            "rel_4",
            false
          )
        ),
        finalColumns.toList.map((column: String) => EntityColumn(finalEntityTable, column, false))
      )
    )
  }

  it should "handle forked relationships" in {
    val fooIdColumn = "foo_id"
    val barIdColumn = "bar_id"
    val rootTable = new TableModel()
      .name("donor")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "string-field", fooIdColumn, barIdColumn).map(new ColumnModel().name(_)).asJava)

    val dependentTable1 = new TableModel()
      .name("foo")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "table_1_col", fooIdColumn).map(new ColumnModel().name(_)).asJava)

    val dependentTable2 = new TableModel()
      .name("bar")
      .primaryKey(null)
      .rowCount(0)
      .columns(List("datarepo_row_id", "table_2_col", barIdColumn).map(new ColumnModel().name(_)).asJava)

    val relationshipName1 = "fooed"
    val relationship1 = new RelationshipModel()
      .from(new RelationshipTermModel().table(rootTable.getName).column(fooIdColumn))
      .to(new RelationshipTermModel().table(dependentTable1.getName).column(fooIdColumn))
      .name(relationshipName1)

    val relationshipName2 = "bared"
    val relationship2 = new RelationshipModel()
      .from(new RelationshipTermModel().table(rootTable.getName).column(barIdColumn))
      .to(new RelationshipTermModel().table(dependentTable2.getName).column(barIdColumn))
      .name(relationshipName2)

    val testTables = List(rootTable, dependentTable1, dependentTable2)

    val snapshotModel = createSnapshotModel(testTables, List(relationship1, relationship2))
    val provider = createTestProvider(snapshotModel)

    val rootTableName = rootTable.getName

    val parsedExpressions = Set(
      ParsedEntityLookupExpression(List.empty, "string-field", "this.string-field"),
      ParsedEntityLookupExpression(List(relationshipName1), "table_1_col", "this.fooed.table_1_col"),
      ParsedEntityLookupExpression(List(relationshipName2), "table_2_col", "this.bared.table_2_col")
    )

    val rootEntityTable = EntityTable(snapshotModel, rootTableName, "root")
    val dependent1EntityTable = EntityTable(snapshotModel, dependentTable1.getName, "entity_3")
    val dependent2EntityTable = EntityTable(snapshotModel, dependentTable2.getName, "entity_1")

    val result =
      provider.figureOutQueryStructureForExpressions(snapshotModel, rootEntityTable, parsedExpressions, "string-field")
    result should contain theSameElementsInOrderAs Seq(
      SelectAndFrom(rootEntityTable, None, Seq(EntityColumn(rootEntityTable, "string-field", false))),
      SelectAndFrom(
        rootEntityTable,
        Option(
          EntityJoin(
            EntityColumn(rootEntityTable, barIdColumn, false),
            EntityColumn(dependent2EntityTable, barIdColumn, false),
            Seq(relationshipName2),
            "rel_2",
            false
          )
        ),
        Seq(EntityColumn(dependent2EntityTable, datarepoRowIdColumn, false),
            EntityColumn(dependent2EntityTable, "table_2_col", false)
        )
      ),
      SelectAndFrom(
        rootEntityTable,
        Option(
          EntityJoin(
            EntityColumn(rootEntityTable, fooIdColumn, false),
            EntityColumn(dependent1EntityTable, fooIdColumn, false),
            Seq(relationshipName1),
            "rel_4",
            false
          )
        ),
        Seq(EntityColumn(dependent1EntityTable, datarepoRowIdColumn, false),
            EntityColumn(dependent1EntityTable, "table_1_col", false)
        )
      )
    )
  }

  behavior of "DataEntityProvider.transformQueryResultToExpressionAndResult()"

  it should "handle root entity attributes" in {
    val provider = createTestProvider()
    val table = EntityTable("proj", "view", "table", "root")
    val selectAndFroms = Seq(
      SelectAndFrom(table,
                    None,
                    Seq(EntityColumn(table, F_BOOLEAN.getName, false),
                        EntityColumn(table, F_STRING.getName, false),
                        EntityColumn(table, F_INTEGER.getName, false)
                    )
      )
    )

    val parsedExpressions = Set(
      ParsedEntityLookupExpression(List.empty, F_BOOLEAN.getName, s"this.${F_BOOLEAN.getName}"),
      ParsedEntityLookupExpression(List.empty, F_INTEGER.getName, s"this.${F_INTEGER.getName}")
    )

    val tableResult = createTestTableResult(3)

    val results = provider.transformQueryResultToExpressionAndResult(datarepoRowIdColumn,
                                                                     parsedExpressions,
                                                                     selectAndFroms,
                                                                     tableResult
    )

    val expectedResults: Set[ExpressionAndResult] = for {
      expr <- parsedExpressions
      row <- tableResult.iterateAll().asScala
      field <- Seq(F_BOOLEAN, F_INTEGER) if field.getName == expr.columnName
    } yield (expr.expression,
             Map(
               row.get(datarepoRowIdColumn).getStringValue -> Success(
                 Seq(provider.fieldToAttribute(field, row).asInstanceOf[AttributeValue])
               )
             )
    )

    results should contain theSameElementsAs expectedResults
  }

  it should "handle related entity attributes" in {
    val provider = createTestProvider()
    val relationshipName = "relationship"
    val table = EntityTable("proj", "view", "table", "root")
    val relatedAlias = "related"
    val relatedTable = EntityTable("proj", "view", "relatedTable", relatedAlias)
    val selectAndFroms = Seq(
      SelectAndFrom(table, None, Seq(EntityColumn(table, F_STRING.getName, false))),
      // the actual to and from columns should not matter anymore
      SelectAndFrom(
        relatedTable,
        Some(EntityJoin(null, null, Seq(relationshipName), relatedAlias, false)),
        Seq(
          EntityColumn(relatedTable, F_BOOLEAN.getName, false),
          EntityColumn(relatedTable, F_STRING.getName, false),
          EntityColumn(relatedTable, F_INTEGER.getName, false)
        )
      )
    )

    val parsedExpressions = Set(
      ParsedEntityLookupExpression(List(relationshipName),
                                   F_BOOLEAN.getName,
                                   s"this.$relationshipName.${F_BOOLEAN.getName}"
      ),
      ParsedEntityLookupExpression(List(relationshipName),
                                   F_INTEGER.getName,
                                   s"this.$relationshipName.${F_INTEGER.getName}"
      )
    )

    val tableResult = createTestTableResultWithNestedStruct(3, relatedAlias)

    val results = provider.transformQueryResultToExpressionAndResult(datarepoRowIdColumn,
                                                                     parsedExpressions,
                                                                     selectAndFroms,
                                                                     tableResult
    )

    val expectedResults: Set[ExpressionAndResult] = for {
      expr <- parsedExpressions
      row <- tableResult.iterateAll().asScala
      field <- Seq(F_BOOLEAN, F_INTEGER) if field.getName == expr.columnName
    } yield {
      val attributeValues = row.get(relatedAlias).getRepeatedValue.asScala.map { fv =>
        provider.fieldToAttribute(field, fv.getRecordValue).asInstanceOf[AttributeValue]
      }
      (expr.expression, Map(row.get(datarepoRowIdColumn).getStringValue -> Success(attributeValues)))
    }

    results should contain theSameElementsAs expectedResults
  }

  behavior of "DataEntityProvider.generateExpressionSQL()"

  it should "create basic query" in {
    val table = EntityTable("proj", "view", "table", "root")
    val selectAndFroms =
      Seq(SelectAndFrom(table, None, Seq(EntityColumn(table, "zoe", false), EntityColumn(table, "bob", true))))

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(
      selectAndFroms
    ) shouldBe "SELECT `root`.`zoe`, `root`.`bob` FROM `proj.view.table` `root`;"
  }

  it should "create query with regular joins" in {
    val rootTable = EntityTable("proj", "view", "rootTable", "root")
    val depTable = EntityTable("proj", "view", "debTable", "dep")
    val selectAndFroms = Seq(
      SelectAndFrom(rootTable, None, Seq(EntityColumn(rootTable, "zoe", false), EntityColumn(rootTable, "bob", true))),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(rootTable, "fk", false), EntityColumn(depTable, "fk", false), Seq.empty, "foo", false)
        ),
        Seq(EntityColumn(depTable, "zoe", false), EntityColumn(depTable, "bob", false))
      )
    )

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(selectAndFroms) shouldBe
      """CREATE TEMP FUNCTION dedup_1(val ANY TYPE) AS ((
        |  SELECT ARRAY_AGG(t)
        |  FROM (SELECT DISTINCT * FROM UNNEST(val) v) t
        |));
        |SELECT `root`.`zoe`, `root`.`bob`,
        |dedup_1(ARRAY_AGG(STRUCT(`dep`.`zoe`, `dep`.`bob`))) `foo`
        |FROM `proj.view.rootTable` `root`
        |LEFT JOIN `proj.view.debTable` `dep` ON `root`.`fk` = `dep`.`fk`
        |GROUP BY `root`.`zoe`, `root`.`bob`;""".stripMargin
  }

  it should "create query with unnest array joins" in {
    val rootTable = EntityTable("proj", "view", "rootTable", "root")
    val depTable = EntityTable("proj", "view", "debTable", "dep")
    val selectAndFroms = Seq(
      SelectAndFrom(rootTable, None, Seq(EntityColumn(rootTable, "zoe", false), EntityColumn(rootTable, "bob", true))),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(rootTable, "fk", false), EntityColumn(depTable, "fk", false), Seq.empty, "foo", true)
        ),
        Seq(EntityColumn(depTable, "zoe", false), EntityColumn(depTable, "bob", false))
      )
    )

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(selectAndFroms) shouldBe
      """CREATE TEMP FUNCTION dedup_1(val ANY TYPE) AS ((
        |  SELECT ARRAY_AGG(t)
        |  FROM (SELECT DISTINCT * FROM UNNEST(val) v) t
        |));
        |SELECT `root`.`zoe`, `root`.`bob`,
        |dedup_1(ARRAY_AGG(STRUCT(`dep`.`zoe`, `dep`.`bob`))) `foo`
        |FROM `proj.view.rootTable` `root`
        |LEFT JOIN UNNEST(`root`.`fk`) `unnest_2`
        |LEFT JOIN `proj.view.debTable` `dep` ON `unnest_2` = `dep`.`fk`
        |GROUP BY `root`.`zoe`, `root`.`bob`;""".stripMargin
  }

  it should "create query with regular joins and array columns" in {
    val rootTable = EntityTable("proj", "view", "rootTable", "root")
    val depTable = EntityTable("proj", "view", "debTable", "dep")
    val selectAndFroms = Seq(
      SelectAndFrom(rootTable, None, Seq(EntityColumn(rootTable, "zoe", false), EntityColumn(rootTable, "bob", true))),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(rootTable, "fk", false), EntityColumn(depTable, "fk", false), Seq.empty, "foo", false)
        ),
        Seq(
          EntityColumn(depTable, "zoe", true),
          EntityColumn(depTable, "bob", true),
          EntityColumn(depTable, datarepoRowIdColumn, false),
          EntityColumn(depTable, "another", false)
        )
      )
    )

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(selectAndFroms) shouldBe
      """CREATE TEMP FUNCTION dedup_1(val ANY TYPE) AS ((
        |  SELECT ARRAY_AGG(STRUCT(`datarepo_row_id`, `another`, `zoe`, `bob`)) FROM (
        |    SELECT `datarepo_row_id`, `another`, ARRAY_AGG(DISTINCT `ac_2`) `zoe`, ARRAY_AGG(DISTINCT `ac_3`) `bob`
        |    FROM (SELECT DISTINCT `datarepo_row_id`, `another`, `ac_2`, `ac_3` FROM UNNEST(val) v, UNNEST(v.`zoe`) `ac_2`, UNNEST(v.`bob`) `ac_3`)
        |    GROUP BY `datarepo_row_id`, `another`
        |  )
        |));
        |SELECT `root`.`zoe`, `root`.`bob`,
        |dedup_1(ARRAY_AGG(STRUCT(`dep`.`zoe`, `dep`.`bob`, `dep`.`datarepo_row_id`, `dep`.`another`))) `foo`
        |FROM `proj.view.rootTable` `root`
        |LEFT JOIN `proj.view.debTable` `dep` ON `root`.`fk` = `dep`.`fk`
        |GROUP BY `root`.`zoe`, `root`.`bob`;""".stripMargin
  }

  it should "create query with unnest array joins and array columns" in {
    val rootTable = EntityTable("proj", "view", "rootTable", "root")
    val depTable = EntityTable("proj", "view", "debTable", "dep")
    val selectAndFroms = Seq(
      SelectAndFrom(rootTable, None, Seq(EntityColumn(rootTable, "zoe", false), EntityColumn(rootTable, "bob", true))),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(rootTable, "fk", false), EntityColumn(depTable, "fk", false), Seq.empty, "foo", true)
        ),
        Seq(
          EntityColumn(depTable, "zoe", true),
          EntityColumn(depTable, "bob", true),
          EntityColumn(depTable, datarepoRowIdColumn, false),
          EntityColumn(depTable, "another", false)
        )
      )
    )

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(selectAndFroms) shouldBe
      """CREATE TEMP FUNCTION dedup_1(val ANY TYPE) AS ((
        |  SELECT ARRAY_AGG(STRUCT(`datarepo_row_id`, `another`, `zoe`, `bob`)) FROM (
        |    SELECT `datarepo_row_id`, `another`, ARRAY_AGG(DISTINCT `ac_2`) `zoe`, ARRAY_AGG(DISTINCT `ac_3`) `bob`
        |    FROM (SELECT DISTINCT `datarepo_row_id`, `another`, `ac_2`, `ac_3` FROM UNNEST(val) v, UNNEST(v.`zoe`) `ac_2`, UNNEST(v.`bob`) `ac_3`)
        |    GROUP BY `datarepo_row_id`, `another`
        |  )
        |));
        |SELECT `root`.`zoe`, `root`.`bob`,
        |dedup_1(ARRAY_AGG(STRUCT(`dep`.`zoe`, `dep`.`bob`, `dep`.`datarepo_row_id`, `dep`.`another`))) `foo`
        |FROM `proj.view.rootTable` `root`
        |LEFT JOIN UNNEST(`root`.`fk`) `unnest_4`
        |LEFT JOIN `proj.view.debTable` `dep` ON `unnest_4` = `dep`.`fk`
        |GROUP BY `root`.`zoe`, `root`.`bob`;""".stripMargin
  }

  it should "create query with regular join missing select columns" in {
    val rootTable = EntityTable("proj", "view", "rootTable", "root")
    val depTable = EntityTable("proj", "view", "debTable", "dep")
    val depTable2 = EntityTable("proj", "view", "debTable2", "dep2")
    val selectAndFroms = Seq(
      SelectAndFrom(rootTable, None, Seq(EntityColumn(rootTable, "zoe", false), EntityColumn(rootTable, "bob", true))),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(rootTable, "fk", false), EntityColumn(depTable, "fk", false), Seq.empty, "foo", false)
        ),
        Seq.empty
      ),
      SelectAndFrom(
        depTable,
        Option(
          EntityJoin(EntityColumn(depTable, "fk2", false),
                     EntityColumn(depTable2, "fk2", false),
                     Seq.empty,
                     "bar",
                     false
          )
        ),
        Seq(EntityColumn(depTable, "zoe", true), EntityColumn(depTable, "bob", false))
      )
    )

    val provider = new DataRepoBigQuerySupport {}
    provider.generateExpressionSQL(selectAndFroms) shouldBe
      """CREATE TEMP FUNCTION dedup_1(val ANY TYPE) AS ((
        |  SELECT ARRAY_AGG(STRUCT(`bob`, `zoe`)) FROM (
        |    SELECT `bob`, ARRAY_AGG(DISTINCT `ac_2`) `zoe`
        |    FROM (SELECT DISTINCT `bob`, `ac_2` FROM UNNEST(val) v, UNNEST(v.`zoe`) `ac_2`)
        |    GROUP BY `bob`
        |  )
        |));
        |SELECT `root`.`zoe`, `root`.`bob`,
        |dedup_1(ARRAY_AGG(STRUCT(`dep`.`zoe`, `dep`.`bob`))) `bar`
        |FROM `proj.view.rootTable` `root`
        |LEFT JOIN `proj.view.debTable` `dep` ON `root`.`fk` = `dep`.`fk`
        |LEFT JOIN `proj.view.debTable2` `dep2` ON `dep`.`fk2` = `dep2`.`fk2`
        |GROUP BY `root`.`zoe`, `root`.`bob`;""".stripMargin
  }

  it should "connect to a Data Repo instance via client" in {
    /* This test uses the data repo client lib to connect to a mock data repo server.
        It verifies that the client lib version in use by Rawls is functional
        and has no inherent runtime errors. Anything beyond this simple connectivity test
        should likely be an integration test talking to a real Data Repo instance.
     */
    val jsonHeader = new Header("Content-Type", "application/json")
    val mockSnapshotId = java.util.UUID.randomUUID()
    val mockPort = 32123

    val mockServer = startClientAndServer(mockPort)
    mockServer
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/repository/v1/snapshots/${mockSnapshotId.toString}")
      )
      .respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(s"""{"id":"${mockSnapshotId.toString}"}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    val dataRepoDAO = new HttpDataRepoDAO("mock", s"http://localhost:$mockPort")
    val snapshotResponse = dataRepoDAO.getSnapshot(mockSnapshotId, userInfo.accessToken)
    mockServer.stopAsync()

    snapshotResponse.getId shouldBe mockSnapshotId
  }

}
