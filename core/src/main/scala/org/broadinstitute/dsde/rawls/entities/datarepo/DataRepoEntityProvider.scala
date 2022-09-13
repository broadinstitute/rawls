package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{LegacySQLTypeName, QueryJobConfiguration, QueryParameterValue, TableResult}
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{
  EntityName,
  ExpressionAndResult,
  LookupExpression
}
import org.broadinstitute.dsde.rawls.entities.base._
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoBigQuerySupport._
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{
  AntlrTerraExpressionParser,
  DataRepoEvaluateToAttributeVisitor,
  LookupExpressionExtractionVisitor,
  ParsedEntityLookupExpression
}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeEntityReference,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValue,
  AttributeValueList,
  AttributeValueRawJson,
  Entity,
  EntityQuery,
  EntityQueryResponse,
  EntityTypeMetadata,
  ErrorReport,
  GoogleProjectId,
  RawlsRequestContext,
  SubmissionValidationEntityInputs
}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json.{JsArray, JsBoolean, JsFalse, JsNull, JsNumber, JsObject, JsString, JsTrue, JsValue}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(snapshotModel: SnapshotModel,
                             requestArguments: EntityRequestArguments,
                             samDAO: SamDAO,
                             bqServiceFactory: GoogleBigQueryServiceFactory,
                             config: DataRepoEntityProviderConfig
)(implicit protected val executionContext: ExecutionContext)
    extends EntityProvider
    with DataRepoBigQuerySupport
    with LazyLogging
    with ExpressionEvaluationSupport {

  override val entityStoreId: Option[String] = Option(snapshotModel.getId.toString)

  private[datarepo] lazy val googleProject: GoogleProjectId =
    /* Determine project to be billed for the BQ job:
        If a project was explicitly specified in the constructor arguments, use that.
        Else, use the workspace's project. This requires canCompute permissions on the workspace.
     */
    requestArguments.billingProject match {
      case Some(billing) => billing
      case None          => requestArguments.workspace.googleProjectId
    }

  override def entityTypeMetadata(useCache: Boolean = false): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: AS-321 auto-switch to see if the ref supplied in argument is a UUID or a name?? Use separate query params? Never allow ID?

    // reformat TDR's response into the expected response structure
    val entityTypesResponse: Map[String, EntityTypeMetadata] = snapshotModel.getTables.asScala.map { table =>
      val primaryKey = pkFromSnapshotTable(table)
      var attrs: Seq[String] = table.getColumns.asScala.map(_.getName).toList
      (table.getName, EntityTypeMetadata(table.getRowCount, primaryKey, attrs))
    }.toMap

    Future.successful(entityTypesResponse)

  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")

  override def deleteEntitiesOfType(entityType: String): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities of type not supported by this provider.")

  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    // extract table definition, with PK, from snapshot schema
    val tableModel = getTableModel(snapshotModel, entityType)

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)
    // determine data project
    val dataProject = snapshotModel.getDataProject
    // determine view name
    val viewName = snapshotModel.getName
    // generate BQ SQL for this entity
    // they should be safe, but we should have layers of protection.
    val query =
      s"SELECT * FROM `${validateSql(dataProject)}.${validateSql(viewName)}.${validateSql(entityType)}` WHERE $pk = @pkvalue;"
    // generate query config, with named param for primary key
    val queryConfigBuilder = QueryJobConfiguration
      .newBuilder(query)
      .addNamedParameter("pkvalue", QueryParameterValue.string(entityName))

    val resultIO = for {
      // get pet service account key for this user
      petKey <- getPetSAKey
      // execute the query against BQ
      queryResults <- runBigQuery(queryConfigBuilder, petKey, GoogleProject(googleProject.value))
    } yield
    // translate the BQ results into a single Rawls Entity
    queryResultsToEntity(queryResults, entityType, pk)
    resultIO.unsafeToFuture()
  }

  override def queryEntities(entityType: String,
                             incomingQuery: EntityQuery,
                             parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[EntityQueryResponse] = {
    // throw immediate error if user supplied filterTerms
    if (incomingQuery.filterTerms.nonEmpty) {
      throw new UnsupportedEntityOperationException("term filtering not supported by this provider.")
    }

    // extract table definition, with PK, from snapshot schema
    val tableModel = getTableModel(snapshotModel, entityType)

    // validate sort column exists in the snapshot's table description, or sort column is
    // one of the magic fields "datarepo_row_id" or "name"
    if (
      datarepoRowIdColumn != incomingQuery.sortField &&
      "name" != incomingQuery.sortField &&
      !tableModel.getColumns.asScala.exists(_.getName == incomingQuery.sortField)
    )
      throw new DataEntityException(code = StatusCodes.BadRequest,
                                    message = s"sortField not valid for this entity type"
      )

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)

    // allow sorting by magic "name" field, which is a derived field containing the pk
    val finalQuery =
      if (incomingQuery.sortField == "name" && !tableModel.getColumns.asScala.exists(_.getName == "name")) {
        incomingQuery.copy(sortField = pk)
      } else {
        incomingQuery
      }

    // calculate the pagination metadata
    val metadata = queryResultsMetadata(tableModel.getRowCount, finalQuery)

    // validate requested page against actual number of pages
    if (finalQuery.page > metadata.filteredPageCount) {
      throw new DataEntityException(
        code = StatusCodes.BadRequest,
        message =
          s"requested page ${incomingQuery.page} is greater than the number of pages ${metadata.filteredPageCount}"
      )
    }

    // if Data Repo indicates this table is empty, create an empty list and don't query BigQuery
    val futurePage: Future[List[Entity]] = if (tableModel.getRowCount == 0) {
      Future.successful(List.empty[Entity])
    } else {
      // determine data project
      val dataProject = snapshotModel.getDataProject
      // determine view name
      val viewName = snapshotModel.getName

      val queryConfigBuilder = queryConfigForQueryEntities(dataProject, viewName, entityType, finalQuery)

      val resultIO = for {
        // get pet service account key for this user
        petKey <- getPetSAKey
        // execute the query against BQ
        queryResults <- runBigQuery(queryConfigBuilder, petKey, GoogleProject(googleProject.value))
      } yield
      // translate the BQ results into a Rawls query result
      queryResultsToEntities(queryResults, entityType, pk)
      resultIO.unsafeToFuture()
    }

    futurePage map { page => EntityQueryResponse(finalQuery, metadata, page) }

  }

  def pkFromSnapshotTable(tableModel: TableModel): String =
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _                          => datarepoRowIdColumn // default data repo value
    }

  private[datarepo] def convertToListAndCheckSize(expressionResultsStream: Stream[ExpressionAndResult],
                                                  expectedSize: Int
  ): Seq[ExpressionAndResult] = {
    // this size of stuff is not meant to be precise but just hopefully close enough
    // so that we can protect from OOMs
    val objectSize = 8
    val bigDecimalSize = objectSize + 40
    val booleanSize = objectSize + 1
    def stringSize(s: String) = objectSize + s.length * 1

    def sizeOfJsValueBytes(jsValue: JsValue): Int =
      jsValue match {
        case JsArray(elements) => objectSize + elements.map(sizeOfJsValueBytes).sum
        case JsBoolean(_)      => booleanSize
        case JsNull            => 0
        case JsNumber(_)       => bigDecimalSize
        case JsObject(fields)  => fields.map { case (k, v) => stringSize(k) + sizeOfJsValueBytes(v) }.sum
        case JsString(value)   => objectSize + stringSize(value)
        case JsFalse           => 1
        case JsTrue            => 1
      }

    def sizeOfAttributeValueBytes(attributeValue: AttributeValue): Int =
      attributeValue match {
        case AttributeNull                => 0
        case AttributeBoolean(_)          => booleanSize
        case AttributeNumber(_)           => bigDecimalSize
        case AttributeString(value)       => objectSize + stringSize(value)
        case AttributeValueRawJson(value) => objectSize + sizeOfJsValueBytes(value)
      }

    val buffer = new ArrayBuffer[ExpressionAndResult](expectedSize)
    var runningSizeEstimateBytes = 0
    expressionResultsStream.foreach { expressionAndResult =>
      val result = expressionAndResult._2
      // the getOrElse(0) below ignores any failures in the Try
      val sizeEstimateBytes = result.values.map(_.map(_.map(sizeOfAttributeValueBytes).sum).getOrElse(0)).sum
      runningSizeEstimateBytes += sizeEstimateBytes

      if (runningSizeEstimateBytes > config.maxBigQueryResponseSizeBytes) {
        throw new DataEntityException(
          s"Query returned too many results likely due to either large one-to-many relationships or arrays. The limit on the total number bytes is ${config.maxBigQueryResponseSizeBytes}."
        )
      }

      buffer += expressionAndResult
    }

    buffer.toList
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[Stream[SubmissionValidationEntityInputs]] =
    expressionEvaluationContext match {
      case ExpressionEvaluationContext(None, None, None, Some(rootEntityType)) =>
        /*
        overall approach here is to extract all the entity lookup expressions from the input expressions,
        generate 1 BigQuery query from the lookup expressions, execute it, use the BQ results to
        construct a value for each input expression for each entity

        Some things to consider:
        input expressions may have 0 or more entity lookup expressions
        the same entity lookup expression may appear more than once within an input expression or across all input expressions
        after running the BigQuery job the work is to figure out where those results get plugged into input values for each entity
         */
        val resultIO = for {
          parsedExpressions <- parseAllExpressions(gatherInputsResult)
          tableModel = getTableModel(snapshotModel, rootEntityType)
          _ <- checkSubmissionSize(parsedExpressions, tableModel)
          entityNameColumn = pkFromSnapshotTable(tableModel)
          (selectAndFroms, bqQueryJobConfig) = queryConfigForExpressions(snapshotModel,
                                                                         parsedExpressions,
                                                                         tableModel,
                                                                         entityNameColumn
          )
          _ = logger.debug(
            s"expressions [${parsedExpressions.map(_.expression).mkString(", ")}] for snapshot id [${snapshotModel.getId} produced sql query ${bqQueryJobConfig.build().getQuery}"
          )
          petKey <- getPetSAKey
          queryResults <- runBigQuery(bqQueryJobConfig, petKey, GoogleProject(googleProject.value))
        } yield {
          val expressionResultsStream =
            transformQueryResultToExpressionAndResult(entityNameColumn, parsedExpressions, selectAndFroms, queryResults)
          val expressionResults = convertToListAndCheckSize(expressionResultsStream, tableModel.getRowCount)
          val rootEntityNames = getEntityNames(expressionResults)
          val workspaceExpressionResultsPerEntity =
            populateWorkspaceLookupPerEntity(workspaceExpressionResults, rootEntityNames)
          val groupedResults = groupResultsByExpressionAndEntityName(
            expressionResults ++ workspaceExpressionResultsPerEntity
          )

          val entityNameAndInputValues =
            constructInputsForEachEntity(gatherInputsResult, groupedResults, rootEntityNames)

          createSubmissionValidationEntityInputs(CollectionUtils.groupByTuples(entityNameAndInputValues))
        }
        resultIO.unsafeToFuture()

      case _ =>
        Future.failed(
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, "Only root entity type supported for Data Repo workflows")
          )
        )
    }

  private def getPetSAKey: IO[String] = {
    logger.debug(s"getPetSAKey attempting against project ${googleProject.value}")
    IO.fromFuture(
      IO(
        samDAO
          .getPetServiceAccountKeyForUser(googleProject, requestArguments.ctx.userInfo.userEmail)
          .recover {
            case report: RawlsExceptionWithErrorReport =>
              val errMessage = s"Error attempting to use project ${googleProject.value}. " +
                s"The project does not exist or you do not have permission to use it: ${report.errorReport.message}"
              throw new RawlsExceptionWithErrorReport(report.errorReport.copy(message = errMessage))
            case err: Exception =>
              throw new RawlsException(
                s"Error attempting to use project ${googleProject.value}. " +
                  s"The project does not exist or you do not have permission to use it: ${err.getMessage}"
              )
          }
      )
    )
  }

  private def getEntityNames(bqExpressionResults: Seq[ExpressionAndResult]): Seq[EntityName] =
    bqExpressionResults.flatMap { case (_, resultsMap) =>
      resultsMap.keys
    }.distinct

  private def populateWorkspaceLookupPerEntity(
    workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]],
    rootEntities: Seq[EntityName]
  ): Seq[ExpressionAndResult] =
    workspaceExpressionResults.toSeq.map { case (lookup, result) =>
      (lookup, rootEntities.map(_ -> result).toMap)
    }

  private def checkSubmissionSize(parsedExpressions: Set[ParsedEntityLookupExpression], tableModel: TableModel) =
    if (tableModel.getRowCount * parsedExpressions.size > config.maxInputsPerSubmission) {
      IO.raiseError(
        new RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.BadRequest,
            s"Too many results. Snapshot row count * number of entity expressions cannot exceed ${config.maxInputsPerSubmission}."
          )
        )
      )
    } else {
      IO.unit
    }

  private def constructInputsForEachEntity(
    gatherInputsResult: GatherInputsResult,
    groupedResults: Seq[(LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])],
    rootEntities: Seq[EntityName]
  ) =
    // gatherInputsResult.processableInputs.toSeq so that the result is not a Set and does not worry about duplicates
    gatherInputsResult.processableInputs.toSeq.flatMap { input =>
      val parser = AntlrTerraExpressionParser.getParser(input.expression)
      val visitor = new LookupExpressionExtractionVisitor()
      val parsedTree = parser.root()
      val lookupExpressions = visitor.visit(parsedTree)

      val expressionResultsByEntityName =
        InputExpressionReassembler.constructFinalInputValues(groupedResults.filter { case (expression, _) =>
                                                               lookupExpressions.contains(expression)
                                                             },
                                                             parsedTree,
                                                             Option(rootEntities),
                                                             Option(input)
        )

      convertToSubmissionValidationValues(expressionResultsByEntityName, input)
    }

  private def groupResultsByExpressionAndEntityName(expressionResults: Seq[ExpressionAndResult]) =
    expressionResults
      .groupBy { case (expression, _) =>
        expression
      }
      .toSeq
      .map { case (expression, groupedList) =>
        (expression,
         groupedList.foldLeft(Map.empty[EntityName, Try[Iterable[AttributeValue]]]) {
           case (aggregateResults, (_, individualResult)) => aggregateResults ++ individualResult
         }
        )
      }

  private def runBigQuery(bqQueryJobConfigBuilder: QueryJobConfiguration.Builder,
                          petKey: String,
                          projectToBill: GoogleProject
  ): IO[TableResult] = {
    logger.debug(s"runBigQuery attempting against project  ${projectToBill.value}")
    try
      bqServiceFactory
        .getServiceForPet(petKey, projectToBill)
        .use(
          _.query(
            bqQueryJobConfigBuilder
              .setMaximumBytesBilled(config.bigQueryMaximumBytesBilled)
              .build()
          )
        )
    catch {
      case ex: GoogleJsonResponseException if ex.getStatusCode == StatusCodes.Forbidden.intValue =>
        throw new RawlsException(
          s"Billing project {googleProject.value} either does not exist or the user does not have access to it."
        )
    }
  }

  /**
    * The tableResult should have a single row per root entity. Each row should have a column for each
    * column request from the root entity. Each row should also have a column for each related entity
    * from which columns were requested. Each of these "related entity" columns is an array of structs.
    * The columns of each struct are the columns requested from that entity.
    *
    * @param entityNameColumn name of column representing the entity name
    * @param parsedExpressions incoming expressions
    * @param selectAndFroms query structure
    * @param tableResult query results
    * @return Stream because it should not suck all the results from BigQuery into memory
    */
  private[datarepo] def transformQueryResultToExpressionAndResult(entityNameColumn: String,
                                                                  parsedExpressions: Set[ParsedEntityLookupExpression],
                                                                  selectAndFroms: Seq[SelectAndFrom],
                                                                  tableResult: TableResult
  ): Stream[ExpressionAndResult] = {
    val selectAndFromByRelationshipPath =
      selectAndFroms.map(sf => sf.join.map(_.relationshipPath).getOrElse(Seq.empty) -> sf).toMap
    val joinAliasesByRelationshipPath =
      selectAndFroms.flatMap(j => j.join.map(r => r.relationshipPath -> r.alias)).toMap
    for {
      resultRow <- tableResult.iterateAll().asScala.toStream // this is the streaming goodness
      parsedExpression <- parsedExpressions
    } yield {
      // Each parsedExpression has a relationshipPath and columnName. relationshipPath should match a relationshipPath
      // of one of the selectAndFroms and columnName should exist in selectColumns from that selectAndFrom.
      // If parsedExpression.relationshipPath is empty then columnName is on the root entity.
      // Using this information we can lookup the value for each parsedExpression in tableResult.

      // determine column index based on position of parsedExpression.columnName in SelectAndFrom.selectColumns
      val columnIndex = selectAndFromByRelationshipPath(parsedExpression.relationshipPath).selectColumns
        .map(_.column)
        .indexOf(parsedExpression.columnName)
      val attribute = joinAliasesByRelationshipPath.get(parsedExpression.relationshipPath) match {
        case None =>
          // this is a root level expression, e.g. this.foo, no alias required it should be at the top level of the row
          val field = tableResult.getSchema.getFields.get(columnIndex)
          val fieldValue = resultRow.get(columnIndex)
          fieldValueToAttribute(field, fieldValue)
        case Some(joinAlias) =>
          // this is a relation expressions, e.g. this.foo.bar, it should be an array of structs (i.e. repeated record) column
          // the name of the column is the relationshipAlias and the sub fields the names of the columns in the expressions
          val field = tableResult.getSchema.getFields.get(joinAlias)
          assert(field.getMode == Mode.REPEATED, "expected result from relationship to be an array")
          assert(field.getType == LegacySQLTypeName.RECORD, "expected result from relationship to be a struct")
          val subField = field.getSubFields.get(columnIndex)
          val attributeValues = resultRow
            .get(joinAlias)
            .getRepeatedValue
            .asScala
            .map { struct =>
              val subFieldValue = struct.getRecordValue.get(columnIndex)
              fieldValueToAttribute(subField, subFieldValue)
            }
            .flatMap {
              case v: AttributeValue     => Seq(v)
              case AttributeValueList(l) => l
              case unsupported           => throw new RawlsException(s"unsupported attribute: $unsupported")
            }
          AttributeValueList(attributeValues.toList)
      }

      val primaryKey: EntityName = resultRow.get(entityNameColumn).getStringValue
      val evaluationResult: Try[Iterable[AttributeValue]] = attribute match {
        case v: AttributeValue     => Success(Seq(v))
        case AttributeValueList(l) => Success(l)
        case unsupported           => Failure(new RawlsException(s"unsupported attribute: $unsupported"))
      }
      (parsedExpression.expression, Map(primaryKey -> evaluationResult))
    }
  }

  /**
    * Iterate through all the input expressions and extract the entity lookup expressions
    *
    * @param gatherInputsResult input expression source
    * @return
    */
  private def parseAllExpressions(gatherInputsResult: GatherInputsResult) = IO {
    gatherInputsResult.processableInputs.flatMap { input =>
      val parser = AntlrTerraExpressionParser.getParser(input.expression)
      val visitor = new DataRepoEvaluateToAttributeVisitor()
      visitor.visit(parser.root())
    }
  }

  override def expressionValidator: ExpressionValidator = new DataRepoEntityExpressionValidator(snapshotModel)

  override def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    throw new UnsupportedEntityOperationException("batch-update entities not supported by this provider.")

  override def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    throw new UnsupportedEntityOperationException("batch-upsert entities not supported by this provider.")
}
