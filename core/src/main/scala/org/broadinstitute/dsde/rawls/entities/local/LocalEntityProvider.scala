package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import cromwell.client.model.ToolInputParameter
import cromwell.client.model.ValueType.TypeNameEnum
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeNull, AttributeValue, AttributeValueEmptyList, AttributeValueList, AttributeValueRawJson, Entity, EntityTypeMetadata, ErrorReport, SubmissionValidationEntityInputs, SubmissionValidationValue, Workspace}
import org.broadinstitute.dsde.rawls.util.{CollectionUtils, EntitySupport}
import spray.json.JsArray
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.DeleteEntitiesConflictException
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata, ErrorReport, Workspace}
import org.broadinstitute.dsde.rawls.util.EntitySupport

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(workspace: Workspace, implicit protected val dataSource: SlickDataSource)
                         (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging with EntitySupport {

  import dataSource.dataAccess.driver.api._

  private val workspaceContext = workspace

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.getEntityTypeMetadata(workspaceContext)
    }
  }

  override def createEntity(entity: Entity): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspace.toWorkspaceName}")))
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }
  }

  // EntityApiServiceSpec has good test coverage for this api
  override def deleteEntities(entRefs: Seq[AttributeEntityReference]): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      // withAllEntities throws exception if some entities not found; passes through if all ok
      withAllEntities(workspaceContext, dataAccess, entRefs) { entities =>
        dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap { referringEntities =>
          if (referringEntities != entRefs.toSet)
            throw new DeleteEntitiesConflictException(referringEntities)
          else {
            dataAccess.entityQuery.hide(workspaceContext, entRefs)
          }
        }
      }
    }
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult): Future[Stream[SubmissionValidationEntityInputs]] = {
    dataSource.inTransaction { dataAccess =>
      withEntityRecsForExpressionEval(expressionEvaluationContext, workspace, dataAccess) { jobEntityRecs =>
        //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
        evaluateExpressionsInternal(workspace, gatherInputsResult.processableInputs, jobEntityRecs, dataAccess) map { valuesByEntity =>
          valuesByEntity
            .map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values.toSet) }).toStream
        }
      }
    }
  }

  override def expressionValidator: ExpressionValidator = new LocalEntityExpressionValidator


  protected[local] def evaluateExpressionsInternal(workspaceContext: Workspace, inputs: Set[MethodInput], entities: Option[Seq[EntityRecord]], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    val entityNames = entities match {
      case Some(recs) => recs.map(_.name)
      case None => Seq("")
    }

    if( inputs.isEmpty ) {
      //no inputs to evaluate = just return an empty map back!
      DBIO.successful(entityNames.map( _ -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(String, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                entityNames.map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
              case Success(attributeMap) =>
                //The expression was evaluated, but that doesn't mean we got results...
                attributeMap.map {
                  case (key, Success(attrSeq)) => key -> unpackResult(attrSeq.toSeq, input.workflowInput)
                  case (key, Failure(regret)) => key -> SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)
                }.toSeq
            }
            validationValuesByEntity
          }
        }

        //Flip the list of DBIO monads into one on the outside that we can map across and then group by entity.
        DBIO.sequence(resultsByInput) map { results =>
          CollectionUtils.groupByTuples(results.flatten)
        }
      }
    }
  }

  private def unpackResult(mcSequence: Iterable[AttributeValue], wfInput: ToolInputParameter): SubmissionValidationValue = wfInput.getValueType.getTypeName match {
    case TypeNameEnum.ARRAY => getArrayResult(wfInput.getName, mcSequence)
    case TypeNameEnum.OPTIONAL  => if (wfInput.getValueType.getOptionalType.getTypeName == TypeNameEnum.ARRAY)
      getArrayResult(wfInput.getName, mcSequence)
    else getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional) //send optional-arrays down the same codepath as arrays
    case _ => getSingleResult(wfInput.getName, mcSequence, wfInput.getOptional)
  }

  private val emptyResultError = "Expected single value for workflow input, but evaluated result set was empty"
  private val multipleResultError  = "Expected single value for workflow input, but evaluated result set had multiple values"

  private def getSingleResult(inputName: String, seq: Iterable[AttributeValue], optional: Boolean): SubmissionValidationValue = {
    def handleEmpty = if (optional) None else Some(emptyResultError)
    seq match {
      case Seq() => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(null) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(AttributeNull) => SubmissionValidationValue(None, handleEmpty, inputName)
      case Seq(singleValue) => SubmissionValidationValue(Some(singleValue), None, inputName)
      case multipleValues => SubmissionValidationValue(Some(AttributeValueList(multipleValues.toSeq)), Some(multipleResultError), inputName)
    }
  }

  private def getArrayResult(inputName: String, seq: Iterable[AttributeValue]): SubmissionValidationValue = {
    val notNull: Seq[AttributeValue] = seq.filter(v => v != null && v != AttributeNull).toSeq
    val attr = notNull match {
      case Nil => Option(AttributeValueEmptyList)
      //GAWB-2509: don't pack single-elem RawJson array results into another layer of array
      //NOTE: This works, except for the following situation: a participant with a RawJson double-array attribute, in a single-element participant set.
      // Evaluating this.participants.raw_json on the pset will incorrectly hit this case and return a 2D array when it should return a 3D array.
      // The true fix for this is to look into why the slick expression evaluator wraps deserialized AttributeValues in a Seq, and instead
      // return the proper result type, removing the need to infer whether it's a scalar or array type from the WDL input.
      case AttributeValueRawJson(JsArray(_)) +: Seq() => Option(notNull.head)
      case _ => Option(AttributeValueList(notNull))
    }
    SubmissionValidationValue(attr, None, inputName)
  }

  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      withEntity(workspaceContext, entityType, entityName, dataAccess) {
        entity => DBIO.successful(entity)
      }
    }
  }

}
