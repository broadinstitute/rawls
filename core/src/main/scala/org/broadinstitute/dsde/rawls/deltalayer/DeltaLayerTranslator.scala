package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeFormat, AttributeName, AttributeNumber, AttributeString, JsonSupport, PlainArrayAttributeListSerializer}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.deltalayer.v1.DeltaRow
import spray.json.{JsValue, enrichAny}

import java.util.UUID
import scala.util.{Failure, Success, Try}

/**
 * Utilities to translate Rawls' batchUpsert JSON/model classes into Delta Layer's JSON/model classes
 */
object DeltaLayerTranslator extends JsonSupport with LazyLogging {

  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  val ERR_INVALID_OPERATION = "This method only accepts AddUpdateAttribute operations."
  val ERR_EMPTY_OPERATIONS = "This method requires at least one AddUpdateAttribute operation."
  val ERR_INVALID_DATATYPE = "This method only accepts string, boolean, and numeric scalar values."
  val ERR_EMPTY_DATATYPE = "This method requires at least one string, boolean, or numeric scalar value to update."

  /**
   * Inspects the incoming EntityUpdateDefinitions and throws exceptions if any of those updates
   * are illegal.
   * @param entityUpdates the updates to validate
   * @return the original set of updates, if all are valid
   */
  def validateEntityUpdates(entityUpdates: Seq[EntityUpdateDefinition]): Seq[EntityUpdateDefinition] = {
    val allOps: Seq[AttributeUpdateOperation] = entityUpdates flatMap { upd => upd.operations }

    if (allOps.isEmpty)
      throw new DeltaLayerException(ERR_EMPTY_OPERATIONS, code = BadRequest)

    val (upserts, others) = allOps.partition {
      case _:AddUpdateAttribute => true
      case _ => false
    }

    if (others.nonEmpty)
      // in the future, may want to provide more info such as entity type/name and illegal operation
      throw new DeltaLayerException(ERR_INVALID_OPERATION, code = BadRequest)

    if (upserts.isEmpty)
      // how did we reach here? This should be impossible
      throw new DeltaLayerException(ERR_EMPTY_OPERATIONS, code = BadRequest)

    // validate value types
    val allValues = allOps.collect {
      case a:AddUpdateAttribute => a.addUpdateAttribute
    }

    if (allValues.size != allOps.size)
      throw new DeltaLayerException("Unexpected error: count of update values did not match count of update operations",
        code = InternalServerError)

    // TODO: should support more types, including lists!
    val (supportedTypes, unsupportedTypes) = allValues.partition {
      case AttributeBoolean(_) => true
      case AttributeNumber(_) => true
      case AttributeString(_) => true
      case _ => false // AttributeNull, AttributeValueRawJson, AttributeEntityReference, AttributeList
    }

    if (unsupportedTypes.nonEmpty)
      throw new DeltaLayerException(ERR_INVALID_DATATYPE, code = BadRequest)

    if (supportedTypes.isEmpty)
      // how did we reach here? This should be impossible
      throw new DeltaLayerException(ERR_INVALID_DATATYPE, code = BadRequest)

    entityUpdates
  }

  def translateEntityUpdates(entityUpdates: Seq[EntityUpdateDefinition]): Seq[DeltaRow] = {
    entityUpdates.flatMap { update =>
      // we expect all calls to use datarepo_row_id as entity name
      val datarepoRowId = Try(UUID.fromString(update.name)) match {
        case Success(id) => id
        case Failure(ex) => throw new DeltaLayerException(s"Invalid datarepo_row_id specified in update request: ${ex.getMessage}",
          code = BadRequest)
      }
      update.operations map { op =>
        // strip "default." from attr name
        val name = op.name.namespace match {
          case AttributeName.defaultNamespace => op.name.name
          case _ => toDelimitedName(op.name)
        }
        val value = op match {
          case aua:AddUpdateAttribute => attributeFormat.writeAttribute(aua.addUpdateAttribute)
          case _ => throw new DeltaLayerException(ERR_INVALID_OPERATION, code = BadRequest)
        }
        DeltaRow(datarepoRowId, name, value)
      }
    }
  }



}
