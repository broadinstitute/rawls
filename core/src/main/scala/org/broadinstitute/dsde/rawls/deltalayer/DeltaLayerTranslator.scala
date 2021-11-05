package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.deltalayer.v1.DeltaRow
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeFormat, AttributeNull, AttributeNumber, AttributeString, AttributeValueEmptyList, AttributeValueList, JsonSupport, PlainArrayAttributeListSerializer}

import java.util.UUID
import scala.util.{Failure, Success, Try}

/**
 * Utilities to translate Rawls' batchUpsert JSON/model classes into Delta Layer's JSON/model classes
 */
object DeltaLayerTranslator extends JsonSupport with LazyLogging {

  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  val ERR_INVALID_ENTITYNAME = "This method only accepts entity names that are valid UUIDs, e.g. datarepo_row_id"
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
    // verify that all entity names are legal UUIDs - they should be datarepo_row_ids
    val uuidAttempts = entityUpdates.map(upd => Try(UUID.fromString(upd.name)))
    if (uuidAttempts.exists(_.isFailure))
      throw new DeltaLayerException(ERR_INVALID_ENTITYNAME, code = BadRequest)

    // grab all the operations out of the EntityUpdateDefinitions
    val allOps: Seq[AttributeUpdateOperation] = entityUpdates flatMap { upd => upd.operations }
    if (allOps.isEmpty)
      throw new DeltaLayerException(ERR_EMPTY_OPERATIONS, code = BadRequest)

    // transform the operations: if the op is an AddUpdateAttribute, extract its Attribute;
    // if it is any other operation, return None
    val maybeAttributes: Seq[Option[Attribute]] = allOps.map {
      case a:AddUpdateAttribute => Option(a.addUpdateAttribute)
      case _ => None
    }

    // throw if we found anything other than AddUpdateAttribute in the operations
    if (maybeAttributes.exists(_.isEmpty))
      // in the future, may want to provide more info such as entity type/name and illegal operation
      throw new DeltaLayerException(ERR_INVALID_OPERATION, code = BadRequest)

    // flatten to remove the Options
    val allValues: Seq[Attribute] = maybeAttributes.flatten

    // validate nonEmpty/expected size of allValues
    if (allValues.isEmpty)
      // how did we reach here? This should be impossible
      throw new DeltaLayerException(ERR_EMPTY_OPERATIONS, code = BadRequest)
    if (allValues.size != allOps.size)
      throw new DeltaLayerException("Unexpected error: count of update values did not match count of update operations",
        code = InternalServerError)

    // validate that all upsert values are string/boolean/number only
    val (supportedTypes, unsupportedTypes) = allValues partition isSupportedDataType

    if (unsupportedTypes.nonEmpty)
      throw new DeltaLayerException(ERR_INVALID_DATATYPE, code = BadRequest)

    if (supportedTypes.isEmpty)
      // how did we reach here? This should be impossible
      throw new DeltaLayerException(ERR_INVALID_DATATYPE, code = BadRequest)

    // everything validated; return the original update definitions
    entityUpdates
  }

  /**
   * is the supplied Attribute supported by Delta Layer?
   * @param attr the Attribute to inspect
   * @return whether or not Delta Layer supports this Attribute's type
   */
  private def isSupportedDataType(attr: Attribute): Boolean = attr match {
    case AttributeBoolean(_) | AttributeNumber(_) | AttributeString(_) | AttributeNull | AttributeValueEmptyList => true
    case AttributeValueList(elems) => elems forall isSupportedDataType
    case _ =>
      // AttributeNull, AttributeValueRawJson, AttributeEntityReference,
      // AttributeEntityReferenceList, AttributeEntityReferenceEmptyList
      false
  }


  /**
   * Transforms the Rawls user-visible model classes into the models we will write
   * into Delta Layer insert files. Calls validateEntityUpdates before performing
   * transformations
   *
   * @param entityUpdates the updates to translate
   * @return the collection of DeltaRow objects to write to a Delta Layer insert file
   */
  def translateEntityUpdates(entityUpdates: Seq[EntityUpdateDefinition]): Seq[DeltaRow] = {

    val validUpdates = validateEntityUpdates(entityUpdates)

    validUpdates.flatMap { update =>
      // we expect all calls to use datarepo_row_id as entity name
      val datarepoRowId = Try(UUID.fromString(update.name)) match {
        case Success(id) => id
        case Failure(_) => throw new DeltaLayerException(ERR_INVALID_ENTITYNAME, code = BadRequest)
      }
      update.operations map { op =>
        // strip "default:" from attr name
        val name = toDelimitedName(op.name)
        val value = op match {
          case aua:AddUpdateAttribute => attributeFormat.writeAttribute(aua.addUpdateAttribute)
          case _ =>
            // this error case is covered by validateEntityUpdates, but we need it here for exhaustive matching
            throw new DeltaLayerException(ERR_INVALID_OPERATION, code = BadRequest)
        }
        DeltaRow(datarepoRowId, name, value)
      }
    }
  }


}
