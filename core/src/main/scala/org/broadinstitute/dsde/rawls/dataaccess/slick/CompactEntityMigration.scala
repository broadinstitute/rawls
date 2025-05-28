package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.{AttributeFormat, AttributeName}
import slick.jdbc.MySQLProfile.api._

import java.util.UUID
import scala.annotation.unused

/**
  * SQL queries for migrating legacy entities to compact entities.
  */
trait CompactEntityMigration {
  this: CompactEntityQuery =>

  def findMaxChunkId(chunkSize: Int, startingId: Long, workspaceId: UUID): ReadAction[Option[Long]] =
    sql"""select max(id)
          from ENTITY
          where id > $startingId
          and workspace_id = $workspaceId
          and deleted = 0
          order by id
          limit #$chunkSize
          """.as[Long].headOption

  /** temp table used during migration from legacy to compact entities */
  def migrationCreateAttributeTempTable: ReadWriteAction[Int] =
    sql"""create temporary table QS_ATTR_TEMP(
            entity_id bigint unsigned NOT NULL,
            attr_name varchar(240) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
            list_index int,
            attr_value json,
            KEY KEY_LIST_INDEX (list_index),
            KEY KEY_ATTR_INDEX (entity_id, attr_name)
          );""".asUpdate

  /** temp table used during migration from legacy to compact entities */
  def migrationCreateEntityTempTable: ReadWriteAction[Int] =
    sql"""create temporary table QS_ENTITY_TEMP(
            entity_id bigint unsigned NOT NULL,
            attributes json,
            KEY KEY_ENT_ID (entity_id)
          );""".asUpdate

  /**
    * First step of data massaging to build compact entities:
    *   - join namespace and name into a single delimited attribute key
    *   - cast individual attribute values to JSON
    * Write all this to a temp table QS_ATTR_TEMP.
    *
    * The order-by clause here is very sensitive. See the comment about JSON_ARRAYAGG behavior on the
    * migrationPopulateEntityTempTable() function; the order-by must insert rows into the temp table in the proper
    * order to respect the list_index value from the legacy attributes.
    *
    * Note also the case for list_length==0. This is a special handling for empty lists; legacy attributes
    * represent empty lists as a row with list_length 0, list_index null, and value_number -1. Without the special-case handling,
    * these would be returned as AttributeNumber(-1). See AttributeComponent.marshalEmptyVal for details.
    */
  def migrationPopulateAttributeTempTable(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""insert into QS_ATTR_TEMP(entity_id, attr_name, list_index, attr_value)
          select
            e.id,
            CASE
              WHEN ea.namespace = ${AttributeName.defaultNamespace} then ea.name
              ELSE CONCAT(ea.namespace, ${AttributeName.delimiter.toString}, ea.name)
            END as attr_name,
            ea.list_index,
            CASE
                WHEN value_string is not null THEN CAST(JSON_QUOTE(value_string) as JSON)
                WHEN value_boolean is not null THEN CAST(value_boolean as JSON)
                WHEN list_length = 0 THEN JSON_ARRAY()
                WHEN value_number is not null THEN CAST(value_number as JSON)
                WHEN VALUE_JSON is not null THEN VALUE_JSON
                WHEN value_entity_ref is not null THEN JSON_OBJECT(${AttributeFormat.ENTITY_TYPE_KEY}, ref.entity_type, ${AttributeFormat.ENTITY_NAME_KEY}, ref.name)
                ELSE null
            END as attr_value
          from ENTITY e
              join ENTITY_ATTRIBUTE_#$shardId ea on e.id = ea.owner_id
              left outer join ENTITY ref on ea.value_entity_ref = ref.id
          where e.workspace_id = $workspaceId
          and e.deleted = 0
          and ea.deleted = 0
          order by ea.list_index, e.id, attr_name""".asUpdate

  /**
    * Second step of data massaging to build compact entities:
    *   - aggregate all list elements into a JSON array
    *   - aggregate all scalars and arrays into a single JSON object per entity; this is the compact entity representation
    * Write the result to a temp table QS_ENTITY_TEMP.
    *
    * Note: the array aggregation _must_ read from a temp table - as opposed to a CTE - because the JSON_ARRAYAGG function
    *   only supports ordering its elements by a table's native row order. Therefore, to respect the list_index value
    *   from the legacy attributes, we must first write the data to a temp table in proper row order, then aggregate.
    */
  def migrationPopulateEntityTempTable: ReadWriteAction[Int] =
    sql"""insert into QS_ENTITY_TEMP (entity_id, attributes)
            with CTE as (
            select entity_id, attr_name,
                case
                    when max(list_index) is null then max(attr_value)
                    else JSON_ARRAYAGG(attr_value)
                end as attr_value
            from QS_ATTR_TEMP
            group by entity_id, attr_name
            order by entity_id, attr_name, list_index)
          select
            entity_id,
            JSON_OBJECT($VERSION_KEY, $CURRENT_VERSION, $ATTRS_KEY, JSON_OBJECTAGG(attr_name, attr_value))
          from CTE
          group by entity_id;""".asUpdate

  /**
    * Update the ENTITY table with the compact entities stored in QS_ENTITY_TEMP.
    */
  def migrationUpdateEntityTable(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY e
              join QS_ENTITY_TEMP tmp
              on e.id = tmp.entity_id
              set e.attributes = tmp.attributes
              where e.workspace_id = $workspaceId
              and deleted = 0;""".asUpdate

  /** Drop the temp table */
  def migrationDropAttributeTempTable: ReadWriteAction[Int] =
    sql"""drop temporary table QS_ATTR_TEMP;""".asUpdate

  /** Drop the temp table */
  def migrationDropEntityTempTable: ReadWriteAction[Int] =
    sql"""drop temporary table QS_ENTITY_TEMP;""".asUpdate

  /** Insert references for the entities we just updated */
  def migrationAddReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""insert into ENTITY_REFS(workspace_id, from_entity_type, from_name, to_entity_type, to_name)
         select e.workspace_id,
          e.entity_type, e.name,
          r.entity_type, r.name
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea, ENTITY r
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId
         and e.deleted = 0
         and ea.value_entity_ref is not null
         and ea.value_entity_ref = r.id;""".asUpdate

  /** Delete the all_attribute_values text from a compact ENTITY.
      * Currently unused, but leaving here in case we change our mind.
      */
  @unused
  def migrationClearAllAttributesString(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY
          set all_attribute_values = null
          where workspace_id = $workspaceId;""".asUpdate

  /** Clean up legacy attributes for entities in a given workspace.
    * Currently unused, but leaving here in case we change our mind.
    */
  @unused
  def migrationDeleteLegacyReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""delete ea
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId""".asUpdate

}
