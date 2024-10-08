package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import slick.jdbc._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.language.postfixOps

/**
  * model class for rows in the ENTITY table, used for high-level Slick operations
  */
case class JsonEntitySlickRecord(id: Long,
                                 name: String,
                                 entityType: String,
                                 workspaceId: UUID,
                                 recordVersion: Long,
                                 deleted: Boolean,
                                 deletedDate: Option[Timestamp],
                                 attributes: Option[String]
) {
  def toEntity: Entity =
    Entity(name, entityType, attributes.getOrElse("{}").parseJson.convertTo[AttributeMap])
}

/**
  * model class for rows in the ENTITY table, used for low-level raw SQL operations
  */
// TODO AJ-2008: handle the all_attribute_values column?
// TODO AJ-2008: probably don't need deletedDate here
case class JsonEntityRecord(id: Long,
                            name: String,
                            entityType: String,
                            workspaceId: UUID,
                            recordVersion: Long,
                            deleted: Boolean,
                            deletedDate: Option[Timestamp],
                            attributes: JsValue
) {
  def toEntity: Entity =
    Entity(name, entityType, attributes.convertTo[AttributeMap])
  def toSlick: JsonEntitySlickRecord =
    JsonEntitySlickRecord(id,
                          name,
                          entityType,
                          workspaceId,
                          recordVersion,
                          deleted,
                          deletedDate,
                          Some(attributes.compactPrint)
    )
}

/**
  * abbreviated model for rows in the ENTITY table when we don't need all the columns
  */
case class JsonEntityRefRecord(id: Long, name: String, entityType: String)

/**
  * model class for rows in the ENTITY_REFS table
  */
case class RefPointerRecord(fromId: Long, toId: Long)

/**
  * companion object for constants, etc.
  */
object JsonEntityComponent {
  // the length of the all_attribute_values column, which is TEXT, minus a few bytes because i'm nervous
  val allAttributeValuesColumnSize = 65532
}

/**
  * Slick component for reading/writing JSON-based entities
  */
trait JsonEntityComponent extends LazyLogging {
  this: DriverComponent =>

  import slick.jdbc.MySQLProfile.api._

  // json codec for entity attributes
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  /** high-level Slick table for ENTITY */
  class JsonEntityTable(tag: Tag) extends Table[JsonEntitySlickRecord](tag, "ENTITY") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name", O.Length(254))
    def entityType = column[String]("entity_type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def version = column[Long]("record_version")
    def deleted = column[Boolean]("deleted")
    def deletedDate = column[Option[Timestamp]]("deleted_date")
    def attributes = column[Option[String]]("attributes")

    // TODO AJ-2008: are these useful?
    // def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    // def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)

    def * =
      (id, name, entityType, workspaceId, version, deleted, deletedDate, attributes) <> (JsonEntitySlickRecord.tupled,
                                                                                         JsonEntitySlickRecord.unapply
      )
  }

  /** high-level Slick table for ENTITY_REFS */
  class JsonEntityRefTable(tag: Tag) extends Table[RefPointerRecord](tag, "ENTITY_REFS") {
    def fromId = column[Long]("from_id")
    def toId = column[Long]("to_id")

    def * =
      (fromId, toId) <> (RefPointerRecord.tupled, RefPointerRecord.unapply)
  }

  /** high-level Slick query object for ENTITY */
  object jsonEntityRefSlickQuery extends TableQuery(new JsonEntityRefTable(_)) {}

  /** high-level Slick query object for ENTITY_REFS */
  object jsonEntitySlickQuery extends TableQuery(new JsonEntityTable(_)) {}

  /** low-level raw SQL queries for ENTITY */
  object jsonEntityQuery extends RawSqlQuery {
    val driver: JdbcProfile = JsonEntityComponent.this.driver

    // read a json column from the db and translate into a JsValue
    implicit val GetJsValueResult: GetResult[JsValue] = GetResult(r => r.nextString().parseJson)

    // write a JsValue to the database by converting it to a string (the db column is still JSON)
    implicit object SetJsValueParameter extends SetParameter[JsValue] {
      def apply(v: JsValue, pp: PositionedParameters): Unit =
        pp.setString(v.compactPrint)
    }

    // select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
    // into a JsonEntityRecord
    implicit val getJsonEntityRecord: GetResult[JsonEntityRecord] =
      GetResult(r => JsonEntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    implicit val getJsonEntityRefRecord: GetResult[JsonEntityRefRecord] =
      GetResult(r => JsonEntityRefRecord(r.<<, r.<<, r.<<))

    /**
      * Insert a single entity to the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] = {
      val attributesJson: JsValue = entity.attributes.toJson

      sqlu"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
          values (${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""
    }

    /**
      * Update a single entity in the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    // TODO AJ-2008: can this use INSERT ... ON DUPLICATE KEY UPDATE instead? That would allow batching multiple updates
    //  into a single statement. But, how would that work with record_version checking?
    def updateEntity(workspaceId: UUID, entity: Entity, recordVersion: Long): ReadWriteAction[Int] = {
      val attributesJson: JsValue = entity.attributes.toJson

      sqlu"""update ENTITY set record_version = record_version+1, attributes = $attributesJson
            where workspace_id = $workspaceId and entity_type = ${entity.entityType} and name = ${entity.name}
            and record_version = $recordVersion;
          """
    }

    /**
      * Read a single entity from the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def getEntity(workspaceId: UUID, entityType: String, entityName: String): ReadAction[Option[JsonEntityRecord]] = {
      val selectStatement: SQLActionBuilder =
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and name = $entityName"""

      uniqueResult(selectStatement.as[JsonEntityRecord])
    }

    /**
      * Read a single entity from the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def getEntityRef(workspaceId: UUID,
                     entityType: String,
                     entityName: String
    ): ReadAction[Option[JsonEntityRefRecord]] = {
      val selectStatement: SQLActionBuilder =
        sql"""select id, name, entity_type
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and name = $entityName"""

      uniqueResult(selectStatement.as[JsonEntityRefRecord])
    }

    /**
      * All entity types for the given workspace, with their counts of active entities
      */
    def typesAndCounts(workspaceId: UUID): ReadAction[Seq[(String, Int)]] =
      sql"""select entity_type, count(1) from ENTITY where workspace_id = $workspaceId and deleted = 0 group by entity_type"""
        .as[(String, Int)]

    /**
      * All attribute names for the given workspace, paired to their entity type
      * The ENTITY_KEYS table is automatically populated via triggers on the ENTITY table; see the db
      * to understand those triggers.
      */
    // TODO AJ-2008: assess performance of ENTITY_KEYS.attribute_keys vs JSON_KEYS(ENTITY.attributes)
    def typesAndAttributes(workspaceId: UUID): ReadAction[Seq[(String, String)]] =
      sql"""SELECT DISTINCT entity_type, json_key FROM ENTITY_KEYS,
              JSON_TABLE(attribute_keys, '$$[*]' COLUMNS(json_key VARCHAR(256) PATH '$$')) t
            where workspace_id = $workspaceId;"""
        .as[(String, String)]

    def typesAndAttributesV2(workspaceId: UUID): ReadAction[Seq[(String, String)]] =
      sql"""SELECT DISTINCT entity_type, json_key FROM ENTITY,
              JSON_TABLE(json_keys(attributes), '$$[*]' COLUMNS(json_key VARCHAR(256) PATH '$$')) t
            where workspace_id = $workspaceId;"""
        .as[(String, String)]

    def queryEntities(workspaceId: UUID, entityType: String, queryParams: EntityQuery): ReadAction[Seq[Entity]] = {

      val offset = queryParams.pageSize * (queryParams.page - 1)

      // get the where clause from the shared method
      val whereClause: SQLActionBuilder = queryWhereClause(workspaceId, entityType, queryParams)

      // sorting
      val orderByClause: SQLActionBuilder = queryParams.sortField match {
        case "name" => sql" order by name #${SortDirections.toSql(queryParams.sortDirection)} "
        case attr =>
          sql" order by JSON_EXTRACT(attributes, '$$.#$attr') #${SortDirections.toSql(queryParams.sortDirection)} "
      }

      // TODO AJ-2008: full-table text search
      // TODO AJ-2008: filter by column
      // TODO AJ-2008: result projection

      val query = concatSqlActions(
        sql"select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes from ENTITY ",
        whereClause,
        orderByClause,
        sql" limit #${queryParams.pageSize} offset #$offset"
      )

      query.as[JsonEntityRecord].map(results => results.map(_.toEntity))
    }

    /**
      * Count the number of entities that match the query, before applying all filters
      */
    def countType(workspaceId: UUID, entityType: String): ReadAction[Int] =
      singleResult(
        sql"select count(1) from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and deleted = 0"
          .as[Int]
      )

    /**
      * Count the number of entities that match the query, after applying all filters
      */
    def countQuery(workspaceId: UUID, entityType: String, queryParams: EntityQuery): ReadAction[Int] = {
      // get the where clause from the shared method
      val whereClause = queryWhereClause(workspaceId, entityType, queryParams)

      val query = concatSqlActions(
        sql"select count(1) from ENTITY ",
        whereClause
      )
      singleResult(query.as[Int])
    }

    /**
      * Shared method to build the where-clause criteria for entityQuery. Used to generate the results and to generate the counts.
      */
    private def queryWhereClause(workspaceId: UUID, entityType: String, queryParams: EntityQuery): SQLActionBuilder =
      sql"where workspace_id = $workspaceId and entity_type = $entityType and deleted = 0"

    /** Given a set of entity references, retrieve those entities */
    def getEntities(workspaceId: UUID, refs: Set[AttributeEntityReference]): ReadAction[Seq[JsonEntityRecord]] =
      // short-circuit
      if (refs.isEmpty) {
        DBIO.successful(Seq.empty[JsonEntityRecord])
      } else {
        // group the entity type/name pairs by type
        val groupedReferences: Map[String, Set[String]] = refs.groupMap(_.entityType)(_.entityName)

        // build select statements for each type
        val queryParts: Iterable[SQLActionBuilder] = groupedReferences.map {
          case (entityType: String, entityNames: Set[String]) =>
            // build the "IN" clause values
            val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name").toSeq, sql",")

            // TODO AJ-2008: check query plan for this and make sure it is properly using indexes
            //   UNION query does use indexes for each select; but it also requires a temporary table to
            //   combine the results, and we can probably do better. `where (entity_type, name) in ((?, ?), (?, ?))
            //   looks like it works well
            // TODO AJ-2008: include `where deleted=0`? Make that an argument?
            concatSqlActions(
              sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
                from ENTITY where workspace_id = $workspaceId and entity_type = $entityType
                and name in (""",
              entityNamesSql,
              sql")"
            )
        }

        // union the select statements together
        val unionQuery = reduceSqlActionsWithDelim(queryParts.toSeq, sql" union ")

        // execute
        unionQuery.as[JsonEntityRecord](getJsonEntityRecord)
      }

    /** Given a set of entity references, retrieve those entities */
    // TODO AJ-2008: address lots of copy/paste between getEntities and getEntityRefs
    def getEntityRefs(workspaceId: UUID, refs: Set[AttributeEntityReference]): ReadAction[Seq[JsonEntityRefRecord]] =
      // short-circuit
      if (refs.isEmpty) {
        DBIO.successful(Seq.empty[JsonEntityRefRecord])
      } else {
        // group the entity type/name pairs by type
        val groupedReferences: Map[String, Set[String]] = refs.groupMap(_.entityType)(_.entityName)

        // build select statements for each type
        val queryParts: Iterable[SQLActionBuilder] = groupedReferences.map {
          case (entityType: String, entityNames: Set[String]) =>
            // build the "IN" clause values
            val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name").toSeq, sql",")

            // TODO AJ-2008: check query plan for this and make sure it is properly using indexes
            //   UNION query does use indexes for each select; but it also requires a temporary table to
            //   combine the results, and we can probably do better. `where (entity_type, name) in ((?, ?), (?, ?))
            //   looks like it works well
            // TODO AJ-2008: include `where deleted=0`? Make that an argument?
            concatSqlActions(
              sql"""select id, name, entity_type
                from ENTITY where workspace_id = $workspaceId and entity_type = $entityType
                and name in (""",
              entityNamesSql,
              sql")"
            )
        }

        // union the select statements together
        val unionQuery = reduceSqlActionsWithDelim(queryParts.toSeq, sql" union ")

        // execute
        unionQuery.as[JsonEntityRefRecord](getJsonEntityRefRecord)
      }

    /**
      * Returns the set of entities which directly reference the supplied targets
      */
    def getReferrers(workspaceId: UUID, targets: Set[AttributeEntityReference]) = {
      val inFragment = refsInFragment(targets)

      val baseSql = sql"""select referrer.id, referrer.name, referrer.entity_type
               from ENTITY referrer, ENTITY_REFS refs, ENTITY target
               where target.id = refs.to_id
               and referrer.id = refs.from_id
               and target.workspace_id = $workspaceId
               and (target.entity_type, target.name) in """

      concatSqlActions(baseSql, inFragment, sql";").as[JsonEntityRefRecord]
    }

    /**
      * Returns the set of entities which directly AND RECURSIVELY reference the supplied targets
      */
    def getRecursiveReferrers(workspaceId: UUID, targets: Set[AttributeEntityReference]) = {

      // max number of rows to consider in the recursive query.
      // this function will never return more than this many results. When called to validate delete requests,
      // it means that we will never return more than 10000 referrers for any given set of entities to be deleted.
      val recursionLimit = 10000

      // a recursive SQL query to retrieve all entities that refer to the ${targets} entities, plus all entities that
      // refer to the referring entities, plus all entities that refer to those, plus ...
      //
      // recursive SQL: https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive
      val startSql =
        sql"""WITH RECURSIVE ancestor AS (
	            select r.from_id, r.to_id
                from ENTITY_REFS r, ENTITY e
                where e.id = r.to_id
                and e.workspace_id = $workspaceId
                and (e.entity_type, e.name) in """

      val inFragment = refsInFragment(targets)

      val endSql =
        sql"""    UNION ALL
                select r.from_id, r.to_id
                from ancestor, ENTITY_REFS r
                where ancestor.from_id = r.to_id
                limit #$recursionLimit)
            select a.from_id, e.entity_type, e.name from ancestor a, ENTITY e
            where a.from_id = e.id;"""

      concatSqlActions(startSql, inFragment, endSql).as[JsonEntityRefRecord]

    }

    def softDelete(workspaceId: UUID, targets: Set[AttributeEntityReference]): ReadWriteAction[Int] = {
      // short-circuit
      if (targets.isEmpty) {
        return DBIO.successful(0)
      }
      val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000)
      val deletedDate = new Timestamp(new Date().getTime)

      val inFragment = refsInFragment(targets)

      val baseSql = sql"""update ENTITY set deleted=1, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix)
             where deleted=0 AND workspace_id=$workspaceId and (entity_type, name) in """

      concatSqlActions(baseSql, inFragment, sql";").asUpdate
    }

    def renameSingleEntity(workspaceId: UUID, entity: AttributeEntityReference, newName: String): ReadWriteAction[Int] =
      sql"""update ENTITY set name = $newName
            where workspace_id = $workspaceId
            and entity_type = ${entity.entityType}
            and name = ${entity.entityName};""".asUpdate

    private def refsInFragment(refs: Set[AttributeEntityReference]) = {
      // build select statements for each type
      val pairs = refs.map { ref =>
        sql"""(${ref.entityType}, ${ref.entityName})"""
      }
      concatSqlActions(sql"(", reduceSqlActionsWithDelim(pairs.toSeq, sql","), sql")")
    }

    def renameEmbeddedReferences(workspaceId: UUID,
                                 toId: Long,
                                 oldReference: AttributeEntityReference,
                                 newReference: AttributeEntityReference
    ): ReadWriteAction[Int] = {
      // build string to be replaced
      val oldStr = s"""{"entityName": "${oldReference.entityName}", "entityType": "${oldReference.entityType}"}"""
      // build string to be the replacement
      val newStr = s"""{"entityName": "${newReference.entityName}", "entityType": "${newReference.entityType}"}"""

      // perform replacements
      sql"""update ENTITY set attributes = REPLACE(attributes, $oldStr, $newStr)
            where workspace_id = $workspaceId and id in (
                select from_id from ENTITY_REFS er
                where er.to_id = $toId
            )""".asUpdate
    }

  }

  private def singleResult[V](results: ReadAction[Seq[V]]): ReadAction[V] =
    results map {
      case Seq()    => throw new RawlsException(s"Expected 1 result but found 0")
      case Seq(one) => one
      case tooMany  => throw new RawlsException(s"Expected 1 result but found ${tooMany.size}")
    }

}
