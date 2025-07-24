package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName

import java.util.UUID
import slick.jdbc.MySQLProfile.api._
import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * Queries for working with the compact entity keys cache,
 * i.e. the ENTITY_KEYS_CACHE table
 */
trait CompactEntityKeysCache {
  this: CompactEntityQuery =>

  // ========== save to cache ==========

  /** save the cache for the given entity type and workspace */
  def saveCache(workspaceId: UUID, entityType: String, keys: Set[AttributeName]): ReadWriteAction[Int] =
    saveCache(workspaceId, Set(EntityTypeAndAttributeKeys(entityType, keys)))

  /** save multiple cache values for the given workspace */
  def saveCache(workspaceId: UUID, cacheValues: Set[EntityTypeAndAttributeKeys]) =
    // short-circuit
    if (cacheValues.isEmpty) {
      DBIO.successful(0)
    } else {
      val valueClauses = cacheValues.map { case EntityTypeAndAttributeKeys(entityType, keys) =>
        // build json array value of keys
        val sortedKeys = keys.toSeq.map(x => AttributeName.toDelimitedName(x)).sorted.toJson.compactPrint
        sql"($workspaceId, $entityType, $sortedKeys, CURRENT_TIMESTAMP(6))"
      }.toSeq

      val values = reduceSqlActionsWithDelim(valueClauses, sql"")

      sqlu"""insert into ENTITY_KEYS_CACHE (workspace_id, entity_type, attribute_keys, cached_at)
              values $values as vals
              on duplicate key update
                ENTITY_KEYS_CACHE.attribute_keys = vals.attribute_keys,
                ENTITY_KEYS_CACHE.cached_at = CURRENT_TIMESTAMP(6);"""
    }

  // ========== cache invalidation ==========

  /** Invalidate the cache for all entity types for this workspace */
  def invalidateCache(workspaceId: UUID): ReadWriteAction[Int] =
    sqlu"""update ENTITY_KEYS_CACHE
            set invalidated_at = CURRENT_TIMESTAMP(6)
            where workspace_id = $workspaceId;"""

  /** Invalidate the cache for the given entity type and workspace */
  def invalidateCache(workspaceId: UUID, entityType: String): ReadWriteAction[Int] =
    invalidateCache(workspaceId, Set(entityType))

  /** Invalidate the cache for the given entity types and workspace */
  def invalidateCache(workspaceId: UUID, entityTypes: Set[String]): ReadWriteAction[Int] =
    // short-circuit
    if (entityTypes.isEmpty) {
      DBIO.successful(0)
    } else {
      val inClause = reduceSqlActionsWithDelim(entityTypes.map(t => sql"$t").toSeq, sql", ")
      sqlu"""update ENTITY_KEYS_CACHE
              set invalidated_at = CURRENT_TIMESTAMP(6)
              where workspace_id = $workspaceId
              and entity_type in ($inClause);"""
    }

  // ========== delete from cache ==========

  /** delete all cache entries for this workspace */
  def deleteCache(workspaceId: UUID): ReadWriteAction[Int] =
    sqlu"""delete from ENTITY_KEYS_CACHE
            where workspace_id = $workspaceId;"""

  /** delete the cache entry for the given entity type and workspace */
  def deleteCache(workspaceId: UUID, entityType: String): ReadWriteAction[Int] =
    deleteCache(workspaceId, Set(entityType))

  /** delete the cache entries for the given entity types and workspace */
  def deleteCache(workspaceId: UUID, entityTypes: Set[String]): ReadWriteAction[Int] =
    // short-circuit
    if (entityTypes.isEmpty) {
      DBIO.successful(0)
    } else {
      val inClause = reduceSqlActionsWithDelim(entityTypes.map(t => sql"$t").toSeq, sql", ")
      sqlu"""delete from ENTITY_KEYS_CACHE
              where workspace_id = $workspaceId
              and entity_type in ($inClause);"""
    }

}
