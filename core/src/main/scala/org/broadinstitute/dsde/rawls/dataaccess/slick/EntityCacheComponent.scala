package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor
import slick.jdbc.JdbcProfile

import java.sql.Timestamp
import java.util.UUID

case class EntityCacheRecord(workspaceId: UUID, entityCacheLastUpdated: Timestamp, errorMessage: Option[String])

trait EntityCacheComponent {
  this: DriverComponent =>

  import driver.api._

  class EntityCacheTable(tag: Tag) extends Table[EntityCacheRecord](tag, "WORKSPACE_ENTITY_CACHE") {
    def workspaceId = column[UUID]("workspace_id", O.PrimaryKey)
    def entityCacheLastUpdated = column[Timestamp]("entity_cache_last_updated",
      O.SqlType("TIMESTAMP(6)"), O.Default(EntityStatisticsCacheMonitor.MIN_CACHE_TIME))
    def errorMessage = column[Option[String]]("error_message")

    def * = (workspaceId, entityCacheLastUpdated, errorMessage) <> (EntityCacheRecord.tupled, EntityCacheRecord.unapply)
  }

  object entityCacheQuery extends TableQuery(new EntityCacheTable(_)) with RawSqlQuery with LazyLogging {

    val driver: JdbcProfile = EntityCacheComponent.this.driver

    def findMostOutdatedEntityCachesAfter(minCacheTime: Timestamp, maxModifiedTime: Timestamp, numResults: Int = 10): ReadAction[Seq[(UUID, Timestamp, Option[Timestamp])]] = {
      // Find the workspace that has the entity cache that is the most out of date:
      // A. Workspace has a cacheLastUpdated date that is not current ("current" means equal to lastModified)
      // B. cacheLastUpdated is after @param minCacheTime
      // C. lastModified is before @param maxModifiedTime, meaning the workspace isn't likely actively being updated
      // D. Ordered by lastModified from oldest to newest. Meaning, return the workspace that was modified the longest ago
      sql"""SELECT w.id, w.last_modified, c.entity_cache_last_updated
                                |FROM WORKSPACE w LEFT OUTER JOIN WORKSPACE_ENTITY_CACHE c
                                |    on w.id = c.workspace_id
                                |where (c.entity_cache_last_updated > $minCacheTime or c.entity_cache_last_updated is null)
                                |  and w.last_modified < $maxModifiedTime
                                |  and (c.entity_cache_last_updated < w.last_modified or c.entity_cache_last_updated is null)
                                |order by w.last_modified asc limit $numResults""".stripMargin.as[(UUID, Timestamp, Option[Timestamp])]
    }

    // insert if not exist
    def updateCacheLastUpdated(workspaceId: UUID, timestamp: Timestamp, errorMessage: Option[String] = None): ReadWriteAction[Int] = {
      entityCacheQuery.insertOrUpdate(EntityCacheRecord(workspaceId, timestamp, errorMessage))
    }

    // TODO: probably rename this method, I expect an "exists" method to return a boolean
    /** does an entity cache exist at all, regardless of how current it is?
      *  returns None if no cache exists
      *  returns Some[Int] if a cache exists. The integer is the number of seconds
      *   by which the cache is stale; this will be zero if the cache is up-to-date
      * */
    def entityCacheExists(workspaceId: UUID): ReadAction[Option[Int]] = {
      val baseQuery = sql"""
                      select TIMESTAMPDIFF(SECOND, c.entity_cache_last_updated, w.last_modified) as staleness
                        from WORKSPACE w, WORKSPACE_ENTITY_CACHE c
                        where c.workspace_id = w.id
                        and c.workspace_id = $workspaceId;""".as[Int]

      uniqueResult[Int](baseQuery)
    }

    /** does an up-to-date entity cache exist? */
    // currently unused except in tests
    def isEntityCacheCurrent(workspaceId: UUID): ReadAction[Boolean] = {
      val baseQuery = sql"""SELECT EXISTS(
              SELECT 1
                FROM WORKSPACE w, WORKSPACE_ENTITY_CACHE c
                WHERE
                  w.id = $workspaceId
                  and w.id = c.workspace_id
                  and w.last_modified = c.entity_cache_last_updated
                LIMIT 1);""".as[Int]

      uniqueResult[Int](baseQuery).map { existsResult =>
        existsResult.contains(1)
      }
    }

  }

}
