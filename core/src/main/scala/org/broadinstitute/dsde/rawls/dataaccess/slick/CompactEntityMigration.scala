package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.MySQLProfile.api._

/**
  * SQL queries for migrating legacy entities to compact entities.
  */
trait CompactEntityMigration {
  this: CompactEntityQuery =>

  /** get the current value of the sort_buffer_size setting */
  def getSortBufferSetting: ReadAction[Long] =
    sql"""SELECT @@sort_buffer_size;""".as[Long].head

  /** set MySQL's sort_buffer_size setting to a given value.
    * default 262144 = 256k
    * 8M = 8,388,608
    */
  def setSessionSortBuffer(bufferSize: Long) =
    sql"""SET SESSION sort_buffer_size = $bufferSize;""".asUpdate

}
