package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.meta.MTable

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  import driver.api._

  "DataAccess" should "test that truncateAll has left the DB in a known empty state" in withEmptyTestDatabase {
    runAndWait(MTable.getTables).map(_.name.name) foreach { tableName =>
      val safeTableName = tableName match {
        case "GROUP" => "`GROUP`"
        case "USER" => "`USER`"
        case _ => tableName
      }

      val count = sql"SELECT COUNT(*) FROM #$safeTableName "
      assertResult(0, tableName + " not empty") {
        runAndWait(count.as[Int].head)
      }
    }
  }
}
