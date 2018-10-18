package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.meta.MTable

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  import driver.api._

  "DataAccess" should "test that truncateAll has left the DB in a known empty state" in withEmptyTestDatabase {
    val rawTableNames: Seq[String] = runAndWait(MTable.getTables).map(_.name.name)

    val safeTableNames: Seq[String] = rawTableNames flatMap {
      case "GROUP" => None
      case "USER" => None
      case "MANAGED_GROUP" => None
      case "DATABASECHANGELOG" => None        // managed by Liquibase
      case "DATABASECHANGELOGLOCK" => None    // managed by Liquibase
      case other => Option(other)
    }

    safeTableNames foreach { tableName =>
      val count = sql"SELECT COUNT(*) FROM #$tableName "
      assertResult(0, tableName + " not empty") {
        runAndWait(count.as[Int].head)
      }
    }
  }
}
