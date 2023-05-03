package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.scalatest.concurrent.ScalaFutures
import slick.jdbc.meta.MTable

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers with ScalaFutures {

  import driver.api._

  behavior of "DataAccess"

  val rawTableNames: Seq[String] = runAndWait(MTable.getTables).map(_.name.name)

  val safeTableNames: Seq[String] = rawTableNames flatMap {
    case "GROUP"                 => None
    case "USER"                  => None
    case "MANAGED_GROUP"         => None
    case "DATABASECHANGELOG"     => None // managed by Liquibase
    case "DATABASECHANGELOGLOCK" => None // managed by Liquibase
    case other                   => Option(other)
  }

  safeTableNames foreach { tableName =>
    it should s"test that truncateAll has left table '$tableName' in a known empty state" in withEmptyTestDatabase {
      val count = sql"SELECT COUNT(*) FROM #$tableName "
      assertResult(0, tableName + " not empty") {
        runAndWait(count.as[Int].head)
      }
    }
  }

  it should "use utf8 (which is an alias for utf8mb3) for MySQL's character_set_server" in withEmptyTestDatabase {
    /* Our live-environment CloudSQL instances, including production, use character_set_server=utf8.
       This setting is critical for SQL queries that specify/override a collation, such as to make a
       case-sensitive query against a column that is case-insensitive by default, e.g. the column
       uses utf8_general_ci collation.

       TODO: what is the CloudSQL default for 8.0, and do we have a flag set?
       See also https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-utf8mb3.html

       A failure of this test likely means that our test environment is not set up correctly; the mysql
       instance used by tests does not have the right setting. If the test-instance mysql is not set up
       correctly, other tests can fail and/or return false positives.
     */
    val charsetLookup =
      runAndWait(sql"""SHOW VARIABLES WHERE Variable_name = 'character_set_server';""".as[(String, String)])
    charsetLookup should have size 1
    withClue(
      "is the mysql against which these unit tests ran set up correctly with --character-set-server=utf8 or equivalent?"
    ) {
      charsetLookup.head._2 shouldBe "utf8mb3"
    }
  }
}
