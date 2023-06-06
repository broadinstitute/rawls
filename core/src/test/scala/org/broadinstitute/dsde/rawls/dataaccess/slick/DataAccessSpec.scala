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

  it should "use utf8 for MySQL's character_set_server" in withEmptyTestDatabase {
    /* Our live-environment CloudSQL instances, including production, use character_set_server=utf8.
       This setting is critical for SQL queries that specify/override a collation, such as to make a
       case-sensitive query against a column that is case-insensitive by default, e.g. the column
       uses utf8_general_ci collation.

       A failure of this test likely means that our test environment is not set up correctly; the mysql
       instance used by tests does not have the right setting. If the test-instance mysql is not set up
       correctly, other tests can fail and/or return false positives.
     */
    val charsetLookup =
      runAndWait(sql"""SHOW VARIABLES WHERE Variable_name = 'character_set_server';""".as[(String, String)])
    charsetLookup should have size 1
    val actual = charsetLookup.head._2
    info(s"actual character set: $actual")
    withClue(
      "is the mysql against which these unit tests ran set up correctly with --character-set-server=utf8 or equivalent?"
    ) {
      actual shouldBe "utf8"
    }
  }

  it should "be testing against MySQL 5.7" in withEmptyTestDatabase {
    val versionLookup =
      runAndWait(sql"""select version();""".as[String])
    versionLookup should have size 1
    val actual = versionLookup.head
    info(s"actual version string: $actual")
    withClue(
      "is the mysql against which these unit tests running 5.7?"
    ) {
      actual should startWith("5.7")
    }
  }

}
