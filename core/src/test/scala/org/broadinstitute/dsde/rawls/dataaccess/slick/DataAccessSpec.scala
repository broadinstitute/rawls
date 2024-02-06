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

  it should "use utf8mb3 (via the utf8 alias) for MySQL's character_set_server" in withEmptyTestDatabase {
    /* Our live-environment CloudSQL instances, including production, use character_set_server=utf8.
       This setting is critical for SQL queries that specify/override a collation, such as to make a
       case-sensitive query against a column that is case-insensitive by default, e.g. the column
       uses utf8_general_ci collation.

       In MySQL 8, "utf8" is an alias for "utf8mb3". So, while we specify "utf8" because CloudSQL restricts our options,
       MySQL reports that it is running with "utf8mb3". This test looks for a value of "utf8mb3".

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
      actual shouldBe "utf8mb3"
    }
  }

  it should "use utf8mb3_general_ci (via the utf8_general_ci alias) for MySQL's collation_server" in withEmptyTestDatabase {
    /* See previous test for more details. utf8_general_ci is the collation associated with a character set of utf8.

       Similar to how "utf8" is an alias for "utf8mb3", "utf8_general_ci" is an alias for "utf8mb3_general_ci". So, this
       test looks for "utf8mb3_general_ci".
     */
    val collationLookup =
      runAndWait(sql"""SHOW VARIABLES WHERE Variable_name = 'collation_server';""".as[(String, String)])
    collationLookup should have size 1
    val actual = collationLookup.head._2
    info(s"actual collation set: $actual")
    withClue(
      "is the mysql against which these unit tests ran set up correctly with --collation_server=utf8_general_ci or equivalent?"
    ) {
      actual shouldBe "utf8mb3_general_ci"
    }
  }

  it should "be testing against MySQL 8.0.35" in withEmptyTestDatabase {
    val versionLookup =
      runAndWait(sql"""select version();""".as[String])
    versionLookup should have size 1
    val actual = versionLookup.head
    info(s"actual version string: $actual")
    withClue(
      "is the mysql against which these unit tests running 8.0.35?"
    ) {
      actual should startWith("8.0.35")
    }
  }

}
