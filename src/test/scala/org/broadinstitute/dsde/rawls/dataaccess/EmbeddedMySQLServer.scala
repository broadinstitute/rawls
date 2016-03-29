package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException

import java.io.File
import java.sql.DriverManager

import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcDriver

import com.mysql.management.MysqldResource
import com.mysql.management.MysqldResourceI
import com.mysql.management.util.QueryUtil

import scala.collection.JavaConversions._

class EmbeddedMySQLServer(databaseConfig: DatabaseConfig[JdbcDriver]) {
  private val conf = databaseConfig.config.getConfig("db")
  private val url = conf.getString("url")
  private val user = conf.getString("user")
  private val password = conf.getString("password")

  private val resource: MysqldResource = {
    val baseDbDir = System.getProperty("java.io.tmpdir")
    val dbName = conf.getString("name")

    new MysqldResource(new File(baseDbDir, dbName))
  }


  def start = {
    if (!resource.isRunning()) {

      val dbOptions = Map(
        MysqldResourceI.PORT -> conf.getString("port"),
        MysqldResourceI.INITIALIZE_USER -> "true",
        MysqldResourceI.INITIALIZE_USER_NAME -> user,
        MysqldResourceI.INITIALIZE_PASSWORD -> password
      )

      resource.start("embedded-mysqld-thread-" + System.currentTimeMillis(), dbOptions)

      if (!resource.isRunning()) {
        throw new RawlsException("Embedded MySQL did not start.")
      }
    }

    // fail-fast by testing connection on init

    val conn = DriverManager.getConnection(url, user, password)
    val queryForString = new QueryUtil(conn).queryForString("SELECT VERSION()")
  }

  def shutdown = {
    if (resource.isRunning()) {
      resource.shutdown()

      if (resource.isRunning()) {
        throw new RawlsException("Embedded MySQL did not stop.")
      }
    }
  }

}
