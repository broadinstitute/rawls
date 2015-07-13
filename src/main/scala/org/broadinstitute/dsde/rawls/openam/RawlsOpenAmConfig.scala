package org.broadinstitute.dsde.rawls.openam

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.vault.common.util.ConfigUtil

class RawlsOpenAmConfig(conf: Config) {
  lazy val deploymentUri = conf.getString("deploymentUri")
  lazy val username = conf.getString("testUsername")
  lazy val password = conf.getString("testPassword")

  lazy val realm = ConfigUtil.getStringOption(conf, "realm")
  lazy val authIndexType = ConfigUtil.getStringOption(conf, "authIndexType")
  lazy val authIndexValue = ConfigUtil.getStringOption(conf, "authIndexValue")
}
