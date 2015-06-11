package org.broadinstitute.dsde.rawls

import java.io.File

import com.typesafe.config.ConfigFactory

trait IntegrationTestConfig {
  // get config defined by Jenkins, which is where integration tests usually run.
  // as a fallback, get config from the usual rawls.conf
  val etcConf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))
  val jenkinsConf = ConfigFactory.parseFile(new File("jenkins.conf"))

  val orientConfig = jenkinsConf.withFallback(etcConf).getConfig("orientdb")
  val orientServer = orientConfig.getString("server")
  val orientRootUser = orientConfig.getString("rootUser")
  val orientRootPassword = orientConfig.getString("rootPassword")

  val methodRepoConfig = jenkinsConf.withFallback(etcConf).getConfig("methodrepo")
  val methodRepoServer = methodRepoConfig.getString("server")

  val openAmConfig = jenkinsConf.withFallback(etcConf).getConfig("openam")
  val openAmUrl = openAmConfig.getString("tokenUrl")
  val openAmTestUser = openAmConfig.getString("testUser")
  val openAmTestUserPassword = openAmConfig.getString("testPassword")
}
