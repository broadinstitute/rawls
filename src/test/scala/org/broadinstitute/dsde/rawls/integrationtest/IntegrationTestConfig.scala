package org.broadinstitute.dsde.rawls.integrationtest

import java.io.File
import scala.collection.JavaConversions._

import com.typesafe.config.ConfigFactory

trait IntegrationTestConfig {
  // get config defined by Jenkins, which is where integration tests usually run.
  // as a fallback, get config from the usual rawls.conf
  val etcConf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))
  val jenkinsConf = ConfigFactory.parseFile(new File("jenkins.conf"))

  val orientConfig = jenkinsConf.withFallback(etcConf).getConfig("orientdb")
  val orientUrl = orientConfig.getString("connectionUrl")
  val orientRootUser = orientConfig.getString("rootUser")
  val orientRootPassword = orientConfig.getString("rootPassword")

  val methodRepoConfig = jenkinsConf.withFallback(etcConf).getConfig("methodrepo")
  val methodRepoServer = methodRepoConfig.getString("server")
  val executionServiceConfig = jenkinsConf.withFallback(etcConf).getConfig("executionservice")
  val executionServiceServer = executionServiceConfig.getString("server")

  val gcsConfig = jenkinsConf.withFallback(etcConf).getConfig("gcs")

  val integrationConfig = jenkinsConf.withFallback(etcConf).getConfig("integration")
  val integrationRunFullLoadTest = integrationConfig.getBoolean("runFullLoadTest")

  val ldapConfig = jenkinsConf.withFallback(etcConf).getConfig("userLdap")
  val ldapProviderUrl = ldapConfig.getString("providerUrl")
  val ldapUser = ldapConfig.getString("user")
  val ldapPassword = ldapConfig.getString("password")
  val ldapGroupDn = ldapConfig.getString("groupDn")
  val ldapMemberAttribute = ldapConfig.getString("memberAttribute")
  val ldapUserObjectClasses = ldapConfig.getStringList("userObjectClasses").toList
  val ldapUserAttributes = ldapConfig.getStringList("userAttributes").toList
  val ldapUserDnFormat = ldapConfig.getString("userDnFormat")
}
