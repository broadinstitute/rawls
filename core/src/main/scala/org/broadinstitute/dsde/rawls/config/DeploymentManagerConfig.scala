package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class DeploymentManagerConfig(
  templatePath: String,
  projectID: String,
  orgID: Long
)
case object DeploymentManagerConfig {
  def apply[T <: DeploymentManagerConfig](conf: Config): DeploymentManagerConfig = {
    val dmConfig = new DeploymentManagerConfig(conf.getString("templatePath"), conf.getString("projectId"), conf.getLong("orgID"))

    val badPathBecauseGithub = dmConfig.templatePath.contains("github.com")
    if(badPathBecauseGithub) {
      throw new RuntimeException("dmConfig.templatePath refers to GitHub; make sure you hit the Raw button to get the non-HTML version at the https://raw.githubusercontent.com/ domain")
    }

    val badPathBecauseBranchName = dmConfig.templatePath.contains("githubusercontent.com") &&
      (dmConfig.templatePath.contains("/blob/master") || dmConfig.templatePath.contains("/blob/develop"))
    if(badPathBecauseBranchName) {
      throw new RuntimeException("dmConfig.templatePath refers to a branch of a GitHub repo. This makes it impossible to know which template was used to create a project. Please use a specific commit instead.")
    }

    dmConfig
  }
}
