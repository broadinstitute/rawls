package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.MethodRepository

case class MethodRepoConfig[T <: MethodRepository](baseUrl: String, path: String) {
  def serviceUrl: String = baseUrl + path
}

case object MethodRepoConfig {
  def apply[T <: MethodRepository](conf: Config): MethodRepoConfig[T] =
    new MethodRepoConfig[T](conf.getString("server"), conf.getString("path"))
}
