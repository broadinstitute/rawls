package org.broadinstitute.dsde.test.pipeline

import io.circe.parser._

import java.util.Base64

object Predefined {
  val BillingProject: String = "BILLING_PROJECT"
}

trait PipelineInjector {
  def environmentName: String
  def usersMetadata: Seq[UserMetadata] =
    sys.env.get(environmentName) match {
      case Some(b64) =>
        val decoded = decode[Seq[UserMetadata]](new String(Base64.getDecoder.decode(b64), "UTF-8"))
        decoded match {
          case Right(u)    => u
          case Left(error) => Seq()
        }
      case _ => Seq()
    }

  def billingProject: String =
    sys.env.getOrElse(Predefined.BillingProject, "")
}

object PipelineInjector {
  def apply(envName: String): PipelineInjector = new PipelineInjector {
    override val environmentName: String = envName
  }
}
