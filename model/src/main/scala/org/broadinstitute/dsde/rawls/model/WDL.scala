package org.broadinstitute.dsde.rawls.model

trait WDL {
  def cacheKey: String
}

case class WdlSource(source: String) extends WDL {
  override def cacheKey: String = source
}

case class WdlUrl(url: String) extends WDL {
  override def cacheKey: String = url
}
