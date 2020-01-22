package org.broadinstitute.dsde.rawls.model

// The kernel of Cromwell's `cromwell.core.WorkflowSourceFilesCollection` - a workflow and its location, to enable relative imports
case class WDL(source: String, url: Option[String]) {

  // The same WDL at a different URL could have different relative imports, and is therefore a different domain object.
  // See `cromwell.languages.util.ParserCache#workflowHashKey` in the Cromwell workflow cache.
  def cacheKey: String = source + url.getOrElse("")
}
