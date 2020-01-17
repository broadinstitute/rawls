package org.broadinstitute.dsde.rawls.model

// The kernel of Cromwell's `cromwell.core.WorkflowSourceFilesCollection` - a workflow and its location, to enable relative imports
case class WDL(source: String, url: Option[String])
