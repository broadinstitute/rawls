package org.broadinstitute.dsde.rawls.config

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class SpendReportingServiceConfig(defaultTableName: String, serviceProject: GoogleProject, maxDateRange: Int)
