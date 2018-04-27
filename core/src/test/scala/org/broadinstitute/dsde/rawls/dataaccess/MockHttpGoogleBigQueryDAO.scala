package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode

class MockHttpGoogleBigQueryDAO(appName: String,
                                googleCredentialMode: GoogleCredentialMode,
                                workbenchMetricBaseName: String) extends HttpGoogleBigQueryDAO(appName, googleCredentialMode, workbenchMetricBaseName)