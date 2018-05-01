package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode

import scala.concurrent.ExecutionContext

class MockHttpGoogleBigQueryDAO(appName: String,
                                googleCredentialMode: GoogleCredentialMode,
                                workbenchMetricBaseName: String)
                               (implicit system: ActorSystem, executionContext: ExecutionContext)
  extends HttpGoogleBigQueryDAO(appName, googleCredentialMode, workbenchMetricBaseName)