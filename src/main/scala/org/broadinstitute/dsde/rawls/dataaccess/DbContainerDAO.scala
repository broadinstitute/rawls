package org.broadinstitute.dsde.rawls.dataaccess

/**
 * Created by mbemis on 8/27/15.
 */
case class DbContainerDAO(
  workflowDAO: WorkflowDAO,
  workspaceDAO: WorkspaceDAO,
  entityDAO: EntityDAO,
  methodConfigurationDAO: MethodConfigurationDAO,
  authDAO: AuthDAO,
  billingDAO: BillingDAO,
  submissionDAO: SubmissionDAO
)
