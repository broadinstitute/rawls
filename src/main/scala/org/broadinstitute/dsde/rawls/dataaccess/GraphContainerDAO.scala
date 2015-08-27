package org.broadinstitute.dsde.rawls.dataaccess

/**
 * Created by mbemis on 8/27/15.
 */
case class GraphContainerDAO(workflowDAO: GraphWorkflowDAO, workspaceDAO: GraphWorkspaceDAO, entityDAO: GraphEntityDAO, methodConfigurationDAO: GraphMethodConfigurationDAO, submissionDAO: SubmissionDAO)
