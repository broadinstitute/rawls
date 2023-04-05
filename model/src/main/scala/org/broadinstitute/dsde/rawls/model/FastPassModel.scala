package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes.IamResourceType
import org.joda.time.DateTime

/**
  * Created by tlangs on 3/16/2023.
  */

object FastPassGrant {
  def newFastPassGrant(workspaceId: String,
                       userSubjectId: WorkbenchUserId,
                       accountEmail: WorkbenchEmail,
                       accountType: IamMemberType,
                       resourceType: IamResourceType,
                       resourceName: String,
                       organizationRole: String,
                       expiration: DateTime
  ) = FastPassGrant(-1L,
                    workspaceId,
                    userSubjectId,
                    accountEmail,
                    accountType,
                    resourceType,
                    resourceName,
                    organizationRole,
                    expiration,
                    DateTime.now()
  )
}
case class FastPassGrant(
  id: Long,
  workspaceId: String,
  userSubjectId: WorkbenchUserId,
  accountEmail: WorkbenchEmail,
  accountType: IamMemberType,
  resourceType: IamResourceType,
  resourceName: String,
  organizationRole: String,
  expiration: DateTime,
  created: DateTime
)
