package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes.IamResourceType

import java.time.{OffsetDateTime, ZoneOffset}

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
                       expiration: OffsetDateTime
  ) = FastPassGrant(-1L,
                    workspaceId,
                    userSubjectId,
                    accountEmail,
                    accountType,
                    resourceType,
                    resourceName,
                    organizationRole,
                    expiration,
                    OffsetDateTime.now(ZoneOffset.UTC)
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
  expiration: OffsetDateTime,
  created: OffsetDateTime
)
