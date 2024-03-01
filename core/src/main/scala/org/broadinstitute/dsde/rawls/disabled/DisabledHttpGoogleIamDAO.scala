package org.broadinstitute.dsde.rawls.disabled

import com.google.api.services.cloudresourcemanager.model.{Policy => ProjectPolicy}
import com.google.api.services.iam.v1.model.Role
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.google.iam.Expr

import scala.concurrent.Future

class DisabledHttpGoogleIamDAO extends GoogleIamDAO{
  def findServiceAccount(serviceAccountProject: GoogleProject, serviceAccountName: ServiceAccountName): Future[Option[google.ServiceAccount]] =
    throw new NotImplementedError("findServiceAccount is not implemented for Azure.")
  def findServiceAccount(serviceAccountProject: GoogleProject, serviceAccountEmail: WorkbenchEmail): Future[Option[google.ServiceAccount]] =
    throw new NotImplementedError("findServiceAccount is not implemented for Azure.")
  def createServiceAccount(serviceAccountProject: GoogleProject, serviceAccountName: ServiceAccountName, displayName: ServiceAccountDisplayName): Future[google.ServiceAccount] =
    throw new NotImplementedError("createServiceAccount is not implemented for Azure.")
  def removeServiceAccount(serviceAccountProject: GoogleProject, serviceAccountName: ServiceAccountName): Future[Unit] =
    throw new NotImplementedError("removeServiceAccount is not implemented for Azure.")
  def testIamPermission(project: GoogleProject, iamPermissions: Set[IamPermission]): Future[Set[IamPermission]] =
    throw new NotImplementedError("testIamPermission is not implemented for Azure.")
  def addRoles(iamProject: GoogleProject, userEmail: WorkbenchEmail, memberType: IamMemberType, rolesToAdd: Set[String], retryIfGroupDoesNotExist: Boolean = false, condition: Option[Expr] = None): Future[Boolean] =
    throw new NotImplementedError("addRoles is not implemented for Azure.")
  def removeRoles(iamProject: GoogleProject, userEmail: WorkbenchEmail, memberType: IamMemberType, rolesToRemove: Set[String], retryIfGroupDoesNotExist: Boolean = false): Future[Boolean] =
    throw new NotImplementedError("removeRoles is not implemented for Azure.")

  def getProjectPolicy(iamProject: GoogleProject): Future[ProjectPolicy] =
    throw new NotImplementedError("getProjectPolicy is not implemented for Azure.")
  def addIamPolicyBindingOnServiceAccount(serviceAccountProject: GoogleProject,
                                          serviceAccount: WorkbenchEmail,
                                          member: WorkbenchEmail,
                                          rolesToAdd: Set[String]
                                         ): Future[Unit] =
    throw new NotImplementedError("addIamPolicyBindingOnServiceAccount is not implemented for Azure.")
  def addServiceAccountUserRoleForUser(serviceAccountProject: GoogleProject,
                                       serviceAccountEmail: WorkbenchEmail,
                                       userEmail: WorkbenchEmail
                                      ): Future[Unit] =
    throw new NotImplementedError("addServiceAccountUserRoleForUser is not implemented for Azure.")
  def createServiceAccountKey(serviceAccountProject: GoogleProject,
                              serviceAccountEmail: WorkbenchEmail
                             ): Future[ServiceAccountKey] =
    throw new NotImplementedError("createServiceAccountKey is not implemented for Azure.")
  def removeServiceAccountKey(serviceAccountProject: GoogleProject,
                              serviceAccountEmail: WorkbenchEmail,
                              keyId: ServiceAccountKeyId
                             ): Future[Unit] =
    throw new NotImplementedError("removeServiceAccountKey is not implemented for Azure.")
  def listServiceAccountKeys(serviceAccountProject: GoogleProject,
                             serviceAccountEmail: WorkbenchEmail
                            ): Future[Seq[ServiceAccountKey]] =
    throw new NotImplementedError("listServiceAccountKeys is not implemented for Azure.")
  def listUserManagedServiceAccountKeys(serviceAccountProject: GoogleProject,
                                        serviceAccountEmail: WorkbenchEmail
                                       ): Future[Seq[ServiceAccountKey]] =
    throw new NotImplementedError("listUserManagedServiceAccountKeys is not implemented for Azure.")
  def getOrganizationCustomRole(roleName: String): Future[Option[Role]] =
    throw new NotImplementedError("getOrganizationCustomRole is not implemented for Azure.")
}
