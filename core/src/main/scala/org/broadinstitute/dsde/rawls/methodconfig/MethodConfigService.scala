package org.broadinstitute.dsde.rawls.methodconfig

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{MethodRepoDAO, SlickDataSource, SlickWorkspaceContext}
import org.broadinstitute.dsde.rawls.expressions.ExpressionValidator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{AgoraEntity, AgoraMethodConfiguration, AttributeString, ErrorReport, MethodConfiguration, MethodConfigurationName, MethodConfigurationNamePair, MethodRepoConfigurationExport, MethodRepoConfigurationImport, MethodRepoMethod, SamWorkspaceActions, UserInfo, ValidatedMethodConfiguration, WDL, WorkspaceName}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, JsonFilterUtils, MethodWiths, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import spray.json.JsonParser

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object MethodConfigService {
  def constructor(dataSource: SlickDataSource, methodRepoDAO: MethodRepoDAO, workbenchMetricBaseName: String)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext) = {

    new MethodConfigService(userInfo, dataSource, methodRepoDAO, workbenchMetricBaseName)
  }
}

class MethodConfigService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val methodRepoDAO: MethodRepoDAO, override val workbenchMetricBaseName: String)(implicit protected val executionContext: ExecutionContext)
  extends WorkspaceSupport with AttributeSupport with MethodWiths with LazyLogging with RawlsInstrumented with JsonFilterUtils {

  import dataSource.dataAccess.driver.api._

  def CreateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) = createMethodConfiguration(workspaceName, methodConfiguration)
  def RenameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName) = renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
  def DeleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def GetMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def OverwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = overwriteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def UpdateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = updateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) = copyMethodConfiguration(methodConfigNamePair)
  def CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationImport) = copyMethodConfigurationFromMethodRepo(query)
  def CopyMethodConfigurationToMethodRepo(query: MethodRepoConfigurationExport) = copyMethodConfigurationToMethodRepo(query)
  def ListAgoraMethodConfigurations(workspaceName: WorkspaceName) = listAgoraMethodConfigurations(workspaceName)
  def ListMethodConfigurations(workspaceName: WorkspaceName) = listMethodConfigurations(workspaceName)
  def CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) = createMethodConfigurationTemplate(methodRepoMethod)
  def GetMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod ) = getMethodInputsOutputs(userInfo, methodRepoMethod)
  def GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")))
            case None => createMCAndValidateExpressions(workspaceContext, methodConfiguration, dataAccess)
          }
        }
      }
    }
  }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          dataAccess.methodConfigurationQuery.delete(workspaceContext, methodConfigurationNamespace, methodConfigurationName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        //It's terrible that we pass unnecessary junk that we don't read in the payload, but a big refactor of the API is going to have to wait until Some Other Time.
        if(newName.workspaceName != workspaceName) {
          DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Workspace name and namespace in payload must match those in the URI")))
        } else {
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfiguration =>
            //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
            dataAccess.methodConfigurationQuery.get(workspaceContext, newName.namespace, newName.name) flatMap {
              case Some(_) if methodConfigurationNamespace != newName.namespace || methodConfigurationName != newName.name =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                  ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
              case Some(_) => DBIO.successful(()) //renaming self to self: no-op
              case None =>
                dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration.copy(name = newName.name, namespace = newName.namespace))
            } map (_ => RequestComplete(StatusCodes.NoContent))
          }
        }
      }
    }

  //validates the expressions in the method configuration, taking into account optional inputs
  private def validateMethodConfiguration(methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    withMethodInputs(methodConfiguration, userInfo) { gatherInputsResult =>
      val vmc = ExpressionValidator.validateAndParseMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity = methodConfiguration.rootEntityType.isDefined, dataAccess)
      DBIO.successful(vmc)
    }
  }

  def createMCAndValidateExpressions(workspaceContext: SlickWorkspaceContext, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration) flatMap { _ =>
      validateMethodConfiguration(methodConfiguration, dataAccess)
    }
  }

  def updateMCAndValidateExpressions(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration) flatMap { _ =>
      validateMethodConfiguration(methodConfiguration, dataAccess)
    }
  }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          validateMethodConfiguration(methodConfig, dataAccess) map { vmc =>
            PerRequest.RequestComplete(StatusCodes.OK, vmc)
          }
        }
      }
    }
  }

  //Overwrite the method configuration at methodConfiguration[namespace|name] with the new method configuration.
  def overwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource.inTransaction { dataAccess =>
          if(methodConfiguration.namespace != methodConfigurationNamespace || methodConfiguration.name != methodConfigurationName) {
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest,
              s"The method configuration name and namespace in the URI should match the method configuration name and namespace in the request body. If you want to move this method configuration, use POST.")))
          } else {
            createMCAndValidateExpressions(workspaceContext, methodConfiguration, dataAccess)
          }
        }
      }
    }
  }

  //Move the method configuration at methodConfiguration[namespace|name] to the location specified in methodConfiguration, _and_ update it.
  //It's like a rename and upsert all rolled into one.
  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { _ =>
            dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
              //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
              case Some(_) if methodConfigurationNamespace != methodConfiguration.namespace || methodConfigurationName != methodConfiguration.name =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                  ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
              case _ =>
                updateMCAndValidateExpressions(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration, dataAccess)
            } map (RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }
  }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result = getWorkspaceContextAndPermissions(mcnp.destination.workspaceName, SamWorkspaceActions.write) flatMap { destContext =>
      getWorkspaceContextAndPermissions(mcnp.source.workspaceName, SamWorkspaceActions.read) flatMap { sourceContext =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.methodConfigurationQuery.get(sourceContext, mcnp.source.namespace, mcnp.source.name) flatMap {
            case None =>
              val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = err))
            case Some(methodConfig) => DBIO.successful((methodConfig, destContext))
          }
        }
      }
    }

    transaction1Result flatMap { case (methodConfig, destContext) =>
      withAttributeNamespaceCheck(methodConfig) {
        dataSource.inTransaction { dataAccess =>
          saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[ValidatedMethodConfiguration] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo) flatMap {
      case None =>
        val name = s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) => Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
        withAttributeNamespaceCheck(targetMethodConfig) {
          getWorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName, SamWorkspaceActions.write) flatMap { destContext =>
            dataSource.inTransaction { dataAccess =>
              saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, dataAccess)
            }
          }
        }
      }
    }

  private def parseAgoraEntity(agoraEntity: AgoraEntity): Try[MethodConfiguration] = {
    val parsed = Try {
      agoraEntity.payload.map(JsonParser(_).convertTo[AgoraMethodConfiguration])
    } recoverWith {
      case e: Exception =>
        Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
    }

    parsed flatMap {
      case Some(agoraMC) => Success(convertToMethodConfiguration(agoraMC))
      case None => Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
    }
  }

  private def convertToMethodConfiguration(agoraMethodConfig: AgoraMethodConfiguration): MethodConfiguration = {
    MethodConfiguration(agoraMethodConfig.namespace, agoraMethodConfig.name, Some(agoraMethodConfig.rootEntityType), Some(Map.empty[String, AttributeString]), agoraMethodConfig.inputs, agoraMethodConfig.outputs, agoraMethodConfig.methodRepoMethod)
  }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodRepoQuery.source.namespace, methodRepoQuery.source.name, dataAccess) { methodConfig =>

          DBIO.from(methodRepoDAO.postMethodConfig(
            methodRepoQuery.methodRepoNamespace,
            methodRepoQuery.methodRepoName,
            methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace, name = methodRepoQuery.methodRepoName),
            userInfo)) map { RequestComplete(StatusCodes.OK, _) }
        }
      }
    }
  }

  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList.filter(_.methodRepoMethod.repo == Agora))
        }
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList)
        }
      }
    }

  def createMethodConfigurationTemplate(methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.toMethodConfiguration(userInfo, wdl, methodRepoMethod) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }
  }

  def getMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.getMethodInputsOutputs(userInfo, wdl) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(RequestComplete(StatusCodes.OK, inputsOutputs))
        }
      }
    }
  }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, destContext: SlickWorkspaceContext, dataAccess: DataAccess) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}")))
      case None => createMCAndValidateExpressions(destContext, target, dataAccess)
    }
  }

}
