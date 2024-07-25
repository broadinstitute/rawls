package org.broadinstitute.dsde.rawls.methods
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.dataaccess.{MethodRepoDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityRequestArguments}
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.AgoraMethodConfigurationFormat
import org.broadinstitute.dsde.rawls.model.{
  Agora,
  AgoraEntity,
  AgoraMethodConfiguration,
  AttributeString,
  ErrorReport,
  ErrorReportSource,
  MethodConfiguration,
  MethodConfigurationName,
  MethodConfigurationNamePair,
  MethodConfigurationShort,
  MethodInputsOutputs,
  MethodRepoConfigurationExport,
  MethodRepoConfigurationImport,
  MethodRepoMethod,
  RawlsRequestContext,
  SamWorkspaceActions,
  UserInfo,
  ValidatedMethodConfiguration,
  WDL,
  Workspace,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, MethodWiths, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import slick.dbio.DBIO
import spray.json.JsonParser

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object MethodConfigurationService {
  def constructor(
    dataSource: SlickDataSource,
    samDAO: SamDAO,
    methodRepoDAO: MethodRepoDAO,
    methodConfigResolver: MethodConfigResolver,
    entityManager: EntityManager,
    workspaceRepository: WorkspaceRepository,
    workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): MethodConfigurationService =
    new MethodConfigurationService(ctx,
                                   dataSource,
                                   samDAO,
                                   methodRepoDAO,
                                   methodConfigResolver,
                                   entityManager,
                                   workspaceRepository,
                                   workbenchMetricBaseName
    )
}

class MethodConfigurationService(
  protected val ctx: RawlsRequestContext,
  val dataSource: SlickDataSource,
  val samDAO: SamDAO,
  val methodRepoDAO: MethodRepoDAO,
  val methodConfigResolver: MethodConfigResolver,
  val entityManager: EntityManager,
  val workspaceRepository: WorkspaceRepository,
  override val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented
    with WorkspaceSupport
    with AttributeSupport
    with MethodWiths {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def getMethodConfiguration(workspaceName: WorkspaceName,
                             methodConfigurationNamespace: String,
                             methodConfigurationName: String
  ): Future[MethodConfiguration] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
          methodConfig =>
            DBIO.successful(methodConfig)
        }
      }
    }

  def getMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod): Future[MethodInputsOutputs] =
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.getMethodInputsOutputs(userInfo, wdl) match {
          case Failure(exception) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(inputsOutputs)
        }
      }
    }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName,
                                        methodConfigurationNamespace: String,
                                        methodConfigurationName: String
  ): Future[ValidatedMethodConfiguration] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      for {
        methodConfig <- dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
            methodConfig =>
              DBIO.successful(methodConfig)
          }
        }
        vmc <- validateMethodConfiguration(methodConfig, workspaceContext)
      } yield vmc
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map(_.toList)
      }
    }

  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          r.toList.filter(_.methodRepoMethod.repo == Agora)
        }
      }
    }

  def createMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        dataSource
          .inTransaction { dataAccess =>
            dataAccess.methodConfigurationQuery.get(workspaceContext,
                                                    methodConfiguration.namespace,
                                                    methodConfiguration.name
            ) flatMap {
              case Some(_) =>
                DBIO.failed(
                  new RawlsExceptionWithErrorReport(
                    errorReport = ErrorReport(StatusCodes.Conflict,
                                              s"${methodConfiguration.name} already exists in ${workspaceName}"
                    )
                  )
                )
              case None => dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
            }
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, workspaceContext)
          }
      }
    }

  // Overwrite the method configuration at methodConfiguration[namespace|name] with the new method configuration.
  def overwriteMethodConfiguration(workspaceName: WorkspaceName,
                                   methodConfigurationNamespace: String,
                                   methodConfigurationName: String,
                                   methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource
          .inTransaction { dataAccess =>
            if (
              methodConfiguration.namespace != methodConfigurationNamespace || methodConfiguration.name != methodConfigurationName
            ) {
              DBIO.failed(
                new RawlsExceptionWithErrorReport(
                  errorReport = ErrorReport(
                    StatusCodes.BadRequest,
                    s"The method configuration name and namespace in the URI should match the method configuration name and namespace in the request body. If you want to move this method configuration, use POST."
                  )
                )
              )
            } else {
              dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
            }
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, workspaceContext)
          }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String,
                                newName: MethodConfigurationName
  ): Future[MethodConfiguration] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        // It's terrible that we pass unnecessary junk that we don't read in the payload, but a big refactor of the API is going to have to wait until Some Other Time.
        if (newName.workspaceName != workspaceName) {
          DBIO.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, "Workspace name and namespace in payload must match those in the URI")
            )
          )
        } else {
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
            methodConfiguration =>
              // If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
              dataAccess.methodConfigurationQuery.get(workspaceContext, newName.namespace, newName.name) flatMap {
                case Some(_)
                    if methodConfigurationNamespace != newName.namespace || methodConfigurationName != newName.name =>
                  DBIO.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport = ErrorReport(
                        StatusCodes.Conflict,
                        s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."
                      )
                    )
                  )
                case Some(_) => DBIO.successful(methodConfiguration) // renaming self to self: no-op
                case None =>
                  dataAccess.methodConfigurationQuery.update(workspaceContext,
                                                             methodConfigurationNamespace,
                                                             methodConfigurationName,
                                                             methodConfiguration.copy(name = newName.name,
                                                                                      namespace = newName.namespace
                                                             )
                  )
              }
          }
        }
      }
    }

  // Move the method configuration at methodConfiguration[namespace|name] to the location specified in methodConfiguration, _and_ update it.
  // It's like a rename and upsert all rolled into one.
  def updateMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String,
                                methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource
          .inTransaction { dataAccess =>
            withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { _ =>
              dataAccess.methodConfigurationQuery.get(workspaceContext,
                                                      methodConfiguration.namespace,
                                                      methodConfiguration.name
              ) flatMap {
                // If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
                case Some(_)
                    if methodConfigurationNamespace != methodConfiguration.namespace || methodConfigurationName != methodConfiguration.name =>
                  DBIO.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport = ErrorReport(
                        StatusCodes.Conflict,
                        s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."
                      )
                    )
                  )
                case _ =>
                  dataAccess.methodConfigurationQuery.update(workspaceContext,
                                                             methodConfigurationNamespace,
                                                             methodConfigurationName,
                                                             methodConfiguration
                  )
              }
            }
          }
          .flatMap { updatedMethodConfig =>
            validateMethodConfiguration(updatedMethodConfig, workspaceContext)
          }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result =
      getV2WorkspaceContextAndPermissions(mcnp.destination.workspaceName, SamWorkspaceActions.write) flatMap {
        destContext =>
          getV2WorkspaceContextAndPermissions(mcnp.source.workspaceName, SamWorkspaceActions.read) flatMap {
            sourceContext =>
              dataSource.inTransaction { dataAccess =>
                dataAccess.methodConfigurationQuery.get(sourceContext,
                                                        mcnp.source.namespace,
                                                        mcnp.source.name
                ) flatMap {
                  case None =>
                    val err = ErrorReport(
                      StatusCodes.NotFound,
                      s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}."
                    )
                    DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = err))
                  case Some(methodConfig) => DBIO.successful((methodConfig, destContext))
                }
              }
          }
      }

    transaction1Result flatMap { case (methodConfig, destContext) =>
      withAttributeNamespaceCheck(methodConfig) {
        dataSource
          .inTransaction { dataAccess =>
            saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, destContext)
          }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(
    methodRepoQuery: MethodRepoConfigurationImport
  ): Future[ValidatedMethodConfiguration] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace,
                                  methodRepoQuery.methodRepoName,
                                  methodRepoQuery.methodRepoSnapshotId,
                                  ctx.userInfo
    ) flatMap {
      case None =>
        val name =
          s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) =>
        Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
          withAttributeNamespaceCheck(targetMethodConfig) {
            getV2WorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName,
                                                SamWorkspaceActions.write
            ) flatMap { destContext =>
              dataSource
                .inTransaction { dataAccess =>
                  saveCopiedMethodConfiguration(targetMethodConfig,
                                                methodRepoQuery.destination,
                                                destContext,
                                                dataAccess
                  )
                }
                .flatMap { methodConfig =>
                  validateMethodConfiguration(methodConfig, destContext)
                }
            }
          }
        }
    }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[AgoraEntity] =
    getV2WorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, SamWorkspaceActions.read) flatMap {
      workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext,
                           methodRepoQuery.source.namespace,
                           methodRepoQuery.source.name,
                           dataAccess
          ) { methodConfig =>
            DBIO.from(
              methodRepoDAO.postMethodConfig(
                methodRepoQuery.methodRepoNamespace,
                methodRepoQuery.methodRepoName,
                methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace,
                                  name = methodRepoQuery.methodRepoName
                ),
                ctx.userInfo
              )
            )
          }
        }
    }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration,
                                            dest: MethodConfigurationName,
                                            destContext: Workspace,
                                            dataAccess: DataAccess
  ) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Conflict,
              s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}"
            )
          )
        )
      case None => dataAccess.methodConfigurationQuery.create(destContext, target)
    }
  }

  private def parseAgoraEntity(agoraEntity: AgoraEntity): Try[MethodConfiguration] = {
    def convertToMethodConfiguration(agoraMethodConfig: AgoraMethodConfiguration): MethodConfiguration =
      MethodConfiguration(
        agoraMethodConfig.namespace,
        agoraMethodConfig.name,
        Some(agoraMethodConfig.rootEntityType),
        Some(Map.empty[String, AttributeString]),
        agoraMethodConfig.inputs,
        agoraMethodConfig.outputs,
        agoraMethodConfig.methodRepoMethod
      )

    val parsed = Try {
      agoraEntity.payload.map(JsonParser(_).convertTo[AgoraMethodConfiguration])
    } recoverWith { case e: Exception =>
      Failure(
        new RawlsExceptionWithErrorReport(
          errorReport =
            ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))
        )
      )
    }

    parsed flatMap {
      case Some(agoraMC) => Success(convertToMethodConfiguration(agoraMC))
      case None =>
        Failure(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")
          )
        )
    }
  }

  def createMethodConfigurationTemplate(methodRepoMethod: MethodRepoMethod): Future[MethodConfiguration] =
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, ctx.userInfo) { wdl: WDL =>
        methodConfigResolver.toMethodConfiguration(ctx.userInfo, wdl, methodRepoMethod) match {
          case Failure(exception) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(methodConfig)
        }
      }
    }

  def deleteMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String
  ): Future[Boolean] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
          methodConfig =>
            dataAccess.methodConfigurationQuery.delete(workspaceContext,
                                                       methodConfigurationNamespace,
                                                       methodConfigurationName
            )
        }
      }
    }

  // validates the expressions in the method configuration, taking into account optional inputs
  private def validateMethodConfiguration(methodConfiguration: MethodConfiguration,
                                          workspaceContext: Workspace
  ): Future[ValidatedMethodConfiguration] =
    for {
      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfiguration)
      gatherInputsResult <- gatherMethodConfigInputs(methodConfiguration)
      vmc <- entityProvider.expressionValidator.validateMCExpressions(methodConfiguration, gatherInputsResult)
    } yield vmc

  private def gatherMethodConfigInputs(
    methodConfig: MethodConfiguration
  ): Future[MethodConfigResolver.GatherInputsResult] =
    methodRepoDAO
      .getMethod(methodConfig.methodRepoMethod, ctx.userInfo)
      .map {
        case None =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.NotFound,
                                      s"Cannot get ${methodConfig.methodRepoMethod.methodUri} from method repo."
            )
          )
        case Some(wdl) =>
          methodConfigResolver
            .gatherInputs(ctx.userInfo, methodConfig, wdl)
            .recoverWith { case regrets =>
              Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regrets)))
            }
            .get
      }
      .recover { case t: Throwable =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.BadGateway,
            s"Unable to query the method repo.",
            methodRepoDAO.toErrorReport(t)
          )
        )
      }

  private def getEntityProviderForMethodConfig(workspaceContext: Workspace,
                                               methodConfiguration: MethodConfiguration
  ): Future[EntityProvider] =
    entityManager.resolveProviderFuture(
      EntityRequestArguments(workspaceContext, ctx, methodConfiguration.dataReferenceName, None)
    )

}
