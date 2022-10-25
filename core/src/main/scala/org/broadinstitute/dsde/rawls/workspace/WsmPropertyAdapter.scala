package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.Property
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, AttributeUpdateOperation, RemoveAttribute}
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, ErrorReport, RawlsRequestContext, Workspace, WorkspaceType}

import scala.jdk.CollectionConverters._

class WsmPropertyAdapter extends LazyLogging {

  val WSM_PROP_NAMESPACE_DELIMITER = "_DELIM_"

  def wsmPropertiesToAttributes(props: java.util.List[Property]): AttributeMap =
    props.asScala
      .filter { prop =>
        prop.getKey.split(WSM_PROP_NAMESPACE_DELIMITER).length == 2
      }
      .map { prop =>
        val tokens = prop.getKey.split(WSM_PROP_NAMESPACE_DELIMITER)
        AttributeName(tokens(0), tokens(1)) -> AttributeString(prop.getValue)
      }
      .toMap

  def rawlsAttributeUpdateOperationToWsmProperty(op: AttributeUpdateOperation): Option[Property] =
    op match {
      case AddUpdateAttribute(attributeName, attr) =>
        attr match {
          case AttributeString(v) =>
            Some(new Property().key(attributeName.namespace + WSM_PROP_NAMESPACE_DELIMITER + attributeName.name).value(v))
          case a =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError, s"unsupported attribute type ${a.getClass.getSimpleName}")
            )
        }
      case _ =>
        logger.warn(    s"unsupported attribute operation type ${op.getClass.getSimpleName}")
        None // should probably throw an exception
    }

  def getAttributesFromWorkspaceManager(workspace: Workspace,
                                        ctx: RawlsRequestContext,
                                        wsmDao: WorkspaceManagerDAO
  ): Option[AttributeMap] =
    workspace.workspaceType match {
      case WorkspaceType.RawlsWorkspace => None
      case WorkspaceType.McWorkspace =>
        val wsmRecord = wsmDao.getWorkspace(workspace.workspaceIdAsUUID, ctx)
        Some(
          wsmPropertiesToAttributes(wsmRecord.getProperties)
        )
    }
}
