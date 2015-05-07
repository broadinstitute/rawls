package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import spray.json.JsonParser.ParsingException
import spray.json._

import org.broadinstitute.dsde.rawls.model.{WorkspaceShort, Workspace}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import scala.collection.mutable

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace)
  def load(namespace: String, name: String): Option[Workspace]
  def list(): Seq[WorkspaceShort]
  def loadShort(namespace: String, name: String): Option[WorkspaceShort]
}

class FileSystemWorkspaceDAO(storageDirectory: Path) extends WorkspaceDAO {
  private def storageLocation(namespace: String, name: String) = {
    storageDirectory.resolve(Paths.get(namespace, name))
  }

  def save(workspace: Workspace): Unit = {
    val location = storageLocation(workspace.namespace, workspace.name)
    Files.createDirectories(location.getParent)
    Files.write(location, workspace.toJson.prettyPrint.getBytes)
  }

  def load(namespace: String, name: String): Option[Workspace] = {
    val location = storageLocation(namespace, name)
    if (!Files.exists(location) || !Files.isReadable(location) || !Files.isRegularFile(location)) {
      None
    } else {
      val json = new String(Files.readAllBytes(location)).parseJson
      Option( WorkspaceFormat.read(json) )
    }
  }

  def loadShort(namespace: String, name: String): Option[WorkspaceShort] = {
    load(namespace, name).map(workspace => WorkspaceShort(workspace.namespace, workspace.name, workspace.createdDate, workspace.createdBy))
  }

  def list(): Seq[WorkspaceShort] = {
    val visitor = new WorkspaceFileVisitor(storageDirectory)
    Files.walkFileTree(storageDirectory, visitor)
    visitor.workspaces.result
  }
}

private class WorkspaceFileVisitor(root: Path) extends SimpleFileVisitor[Path] with LazyLogging {
  val workspaces = mutable.ArraySeq.newBuilder[WorkspaceShort]
  override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
    if (attrs.isRegularFile) {
      try {
        val json = new String(Files.readAllBytes(file)).parseJson
        workspaces += WorkspaceShortFormat.read(json)
      } catch {
        case t: ParsingException => logger.info(s"could not parse workspace file $file", t)
      }
    }
    FileVisitResult.CONTINUE
  }
}
