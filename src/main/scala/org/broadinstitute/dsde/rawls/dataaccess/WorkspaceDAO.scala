package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.file.{Files, Path, Paths}

import org.broadinstitute.dsde.rawls.RawlsException
import spray.json._

import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace)
  def load(namespace: String, name: String): Workspace
}

class WorkspaceDoesNotExistException(message: String) extends RawlsException(message)

class FileSystemWorkspaceDAO(storageDirectory: Path) extends WorkspaceDAO {
  private def storageLocation(namespace: String, name: String) = {
    storageDirectory.resolve(Paths.get(namespace, name))
  }

  def save(workspace: Workspace): Unit = {
    val location = storageLocation(workspace.namespace, workspace.name)
    Files.createDirectories(location.getParent)
    Files.write(location, workspace.toJson.prettyPrint.getBytes)
  }

  def load(namespace: String, name: String): Workspace = {
    val location = storageLocation(namespace, name)
    if (!Files.exists(location) || !Files.isReadable(location) || !Files.isRegularFile(location)) {
      throw new WorkspaceDoesNotExistException(s"${location.toString} does not exist, cannot be read, or is not a file")
    }
    val json = new String(Files.readAllBytes(location)).parseJson
    WorkspaceFormat.read(json)
  }
}
