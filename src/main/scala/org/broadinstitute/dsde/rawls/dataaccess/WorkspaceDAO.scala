package org.broadinstitute.dsde.rawls.dataaccess

import java.io._
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
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

class FileSystemWorkspaceDAO(storageDirectory: File) extends WorkspaceDAO {
  private def storageLocation(namespace: String, name: String) = {
    Paths.get(storageDirectory.getAbsolutePath, namespace, name).toFile
  }

  def save(workspace: Workspace): Unit = {
    val location = storageLocation(workspace.namespace, workspace.name)
    location.getParentFile.mkdirs()
    FileUtils.write(location, workspace.toJson.toString())
  }

  def load(namespace: String, name: String): Workspace = {
    val location = storageLocation(namespace, name)
    if (!location.canRead || !location.isFile) {
      throw new WorkspaceDoesNotExistException(s"${location.getAbsolutePath} does not exist, cannot be read, or is not a file")
    }
    val json = FileUtils.readFileToString(location).parseJson
    WorkspaceFormat.read(json)
  }
}
