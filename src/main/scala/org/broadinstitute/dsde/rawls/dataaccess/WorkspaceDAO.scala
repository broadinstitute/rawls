package org.broadinstitute.dsde.rawls.dataaccess

import java.io._

import org.apache.commons.io.FileUtils
import spray.json._

import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

/**
 * Created by dvoet on 4/24/15.
 */
class WorkspaceDAO(storageDirectory: File) {
  private def storageLocation(name: String) = new File(storageDirectory, name)

  def save(workspace: Workspace): Unit = {
    FileUtils.write(storageLocation(workspace.name), workspace.toJson.toString())
  }

  def load(name: String): Workspace = {
    val json = FileUtils.readFileToString(storageLocation(name)).parseJson
    WorkspaceFormat.read(json)
  }
}
