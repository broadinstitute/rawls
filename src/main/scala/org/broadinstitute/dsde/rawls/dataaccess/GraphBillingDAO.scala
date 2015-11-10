package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._

class GraphBillingDAO extends BillingDAO with GraphDAO {
  override def saveProject(rawlsProject: RawlsBillingProject, txn: RawlsTransaction) = txn withGraph { db =>
    val vertex = getBillingProjectVertex(db, rawlsProject.projectName).getOrElse(addVertex(db, VertexSchema.BillingProject))
    saveObject[RawlsBillingProject](rawlsProject, vertex, None, db)
    rawlsProject
  }

  override def loadProject(rawlsProjectName: RawlsBillingProjectName, txn: RawlsTransaction) = txn withGraph { db =>
    getBillingProjectVertex(db, rawlsProjectName) map { loadObject[RawlsBillingProject] }
  }

  override def deleteProject(rawlsProject: RawlsBillingProject, txn: RawlsTransaction) = txn withGraph { db =>
    getBillingProjectVertex(db, rawlsProject.projectName) match {
      case Some(v) =>
        removeObject(v, db)
        true
      case None =>
        false
    }
  }
}
