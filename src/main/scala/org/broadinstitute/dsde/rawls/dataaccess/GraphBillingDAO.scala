package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConversions._

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

  override def listUserProjects(user: RawlsUserRef, txn: RawlsTransaction): Traversable[RawlsBillingProjectName] = txn withGraph { db =>
    userPipeline(db, user).as("vtx").in().loop("vtx", invert(isVertexOfClass(VertexSchema.BillingProject))).toList.map(loadObject[RawlsBillingProject](_).projectName)
  }

}
