package org.broadinstitute.dsde.rawls.metrics

import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}

/**
  * Created by rtitle on 7/25/17.
  */
object RawlsExpansion {

  // Typeclass instances:

  /**
    * Implicit expansion for WorkspaceName.
    * Statsd doesn't allow slashes in metric names, so we override makeName to override
    * the default toString based implementation.
    */
  implicit object WorkspaceNameExpansion extends Expansion[WorkspaceName] {
    override def makeName(n: WorkspaceName): String = n.toString.replace('/', '.')
  }

  /**
    * Implicit expansion for RawlsEnumeration using the default makeName.
    * This takes an upper type bound {{{A <: RawlsEnumeration}}} so it can work with any
    * subtype of RawlsEnumeration.
    */
  implicit def RawlsEnumerationExpansion[A <: RawlsEnumeration[_]] = new Expansion[A] {}
}
