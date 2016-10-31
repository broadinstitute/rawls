package org.broadinstitute.dsde.rawls.model

trait RawlsEnumeration[T <: RawlsEnumeration[T]] { self: T =>
  def toString: String
  def withName(name:String): T
}
