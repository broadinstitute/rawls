package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}

import scala.annotation.implicitNotFound

/**
  * Typeclass for something that can be converted into a metric name fragment.
  * Metric name fragments can be combined via ExpandedMetricBuilder to generate an "expanded" metric name.
  * By default this just calls toString on the object of type A, but this can be overridden.
  */
@implicitNotFound(msg = "Cannot expand instances of type ${A}")
trait Expansion[A] {
  def makeName(a: A): String = a.toString

  final def makeNameWithKey(key: String, a: A) =
    s"$key.${makeName(a)}"
}

object Expansion {

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
    * Implicit expansion for UUID using the default makeName.
    */
  implicit object UUIDExpansion extends Expansion[UUID]

  /**
    * Implicit expansion for RawlsEnumeration using the default makeName.
    * This takes an upper type bound {{{A <: RawlsEnumeration}}} so it can work with any
    * subtype of RawlsEnumeration.
    */
  implicit def RawlsEnumerationExpansion[A <: RawlsEnumeration[_]] = new Expansion[A] {}

  // Implicit expansions for String and Int.
  // It's preferable to use more specific types when possible, but sometimes expanding
  // primitive types into metric names is needed.
  implicit object StringExpansion extends Expansion[String]
  implicit object IntExpansion extends Expansion[Int]
}