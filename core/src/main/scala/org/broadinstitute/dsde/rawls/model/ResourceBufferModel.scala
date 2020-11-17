package org.broadinstitute.dsde.rawls.model

case class ProjectPoolId(value: String)
case class PoolId(value: String)

// enum for project pools
object ProjectPoolType extends Enumeration {
  type ProjectPoolType = Value
  val Regular, ServicePerimeter = Value
}
