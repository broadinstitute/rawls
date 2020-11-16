package org.broadinstitute.dsde.rawls.model

case class ProjectPoolId(value: String) //todo: convert everything to ProjectPoolId and poolId
case class PoolId(value: String)

// enum for project pools
object ProjectPoolType extends Enumeration {
  type ProjectPoolType = Value
  val Regular, ServicePerimeter = Value
}