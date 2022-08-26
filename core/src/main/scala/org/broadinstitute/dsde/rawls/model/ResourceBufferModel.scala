package org.broadinstitute.dsde.rawls.model

case class ProjectPoolId(value: String)

// enum for project pools
object ProjectPoolType extends Enumeration {
  type ProjectPoolType = Value
  val Regular, ExfiltrationControlled = Value
}
