package org.broadinstitute.dsde.rawls.model

object TpsModel {
  val TERRA_POLICY_NAMESPACE: String = "terra"

  object TpsPolicies {
    sealed trait TpsPolicy {
      val name: String
      val additionalDataKey: String
    }

    case object GroupConstraint extends TpsPolicy {
      override val name: String = "group-constraint"
      override val additionalDataKey: String = "group"
    }

    case object ProtectedData extends TpsPolicy {
      override val name: String = "protected-data"
      override val additionalDataKey: String =
        "" // protected-data policies do not pass additional data and therefore do not need keys
    }

    // Rawls doesn't set up this policy, but it can cause conflicts when combining PAOs. This is here so we can check for its existence and try to avoid conflicts.
    case object RegionConstraint extends TpsPolicy {
      override val name: String = "region-constraint"
      override val additionalDataKey: String = "region-name"
    }
  }
}
