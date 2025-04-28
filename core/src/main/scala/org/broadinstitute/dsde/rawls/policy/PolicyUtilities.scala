package org.broadinstitute.dsde.rawls.policy

import bio.terra.policy.model.TpsPaoGetResult
import org.broadinstitute.dsde.rawls.model.TpsModel.TpsPolicies.TpsPolicy
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}

import scala.jdk.CollectionConverters._

object PolicyUtilities {

  def containsProtectedDataPolicy(tpsPaoGetResult: TpsPaoGetResult): Boolean =
      containsPolicy(tpsPaoGetResult, TpsPolicies.ProtectedData)

  def getGroupConstraintGroups(tpsPaoGetResult: TpsPaoGetResult): Set[String] =
        tpsPaoGetResult.getEffectiveAttributes.getInputs.asScala
          .find(p => p.getNamespace == TERRA_POLICY_NAMESPACE && p.getName == TpsPolicies.GroupConstraint.name)
          .map { groupConstraintPolicy =>
            groupConstraintPolicy.getAdditionalData.asScala.map(_.getValue).toSet
          }

      .getOrElse(Set.empty)

  def containsRegionConstraintPolicy(tpsPaoGetResult: TpsPaoGetResult): Boolean = containsPolicy(tpsPaoGetResult, TpsPolicies.RegionConstraint)


  private def containsPolicy(tpsPaoGetResult: TpsPaoGetResult, tpsPolicy: TpsPolicy): Boolean = {
    tpsPaoGetResult.getEffectiveAttributes.getInputs.asScala.exists(_.getName == tpsPolicy.name)
  }
}
