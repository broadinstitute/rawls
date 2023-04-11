package org.broadinstitute.dsde.rawls.model

import bio.terra.profile.model.{CloudPlatform => BPMCloudPlatform, ProfileModel}
import bio.terra.workspace.model.{CloudPlatform => WSMCloudPlatform}
import bio.terra.datarepo.model.{CloudPlatform => DRCloudPlatform}

object CloudPlatform extends Enumeration {
  type CloudPlatform = Value
  val GCP: CloudPlatform = Value("GCP")
  val AZURE: CloudPlatform = Value("AZURE")
  val UNKNOWN: CloudPlatform = Value("UNKNOWN")

  def apply(platform: BPMCloudPlatform): CloudPlatform = platform match {
    case BPMCloudPlatform.GCP   => GCP
    case BPMCloudPlatform.AZURE => AZURE
  }

  def apply(platform: WSMCloudPlatform): CloudPlatform = platform match {
    case WSMCloudPlatform.GCP   => GCP
    case WSMCloudPlatform.AZURE => AZURE
    case WSMCloudPlatform.AWS   => UNKNOWN // AWS exists only in WorkspaceManager at the moment
  }

  def apply(platform: DRCloudPlatform): CloudPlatform = platform match {
    case DRCloudPlatform.GCP   => GCP
    case DRCloudPlatform.AZURE => AZURE
  }

  def apply(profile: ProfileModel): CloudPlatform = profile.getCloudPlatform match {
    case BPMCloudPlatform.GCP   => GCP
    case BPMCloudPlatform.AZURE => AZURE
    case _ => UNKNOWN // BPM requires cloud platform to be set, so we shouldn't need to rely on the app coordinates
  }

}
