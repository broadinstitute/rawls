package org.broadinstitute.dsde.rawls.model

import com.wordnik.swagger.annotations.{ApiModelProperty, ApiModel}

import scala.annotation.meta.field

@ApiModel(value = "VaultEntity")
case class VaultEntity(
  @(ApiModelProperty@field)(required = true, value = "The id of this Vault object")
  id: String,
  @(ApiModelProperty@field)(required = true, value = "The URL to this Vault object")
  getURL: String,
  @(ApiModelProperty@field)(required = true, value = "The type of this Vault object")
  entityType: String,
  @(ApiModelProperty@field)(required = true, value = "This Vault object's metadata")
  attrs: Map[String, Any]
)
