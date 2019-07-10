package org.broadinstitute.dsde.rawls.dataaccess


import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo
import scala.collection.JavaConverters._


class CromwellSwaggerClient(cromwellBasePath: String) extends LazyLogging {


  private def getCromwellWomtoolApi(accessToken: String): WomtoolApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(cromwellBasePath)
    new WomtoolApi(apiClient)
  }


  // CHANGING THIS MOMENTARILY TO TEST A BUG
  def describe(userInfo: UserInfo, wdl: String): WorkflowDescription = {
    val wfdescription = getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl, null, null, null, null)
    val wfdescriptionInputs  = wfdescription.getInputs.asScala.toList.map { input =>
      val rawInput = input.getName
      input.name(wfdescription.getName + "." + rawInput)
    }
    val wfdescriptionOutputs = wfdescription.getOutputs.asScala.toList map { output =>
      val rawOutput = output.getName
      output.name(wfdescription.getName + "." + rawOutput)
    }
    wfdescription.inputs(wfdescriptionInputs.asJava).outputs(wfdescriptionOutputs.asJava)
  }

}