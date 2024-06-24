package org.broadinstitute.dsde.rawls.metrics

import bio.terra.bard.api.DefaultApi
import bio.terra.bard.client.ApiClient
import bio.terra.bard.model.EventsEventLogRequest
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.broadinstitute.dsde.rawls.metrics.logEvents.BardEvent
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.RestTemplate

class BardService(bardEnabled: Boolean, bardUrl: String, connectionPoolSize: Int) extends LazyLogging {

  private val restTemplate = makeRestTemplateWithPooling
  private val appId = "rawls"

  def sendEvent(event: BardEvent, userInfo: UserInfo): Unit = {
    if (!bardEnabled) {
      logger.debug(s"Bard is disabled, not sending event: ${event.eventName}")
      return
    }
    try {
      val eventLogRequest = new EventsEventLogRequest().properties(event.getProperties)
      val client = getEventApi(restTemplate, userInfo)
      client.eventsEventLog(event.eventName, appId, eventLogRequest)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to send event to Bard: ${e.getMessage}", e)
    }
    ()
  }

  private def getEventApi(restTemplate: RestTemplate, userInfo: UserInfo): DefaultApi = {
    val bardClient = new ApiClient(restTemplate)
    bardClient.setBasePath(bardUrl)
    bardClient.setBearerToken(userInfo.accessToken.token)
    new DefaultApi(bardClient)
  }

  /**
    * @return a new RestTemplate backed by a pooling connection manager
    */
  private def makeRestTemplateWithPooling: RestTemplate = {
    val poolingConnManager = new PoolingHttpClientConnectionManager()
    poolingConnManager.setMaxTotal(connectionPoolSize)
    poolingConnManager.setDefaultMaxPerRoute(connectionPoolSize)
    val httpClient = HttpClients.custom.setConnectionManager(poolingConnManager).build
    val factory = new HttpComponentsClientHttpRequestFactory(httpClient)
    new RestTemplate(factory)
  }

}
