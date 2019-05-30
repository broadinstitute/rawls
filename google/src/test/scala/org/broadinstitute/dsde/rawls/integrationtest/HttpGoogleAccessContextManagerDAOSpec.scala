package org.broadinstitute.dsde.rawls.integrationtest

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.google.{HttpGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, ServicePerimeterName}
import org.broadinstitute.dsde.rawls.util.{MockitoTestUtils, Retry}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Minutes, Seconds, Span}

class HttpGoogleAccessContextManagerDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Retry with LazyLogging with Eventually with MockitoTestUtils with StatsDTestUtils with ScalaFutures {
  implicit val system = ActorSystem("HttpGoogleAccessContextManagerDAOSpec")

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(180, Seconds)))
 // val etcConf = ConfigFactory.load()
 // val jenkinsConf = ConfigFactory.parseFile(new File("jenkins.conf"))
 // val gcsConfig = jenkinsConf.withFallback(etcConf).getConfig("gcs")

  import scala.concurrent.ExecutionContext.Implicits.global
  val gacmDAO = new HttpGoogleAccessContextManagerDAO(
    "",
    "",
    "firecloud:rawls",
    "broad-dsde-dev",
    workbenchMetricBaseName
  )

  "HttpGoogleAccessContextManagerDAOSpec" should "add a billing project to a service perimeter" in {

    val organizationId = ""
    val accessPolicies = gacmDAO.listAccessPolicies(organizationId).futureValue

    println("ACCESSPOLICIES " + accessPolicies)
    val accessPolicy = accessPolicies.head


    val servicePerimeterName = ServicePerimeterName("")
    val billingProjectNumber = ""

    val additionResponse = gacmDAO.addProjectToServicePerimeter(accessPolicy, servicePerimeterName, billingProjectNumber).futureValue

    println("OPERATION ID " + additionResponse)

  }
}
