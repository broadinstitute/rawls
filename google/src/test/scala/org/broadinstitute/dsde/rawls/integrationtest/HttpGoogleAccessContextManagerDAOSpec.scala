package org.broadinstitute.dsde.rawls.integrationtest

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.google.HttpGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName
import org.broadinstitute.dsde.rawls.util.{MockitoTestUtils, Retry}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

class HttpGoogleAccessContextManagerDAOSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with Retry
    with LazyLogging
    with Eventually
    with MockitoTestUtils
    with StatsDTestUtils
    with ScalaFutures {
  implicit val system = ActorSystem("HttpGoogleAccessContextManagerDAOSpec")

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(180, Seconds)))
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

    val organizationId = "organizations/400176686919"

    val servicePerimeterName =
      ServicePerimeterName("accessPolicies/228353087260/servicePerimeters/terra_dev_aou_test_service_perimeter")
    val billingProjectNumber = "624692839739"

    val additionResponse =
      gacmDAO.overwriteProjectsInServicePerimeter(servicePerimeterName, Set(billingProjectNumber)).futureValue

    println("OPERATION ID " + additionResponse)

  }
}
