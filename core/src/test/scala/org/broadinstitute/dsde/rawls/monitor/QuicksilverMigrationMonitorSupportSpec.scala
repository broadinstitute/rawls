package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddUpdateAttribute,
  AttributeUpdateOperation,
  EntityUpdateDefinition
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeFormat,
  AttributeName,
  AttributeString,
  AttributeValueList,
  Entity,
  ImportStatuses,
  RawlsRequestContext,
  TypedAttributeListSerializer,
  UserInfo,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.ApiServiceSpec
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class QuicksilverMigrationMonitorSupportSpec()
    extends QuicksilverMigrationMonitorSupport
    with AnyFlatSpecLike
    with Matchers {

  behavior of "compareLists"

  it should "return Correct for exact-same value lists" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Correct
  }

  it should "return StartsWith when it does actually start with" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("baz")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.StartsWith
  }

  it should "return Reordered when it was reordered" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("bar"),
        AttributeString("foo")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Reordered
  }

  it should "return Different with same elements but in different counts" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("foo")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Different
  }

  it should "return Different with different elements" in {
    val current = AttributeValueList(
      Seq(
        AttributeString("cat"),
        AttributeString("dog")
      )
    )
    val correction = AttributeValueList(
      Seq(
        AttributeString("foo"),
        AttributeString("bar")
      )
    )
    val actual = compareLists(current, correction)
    actual shouldBe AttributeCorrectionStatus.Different
  }

}
