package org.broadinstitute.dsde.rawls.monitor.workspace

import org.broadinstitute.dsde.rawls.monitor.WorkspaceResourceMonitor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

class WorkspaceResourceMonitorSpec extends AnyFlatSpec {

  import org.mockito.MockedStatic
  import org.mockito.Mockito

  behavior of "static methods"

  it should "work" in {
    val m = mock[WorkspaceResourceMonitor]
    m.companion.pureFunction()
//WorkspaceResourceMonitor.p
    try {

      val monitorCompanion = Mockito.mockStatic(classOf[WorkspaceResourceMonitor]) //WorkspaceResourceMonitor.getClass.c)
     //monitorCompanion.p
      //monitorCompanion.com
    }
  }

  /*
  @Test  def lookMomICanMockStaticMethods(): Unit =  { assertThat(Buddy.name).isEqualTo("John")
    try {
      val theMock: MockedStatic[Nothing] = Mockito.mockStatic(classOf[Nothing])
      try {theMock.when(Buddy.name).thenReturn("Rafael")
        assertThat(Buddy.name).isEqualTo("Rafael")} finally {
        if (theMock != null) theMock.close()
      }
    }
    assertThat(Buddy.name).isEqualTo("John")
  }
*/

  /*
  import org.mockito.MockedStatic
import org.mockito.Mockito
import java.util
try {
val utilities: MockedStatic[Nothing] = Mockito.mockStatic(classOf[Nothing])
try {utilities.when(() => StaticUtils.range(2, 6)).thenReturn(util.Arrays.asList(10, 11, 12))
assertThat(StaticUtils.range(2, 6)).containsExactly(10, 11, 12)} finally {
if (utilities != null) utilities.close()
}
}

import org.mockito.MockedStatic
import org.mockito.Mockito
@Test  def lookMomICanMockStaticMethods(): Unit =  { assertThat(Buddy.name).isEqualTo("John")
try {
val theMock: MockedStatic[Nothing] = Mockito.mockStatic(classOf[Nothing])
try {theMock.when(Buddy.name).thenReturn("Rafael")
assertThat(Buddy.name).isEqualTo("Rafael")} finally {
if (theMock != null) theMock.close()
}
}
assertThat(Buddy.name).isEqualTo("John")
}

   */
}
