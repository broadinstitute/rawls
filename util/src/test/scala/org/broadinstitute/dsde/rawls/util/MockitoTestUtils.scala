package org.broadinstitute.dsde.rawls.util

import org.mockito.ArgumentCaptor
import org.scalatestplus.mockito.MockitoSugar

import scala.reflect.{classTag, ClassTag}

/**
  * Created by rtitle on 7/14/17.
  */
trait MockitoTestUtils extends MockitoSugar {

  // Scala sugar for Mockitor ArgumentCaptor
  def captor[T: ClassTag]: ArgumentCaptor[T] =
    ArgumentCaptor.forClass(classTag[T].runtimeClass).asInstanceOf[ArgumentCaptor[T]]

}
