package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.MockCromwellSwaggerClient._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, AttributeEntityReferenceList, AttributeName, AttributeNumber, AttributeString, AttributeValueList, AttributeValueRawJson, Entity, MethodConfiguration, WdlSource, Workspace}

import scala.collection.immutable.Map

trait MethodConfigTestSupport {
  this: TestDriverComponent =>

  import driver.api._

  val littleWdl =
    WdlSource(
      """
        |task t1 {
        |  Int int_arg
        |  Int? int_opt
        |  command {
        |    echo ${int_arg}
        |    echo ${int_opt}
        |  }
        |}
        |
        |workflow w1 {
        |  call t1
        |}
      """.stripMargin)

  val arrayWdl =
    WdlSource(
      """
        |task t1 {
        |  Int int_arg
        |  command {
        |    echo ${int_arg}
        |  }
        |}
        |
        |workflow w1 {
        |  Array[Int] int_array
        |  scatter(i in int_array) {
        |    call t1 { input: int_arg = i }
        |  }
        |}
      """.stripMargin)

  val doubleArrayWdl =
    WdlSource(
      """
        |task t1 {
        |  Array[Int] aint_arg
        |  command {
        |    echo ${aint_arg}
        |  }
        |}
        |
        |workflow w1 {
        |  Array[Array[Int]] aint_array
        |  scatter(ai in aint_array) {
        |    call t1 { input: aint_arg = ai }
        |  }
        |}
      """.stripMargin)


  val optionalDoubleArrayWdl =
    WdlSource(
      """
        |task t1 {
        |  Array[Int] aint_arg
        |  command {
        |    echo ${aint_arg}
        |  }
        |}
        |
        |workflow w1 {
        |  Array[Array[Int]]? aint_array
        |  scatter(ai in aint_array) {
        |    call t1 { input: aint_arg = ai }
        |  }
        |}
      """.stripMargin)


  val tripleArrayWdl =
    WdlSource(
      """
        |task t1 {
        |  Array[Array[Int]] aint_arg
        |  command {
        |    echo ${aint_arg}
        |  }
        |}
        |
        |workflow w1 {
        |  Array[Array[Array[Int]]] aaint_array
        |  scatter(ai in aaint_array) {
        |    call t1 { input: aint_arg = ai }
        |  }
        |}
      """.stripMargin)

  val wdlVersionOneWdl =
    WdlSource(
      """
        |version 1.0
        |
        |task use_this_name {
        |
        |  input {
        |    String s
        |    File f
        |  }
        |
        |  command {}
        |
        |  meta {
        |    email: "skroob@spaceballs.gov"
        |    author: "President Skroob"
        |    description: "Spaceballs: The Unit Test"
        |  }
        |
        |  output {
        |    File f2 = "a"
        |  }
        |
        |  runtime {
        |    docker: "docker image"
        |  }
        |}
      """.stripMargin)

  val wdlStructInputWdl =
    WdlSource("""
                |version 1.0
                |
                |struct Participant {
                |  Int id
                |  String sample_name
                |  Array[Int] samples
                |}
                |
                |task do_something {
                |  input {
                |    Int id
                |    String sample_name
                |    Array[Int] samples
                |  }
                |
                |  command {
                |    echo "Hello participant ~{id} ~{sample_name} !"
                |  }
                |
                |  runtime {
                |    docker: "docker image"
                |  }
                |}
                |
                |workflow wdlStructWf {
                |  input {
                |    Participant obj
                |  }
                |
                |  call do_something {input: id = obj.id, sample_name = obj.sample_name, samples = obj.samples}
                |}
    """.stripMargin)

  val wdlStructInputWdlWithNestedStruct =
    WdlSource("""
                |version 1.0
                |
                |struct Foo {
                |  Array[Int] bar
                |}
                |
                |struct Participant {
                |  Int id
                |  String sample_name
                |  Array[Int] samples
                |  Foo nested_struct
                |}
                |
                |task do_something {
                |  input {
                |    Int id
                |    String sample_name
                |  }
                |
                |  command {
                |    echo "Hello participant ~{id} ~{sample_name} !"
                |  }
                |
                |  runtime {
                |    docker: "docker image"
                |  }
                |}
                |
                |workflow wdlStructWf {
                |  input {
                |    Participant obj
                |  }
                |
                |  call do_something {input: id = obj.id, sample_name = obj.sample_name, samples = obj.samples}
                |
                |  output {
                |    Int count1 = length(obj.samples)
                |    Int count2 = length(obj.nested_struct.bar)
                |  }
                |}
    """.stripMargin)

  val badWdl = WdlSource("This is not a valid workflow [MethodConfigResolverSpec]")

  val littleWdlName = "w1"
  val intArgName = "t1.int_arg"
  val intArgNameWithWfName = "w1.t1.int_arg"
  val intOptName = "t1.int_opt"
  val intOptNameWithWfName = "w1.t1.int_opt"
  val intArrayName = "int_array"
  val intArrayNameWithWfName = "w1.int_array"
  val doubleIntArrayName = "aint_array"
  val doubleIntArrayNameWithWfName = "w1.aint_array"
  val tripleIntArrayName = "aaint_array"
  val tripleIntArrayNameWithWfName = "w1.aaint_array"
  val wdlVersionOneWdlName = "use_this_name"
  val wdlVersionOneStringInputName = "s"
  val wdlVersionOneFileInputName = "f"
  val wdlVersionOneFileOutputName = "f2"
  val wdlStructInput = "obj"
  val wdlStructInputWithWfName = "wdlStructWf.obj"

  val littleWdlWorkflowDescriptionRequiredInput = makeToolInputParameter(intArgName, false, makeValueType("Int"), "Int")
  val littleWdlWorkflowDescriptionOptionalInput = makeToolInputParameter(intOptName, true, makeValueType("Int"), "Int?")
  val littleWdlWorkflowDescription = makeWorkflowDescription("w1", List(littleWdlWorkflowDescriptionRequiredInput, littleWdlWorkflowDescriptionOptionalInput), List.empty)

  val requiredArrayInput = makeToolInputParameter(intArrayName, false, makeArrayValueType(makeValueType("Int")), "Array[Int]")
  val requiredArrayWorkflowDescription = makeWorkflowDescription("w1", List(requiredArrayInput), List.empty)

  val requiredDoubleArrayInput =   makeToolInputParameter(doubleIntArrayName, false, makeArrayValueType(makeArrayValueType(makeValueType("Int"))), "Array[Array[Int]]")
  val requiredDoubleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(requiredDoubleArrayInput), List.empty)

  val optionalDoubleArrayInput =   makeToolInputParameter(doubleIntArrayName, true, makeOptionalValueType(makeArrayValueType(makeArrayValueType(makeValueType("Int")))), "Array[Array[Int]]?")
  val optionalDoubleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(optionalDoubleArrayInput), List.empty)


  val requiredTripleArrayInput  = makeToolInputParameter(tripleIntArrayName, true, makeArrayValueType(makeArrayValueType(makeArrayValueType(makeValueType("Int")))), "Array[Array[Array[Int]]]")
  val requiredTripleArrayWorkflowDescription =  makeWorkflowDescription("w1", List(requiredTripleArrayInput), List.empty)

  val badWdlWorkflowDescription = makeBadWorkflowDescription("badwdl", List("ERROR: Finished parsing without consuming all tokens.\n\nThis is not a valid workflow [MethodConfigResolverSpec]\n^\n     "))

  val wdlVersionOneWdlStringInput = makeToolInputParameter(wdlVersionOneStringInputName, false, makeValueType("String"), "String")
  val wdlVersionOneWdlFileInput   = makeToolInputParameter(wdlVersionOneFileInputName, false, makeValueType("File"), "File")
  val wdlVersionOneWdlFileOutput  = makeToolOutputParameter(wdlVersionOneFileOutputName, makeValueType("File"), "File")
  val wdlVersionOneWdlWorkflowDescription = makeWorkflowDescription(wdlVersionOneWdlName, List(wdlVersionOneWdlStringInput, wdlVersionOneWdlFileInput), List(wdlVersionOneWdlFileOutput))

  val wdlStructObjectFields = Map(
    "id" -> makeValueType("Int"),
    "sample_name" -> makeValueType("String"),
    "samples" -> makeArrayValueType(makeValueType("Int"))
  )
  val requiredWdlStructWfInput = makeToolInputParameter(wdlStructInput, false, makeObjectValueType(wdlStructObjectFields), "Participant")
  val wdlStructWfDescription = makeWorkflowDescription("wdlStructWf", List(requiredWdlStructWfInput), List.empty)

  val nestedStructFields = Map(
    "bar" -> makeArrayValueType(makeValueType("Int"))
  )
  val wdlStructWithNestedStructFields = Map(
    "id" -> makeValueType("Int"),
    "sample_name" -> makeValueType("String"),
    "samples" -> makeArrayValueType(makeValueType("Int")),
    "foo" -> makeObjectValueType(nestedStructFields)
  )
  val nestedWdlStructWfInput = makeToolInputParameter(wdlStructInput, false, makeObjectValueType(wdlStructWithNestedStructFields), "Participant")
  val nestedWdlStructWfDescription = makeWorkflowDescription("wdlStructWf", List(nestedWdlStructWfInput), List.empty)

  mockCromwellSwaggerClient.workflowDescriptions += (littleWdl -> littleWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (arrayWdl  -> requiredArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (doubleArrayWdl -> requiredDoubleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (optionalDoubleArrayWdl -> optionalDoubleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (tripleArrayWdl -> requiredTripleArrayWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (badWdl -> badWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (wdlVersionOneWdl -> wdlVersionOneWdlWorkflowDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (wdlStructInputWdl -> wdlStructWfDescription)
  mockCromwellSwaggerClient.workflowDescriptions += (wdlStructInputWdlWithNestedStruct -> nestedWdlStructWfDescription)

  val workspace = Workspace("workspaces", "test_workspace", UUID.randomUUID().toString(), "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

  import spray.json._
  val sampleGood = Entity("sampleGood", "Sample",
    Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(1),
      AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[0,1,2],[3,4,5]]".parseJson)))
  val sampleGood2 = Entity("sampleGood2", "Sample",
    Map(AttributeName.withDefaultNS("blah") -> AttributeNumber(2),
      AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[3,4,5],[6,7,8]]".parseJson)))
  val sampleMissingValue = Entity("sampleMissingValue", "Sample", Map.empty)
  val sampleWithSingleElementArray = Entity("sampleGood3", "Sample", Map(AttributeName.withDefaultNS("blah") -> AttributeValueList(Seq(AttributeNumber(101)))))

  val sampleSet = Entity("daSampleSet", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference,
      sampleMissingValue.toReference)
    ))
  )

  val sampleSet2 = Entity("daSampleSet2", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference,
      sampleGood2.toReference
    )),
      AttributeName.withDefaultNS("rawJsonDoubleArray") -> AttributeValueRawJson( "[[0,1,2],[3,4,5]]".parseJson )
    )
  )

  val sampleSet3 = Entity("daSampleSet3", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleGood.toReference))))

  val sampleForWdlStruct = Entity("sample3", "SampleSet",
    Map(
      AttributeName.withDefaultNS("participant_id") -> AttributeNumber(101),
      AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
        sampleGood.toReference,
        sampleGood2.toReference
      ))
    )
  )

  val sampleSet4 = Entity("daSampleSet4", "SampleSet",
    Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sampleWithSingleElementArray.toReference)
    ))
  )

  val sampleForWdlStruct2 = Entity("daSampleSet5", "SampleSet",
    Map(
      AttributeName.withDefaultNS("participant_id") -> AttributeNumber(123),
      AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
        sampleWithSingleElementArray.toReference
      ))
    )
  )

  val dummyMethod = AgoraMethod("method_namespace", "test_method", 1)

  val configGood = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    None, Map(intArgNameWithWfName -> AttributeString("this.blah")), Map.empty, dummyMethod)

  val configEvenBetter = MethodConfiguration("config_namespace", "configGood", Some("Sample"),
    None, Map(intArgNameWithWfName -> AttributeString("this.blah"), intOptNameWithWfName -> AttributeString("this.blah")),
    Map.empty, dummyMethod)

  val configMissingExpr = MethodConfiguration("config_namespace", "configMissingExpr", Some("Sample"),
    None, Map.empty, Map.empty, dummyMethod)

  val configSampleSet = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayNameWithWfName -> AttributeString("this.samples.blah")), Map.empty, dummyMethod)

  val configEmptyArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(intArrayNameWithWfName -> AttributeString("this.nonexistent")), Map.empty, dummyMethod)

  val configRawJsonDoubleArray = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(doubleIntArrayNameWithWfName -> AttributeString("this.rawJsonDoubleArray")), Map.empty, dummyMethod)

  val configRawJsonTripleArray = MethodConfiguration("config_namespace", "configSample", Some("Sample"),
    None, Map(tripleIntArrayNameWithWfName -> AttributeString("this.samples.rawJsonDoubleArray")), Map.empty, dummyMethod)

  val configArrayWithAttrRef = MethodConfiguration("config_namespace", "configSampleSet", Some("SampleSet"),
    None, Map(doubleIntArrayNameWithWfName -> AttributeString("""[[10,11,12],this.samples.blah]""")), Map.empty, dummyMethod)

  val configWdlStruct = MethodConfiguration("config_namespace", "configWdlStruct", Some("SampleSet"),
    None, Map(wdlStructInputWithWfName -> AttributeString("""{"id":this.participant_id,"sample":"sample1","samples":this.samples.blah}""")), Map.empty, dummyMethod)

  val configNestedWdlStruct = MethodConfiguration("config_namespace", "configNestedWdlStruct", Some("SampleSet"),
    None, Map(wdlStructInputWithWfName -> AttributeString("""{"id":this.participant_id,"sample":"sample1","samples":this.samples.blah,"foo":{"bar":this.samples.blah}}""")), Map.empty, dummyMethod)

  class ConfigData extends TestData {
    override def save() = {
      DBIO.seq(
        workspaceQuery.createOrUpdate(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sampleGood),
            entityQuery.save(context, sampleGood2),
            entityQuery.save(context, sampleMissingValue),
            entityQuery.save(context, sampleWithSingleElementArray),
            entityQuery.save(context, sampleSet),
            entityQuery.save(context, sampleSet2),
            entityQuery.save(context, sampleSet3),
            entityQuery.save(context, sampleSet4),
            entityQuery.save(context, sampleForWdlStruct),
            entityQuery.save(context, sampleForWdlStruct2),
            methodConfigurationQuery.create(context, configGood),
            methodConfigurationQuery.create(context, configMissingExpr),
            methodConfigurationQuery.create(context, configSampleSet),
            methodConfigurationQuery.create(context, configNestedWdlStruct)
          )
        }
      )
    }
  }

  val configData = new ConfigData()

  def withConfigData[T](testCode: => T): T = {
    withCustomTestDatabaseInternal(configData)(testCode)
  }
}
