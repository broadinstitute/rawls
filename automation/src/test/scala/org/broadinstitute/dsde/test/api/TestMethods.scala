package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.config.Config
import org.broadinstitute.dsde.workbench.fixture.Method
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}

trait TestMethods {
  // METHODS

  val SubWorkflowChildMethod = Method(
    methodName = "child_method_for_subworkflow_test",
    methodNamespace = "subworkflow_methods",
    snapshotId = 1,
    rootEntityType = "participant",
    synopsis = "testtestsynopsis",
    documentation = "",
    payload =
      """
        |workflow turboHC {
        |
        |	File refFasta
        |	File refIndex
        |	File refDict
        |
        |	File inputBam
        |	File inputBamIndex
        |	File intervalsList
        |
        |	String gatk_docker
        |
        |	String output_name = basename(inputBam, ".bam") + ".g.vcf"
        |
        |	Array[String] callingIntervals = read_lines(intervalsList)
        |
        |	scatter(interval in callingIntervals) {
        |		call HaplotypeCaller_GVCF {
        |			input:
        |				refFasta = refFasta,
        |				refIndex = refIndex,
        |				refDict = refDict,
        |
        |				inputBam = inputBam,
        |				inputBamIndex = inputBamIndex,
        |				intervals = interval,
        |				gvcf_name = output_name,
        |				docker_image = gatk_docker
        |		}
        |	}
        |	call MergeGVCFs {
        |		input:
        |			gvcfs = HaplotypeCaller_GVCF.output_gvcf,
        |			gvcf_name = output_name,
        |			docker_image = gatk_docker
        |	}
        |
        |	output {
        |		File outputGVCF = MergeGVCFs.mergedGVCF
        |	}
        |}
        |
        |task HaplotypeCaller_GVCF {
        |
        |	File refFasta
        |	File refIndex
        |	File refDict
        |	File inputBam
        |	File inputBamIndex
        |
        |	String gvcf_name
        |	String intervals
        |
        |	String docker_image
        |
        |	command {
        |		gatk HaplotypeCaller \
        |			-R ${refFasta} \
        |			-I ${inputBam} \
        |			-O ${gvcf_name} \
        |			-ERC GVCF \
        |			-L ${intervals} \
        |			-ip 100
        |	}
        |
        |	output {
        |		File output_gvcf = "${gvcf_name}"
        |	}
        |
        |	runtime {
        |		docker: docker_image
        |	}
        |}
        |
        |task MergeGVCFs {
        |
        |	Array[File] gvcfs
        |	String gvcf_name
        |
        |	String docker_image
        |
        |  command {
        |    gatk MergeVcfs \
        |	    -I ${sep=' -I' gvcfs} \
        |	    -O ${gvcf_name}
        |  }
        |
        |  output {
        |    File mergedGVCF = "${gvcf_name}"
        |  }
        |
        |  runtime {
        |	docker: docker_image
        |  }
        |
        |}
      """.stripMargin)

  def subWorkflowParentMethod(child: Method): Method = {

    // Orchestration in real environments has a globally resolvable name like "firecloud-orchestration.dsde-dev.b.o"
    // but not in FIABs; instead they can use the docker network name

    val orchUrl = if (Config.FireCloud.orchApiUrl.contains("fiab"))
      "http://orch-app:8080/"
    else
      Config.FireCloud.orchApiUrl

    val childUrl = s"${orchUrl}ga4gh/v1/tools/${child.methodNamespace}:${child.methodName}/versions/1/plain-WDL/descriptor"

    Method(
      methodName = "parent_method_for_subworkflow_test",
      methodNamespace = "subworkflow_methods",
      snapshotId = 1,
      rootEntityType = "participant",
      synopsis = "testtestsynopsis",
      documentation = "",
      payload =
        s"""
           |import "$childUrl" as TurboHC_WF
           |
           |workflow scatterHC_samples {
           |
           |	File refFasta
           |	File refIndex
           |	File refDict
           |
           |	String gatk_docker
           |
           |	File intervalsList
           |	File fileOfInputs
           |
           |	Array[Array[File]] input_bams = read_tsv(fileOfInputs)
           |
           |	scatter(bam in input_bams) {
           |
           |		call TurboHC_WF.turboHC {
           |			input:
           |				refFasta = refFasta,
           |				refIndex = refIndex,
           |				refDict = refDict,
           |
           |				inputBam=bam[0],
           |				inputBamIndex=bam[1],
           |				intervalsList = intervalsList,
           |				gatk_docker = gatk_docker
           |		}
           |	}
           |
           |	output {
           |		Array[File] outputGVCF = turboHC.outputGVCF
           |	}
           |}
      """.stripMargin)
  }

  // CONFIGS

  // TODO: make this and wb-libs MethodData configs into a reasonable MethodConfig type

  object SubWorkflowConfig {
    val bucket = GcsBucketName("workbench-test-bucket-do-not-delete")
    val testFolder = "rawls-subworkflow-metadata"

    // Construct a GCS Path, convert to URI, and string-escape
    def objectPathToInputExpression(path: String): String = {
      val uri = GcsPath(bucket, GcsObjectName(s"$testFolder/$path")).toUri
      '"' + uri + '"'
    }

    val configName = "subworkflow_config"
    val configNamespace = "subworkflow_methods"
    val snapshotId = 1
    val rootEntityType = "participant"
    val inputs = Map(
      "scatterHC_samples.gatk_docker" -> "\"us.gcr.io/broad-gatk/gatk\"",

      "scatterHC_samples.fileOfInputs" -> objectPathToInputExpression("inputs_for_bucket.tsv"),
      "scatterHC_samples.intervalsList" -> objectPathToInputExpression("intervals.txt"),
      "scatterHC_samples.refFasta" -> objectPathToInputExpression("ref.fasta"),
      "scatterHC_samples.refIndex" -> objectPathToInputExpression("ref.fasta.fai"),
      "scatterHC_samples.refDict" -> objectPathToInputExpression("ref.dict")
    )

    val outputs = Map("scatterHC_samples.outputGVCF" -> "workspace.result")
  }

  // ENTITIES

  object SingleParticipant {
    val participantEntity = "entity:participant_id\nparticipant1"
    val entityId = "participant1"
  }

}