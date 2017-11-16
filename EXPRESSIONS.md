# Example WDL for CancerExomePipeline_v2

```
task M2 {
  File ref_fasta
  File ref_fasta_dict
  File ref_fasta_fai
  File tumor_bam
  File tumor_bai
  File normal_bam
  File normal_bai
  File intervals
  String m2_output_vcf_name

  command {
    java -jar /task/GenomeAnalysisTK_latest_unstable.jar -T M2 \
    --no_cmdline_in_header -dt NONE -ip 50 \
    -R ${ref_fasta} \
    -I:tumor ${tumor_bam} \
    -I:normal ${normal_bam} \
    -L ${intervals} \
    -o ${m2_output_vcf_name} \
    --disable_auto_index_creation_and_locking_when_reading_rods
  }
  runtime {
    docker: "gcr.io/broad-dsde-dev/cep3"
  }
  output {
    File m2_output_vcf = "${m2_output_vcf_name}"
  }
}

workflow CancerExomePipeline_v2 {
  call M2
}
```
# Example Root Entity

Entity Name: ```HCC1143_pair```

Entity Type: ```pair```

Entity Attributes: ```control_bai```, ```case_bai```, ```output_vcf```, ```ref_pac```, ```ref_ann```, ```case_bam```, ```case_sample```, ```participant```, ```vcf_output_name```, ```ref_fasta```, ```ref_amb```, ```ref_intervals```, ```control_sample```, ```ref_sa```, ```ref_bwt```, ```control_bam```, ```ref_fai```, ```ref_dict```

# Background

An entity can be a sample, participant, pair, or a set of any of the aforementioned types. Entities have properties called "attributes", which may store values such as file paths, primitive types, or references to other entities. Workspaces also have attributes. These attributes can be configured to workflow inputs and outputs using method configurations.

Input and output expressions will tell the method configuration what to associate the WDL variables with. When writing an expression, ```this``` refers to the root entity that is selected when configuring a method. To write an expression that binds to data associated with the root entity, use the ```this.<attribute_name>``` syntax (i.e. to access a reference FASTA associated with the example root entity (shown above), use ```this.ref_fasta```, where ```ref_fasta``` is the attribute name associated with your FASTA file pointer.) Input expressions may also dereference intermediate entities from the root entity. For example, to dereference an attribute called ```ref_fasta``` on the entity ```intermediate_entity```, use the expression  ```this.intermediate_entity.ref_fasta```. Output expressions do not have the ability to dereference intermediate entities. To bind data in the workspace, use the ```workspace.<attribute_name>``` syntax, which functions similarly to the ```this.``` syntax.  Output expressions can be blank, to indicate that the results of the workflow will not be bound.

Workflow expansion expressions can also be associated with submissions, and will define how to expand a workflow on a single entity into multiple workflows that is run on multiple entities. One example of this would be using a workflow expansion expression to run a workflow on each ```pair``` in a ```pair_set```. In this case, the workflow expansion expression would be ```this.pairs```. Workflow expansion expressions work on workspace collection types (```pair_set```, ```participant_set```, and ```sample_set```), as well as user defined reference lists.

# General Syntax
|Input Name|Input Value|
|---|---|
|```<workflow_id>.<task_id>.<input_name>```|```<this|workspace>.<attribute_name>```|

|Output Name|Output Value|
|---|---|
|```<workflow_id>.<task_id>.<output_name>```|```<this|workspace>.<attribute_name>```|


To map the ```ref_fasta``` attribute of ```HCC1143_pair``` to the input ```ref_fasta``` in the ```CancerExomePipeline_v2```, use an Input Name of ```CancerExomePipeline.M2.ref_fasta``` and an Input Value of ```this.ref_fasta```.

The rest of the workflow inputs and outputs follow the same pattern:

|Input Name|Input Value|
|---|---|
|```CancerExomePipeline.M2.ref_fasta_dict```|```this.ref_dict```|
|```CancerExomePipeline.M2.ref_fasta_fai```|```this.ref_fasta```|
|```CancerExomePipeline.M2.tumor_bam```|```this.case_bam```|
|```CancerExomePipeline.M2.tumor_bai```|```this.case_bai```|
|```CancerExomePipeline.M2.normal_bam```|```this.control_bam```|
|```CancerExomePipeline.M2.normal_bai```|```this.control_bai```|
|```CancerExomePipeline.M2.intervals```|```this.ref_intervals```|
|```CancerExomePipeline.M2.m2_output_vcf_name```|```this.vcf_output_name```|

|Output Name|Output Value|
|---|---|
|```CancerExomePipeline.M2.m2_output_vcf```|```this.output_vcf```|