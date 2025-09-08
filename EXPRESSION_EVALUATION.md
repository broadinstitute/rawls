# Submissions and Expression Evaluation in Rawls

This document describes the architecture for submission requests with focus on evaluating expressions in Rawls, 
specifically as implemented in the [`CompactExpressionEvaluator`](core/src/main/scala/org/broadinstitute/dsde/rawls/expressions/CompactExpressionEvaluator.scala), which is used by the `CompactEntityProvider`.  
Legacy expression parsing done through the `LocalEntityProvider` is not discussed.


## Submission Request Pipeline

A submission executes a workflow against one or more entities of the same type. 
Each workflow run corresponds to a single entity and uses that entity's attributes as inputs. 
The submission request specifies either a single entity (via `entityType` and `entityName`) or multiple entities through an entity set expression (via `expression` like "this.pairs" to run on each pair in a pair_set). 
Terra expressions are used to map entity attributes and workspace attributes to workflow inputs, enabling dynamic data binding using syntax like `this.ref_fasta` for entity attributes or `workspace.reference_genome` for workspace-level data.

### Method Configuration

A method configuration defines how to execute a specific workflow by mapping Terra expressions to workflow inputs and specifying execution parameters. 
The method configuration must be specified before submitting a submission request, and must be consistent with the input from the submission request.

Each method configuration:
- references a WDL (Workflow Description Language) workflow
- specifies the root entity type that workflows will execute against
- contains a set of input expressions to map entity attributes and workspace data to workflow parameters

For example, a method configuration might specify a root entity type of "sample", then map the WDL input `myWorkflow.myTask.inputFile` to the Terra expression `this.bam_file`, which tells Terra to use each sample entity's `bam_file` attribute as the input file for that workflow execution. 
When a submission is created, the method configuration is resolved using [`MethodConfigResolver.gatherInputs()`](core/src/main/scala/org/broadinstitute/dsde/rawls/jobexec/MethodConfigResolver.scala#L18), which parses the WDL to identify all required inputs, validates the expression mappings, and categorizes inputs as:
- processable (with valid expressions)
- missing (required but unmapped)
- extra (mapped but not needed by the workflow)

For our purposes, we only care about the processable inputs, which are those used in expression evaluation for a submission.

### Submission Preparation

When a client creates a submission, the [`SubmissionService.prepareSubmission()`](core/src/main/scala/org/broadinstitute/dsde/rawls/submissions/SubmissionsService.scala#L597) method creates a `PreparedSubmission`:
- retrieves the specified method configuration using the `methodConfigurationNamespace` and `methodConfigurationName` from the request
- calls `MethodConfigResolver.gatherInputs()` (as explained above) 
- extracts the Terra expressions from the processable inputs
- **evaluates both entity-specific expressions (like `this.sample_id`) and workspace-level expressions (like `workspace.reference_genome`)** <- This is the step this document focuses on
- validates the consistency between the submission request's entity specification (single entity vs. entity set) and the method configuration's root entity type

### Expression Evaluation Handoff to Execution

Once expression evaluation is complete, the resolved input values are assembled into a comprehensive submission record that contains all the information needed for workflow execution; see [`SubmissionService.saveSubmission()`](core/src/main/scala/org/broadinstitute/dsde/rawls/submissions/SubmissionsService.scala#L800).
Once in the database, the [`SubmissionMonitorActor`](core/src/main/scala/org/broadinstitute/dsde/rawls/jobexec/SubmissionMonitorActor.scala) takes over responsibility for the submission lifecycle, periodically querying for submissions in `Submitted` status and coordinating their execution with external workflow engines like Cromwell.

## Expression Evaluation

The expression evaluation system turns input expressions (like `this.sample.participant.id`) into [`SubmissionValidationEntityInputs`](model/src/main/scala/org/broadinstitute/dsde/rawls/model/ExecutionModel.scala#L272) objects.
Each `SubmissionValidationEntityInputs` contains the entity name and a set of `inputResolutions` - one [`SubmissionValidationValue`](model/src/main/scala/org/broadinstitute/dsde/rawls/model/ExecutionModel.scala#L265) for each input defined in the method configuration. 
For individual entities, each input resolution contains a single `AttributeValue` (like a file path or parameter). 
For entity sets, the input resolution contains an `AttributeValueList` aggregating all the attribute values from the entities in the set. 
For example, if the expression `"this.samples.bam"` is evaluated against a sample set containing three samples, the result would include an `AttributeValueList` containing all three BAM file paths from the individual samples. 

### Expression Evaluation Methods

[`evaluateExpression`](core/src/main/scala/org/broadinstitute/dsde/rawls/expressions/CompactExpressionEvaluator.scala#L70) evaluates a single expression and returns only the attributes, no `SubmissionValidationEntityInputs`. It doesn't distinguish an entity expression from an input expression. This can be used from the API for expression verification, but is not otherwise used in Terra.

[`evaluateExpressions`](core/src/main/scala/org/broadinstitute/dsde/rawls/expressions/CompactExpressionEvaluator.scala#L132) is used in submission processing. It evaluates both the entity expression and all the input expressions from the method config.

### ANTLR Expression Parsing

Terra expressions are parsed using ANTLR4 (ANother Tool for Language Recognition), a parser generator that converts Terra expression strings into structured parse trees. 
ANTLR generates lexer and parser classes from a grammar definition ([`TerraExpression.g4`](core/src/main/antlr4/org/broadinstitute/dsde/rawls/expressions/parser/antlr/TerraExpression.g4) ) that extends standard JSON syntax with Terra-specific lookups like `this.sample.participant.id` and `workspace.reference_genome`.

The parsing process uses the visitor pattern to extract different types of information from the same parse tree. Each visitor implements specific logic for processing expressions:

- **CompactEvaluateVisitor**: Extracts `ExpressionLookup` objects containing database lookup requirements (relation chains, attribute names)
- **ReconstructExpressionVisitor**: Reconstructs expressions by substituting lookup values with actual resolved data
- **Validation Visitors**: Validate expressions without executing them, checking syntax and entity type compatibility

The result of expression parsing using the `CompactEvaluateVisitor` is an [`ExpressionLookup`](core/src/main/scala/org/broadinstitute/dsde/rawls/expressions/parser/antlr/CompactEvaluateVisitor.scala#L14) object:
```scala
case class ExpressionLookup(
     expression: String,
     relations: List[RelationContext],
     attributeName: Option[String], // None for literals
)
```

Represents a single expression's database requirements:
- `expression`: the original expression or literal
- `relations`: list of `RelationContext`s found in the expression
- `attributeName`: the final attribute to get for the expression

A `RelationContext` is a class defined in ANTLR's `TerraExpressionParser.scala`; 
it stores the name of a relation (i.e. the column in which a reference to another entity is stored) 
in the `attributeName()` field.
`RelationContexts` are represented in this document by strings of the `attributeName`.

Examples:
```scala
  ExpressionLookup("this.file", List(), Some("file"))
  ExpressionLookup("this.sample.name", List("sample"), Some("name"))
  ExpressionLookup("this.sample.participant.id", List("sample", "participant"), Some("id"))
  //Note that a single expression can return multiple lookups
  //original expression: "{\"id\": this.bar, \"this.samples\": this.samples.blah}"
  ExpressionLookup("this.bar",List(),Some("bar"))
  ExpressionLookup("this.samples.blah",List("samples"),Some(blah))

```

Each ExpressionLookup corresponds to a single attribute and includes the relation chain needed to reach it from the base entity. The keyword "this" always refers to the root entity type specified in the method configuration. When the input entity type differs from the root entity type (e.g., submitting a sample_set when the root entity type is sample), the entity expression defines how to navigate from the input entity to the root entity type.

### Entity Type and Expression Combinations

The given entity type and entity expression would be defined in a `SubmissionRequest`.  Root entity type and input expression
are pulled from the method config.  End result specifies the shape of the `SubmissionValidationValue`s that `evaluateExpressions` would return.

The following examples assume an entity model where:
- **Sample_set** entities have a `samples` attribute (referencing Sample entities) and a `file` attribute
- **Sample** entities have a `bam` attribute

| given entity type | entity expression | root entity type | input expression | end result                                                                            |
| -- | -- | -- |------------------|---------------------------------------------------------------------------------------|
| sample_set | none | sample_set | this.file        | sample_set_name -> single attribute                                                   |
| sample_set | none | sample_set | this.samples.bam | sample_set_name -> attributeValueList(bam1, bam2...)                                  |
| sample_set | none | sample | this.samples.bam | List((sample_name -> single bam attribute), sample2_name -> sample bam attribute)...) |
| sample_set | none | sample | this.file        | ERROR - sample has no file attribute                                                  |
| sample | none | sample | this.bam         | sample_name -> single bam attribute                                                   |
| sample_set | this.samples | sample_set | -anything-       | ERROR -> entity expression clashes with root entity type                              |
| sample_set | this.samples | sample | this.bam         | List((sample_name -> single bam atribute), sample2_name -> sample bam attribute)...)  |
| sample_set | this.samples | sample | this.samples.bam | ERROR -> sample has no samples attribute                                              |
| sample_set_set | this.sample_set | sample_set | this.samples.bam | Map((sample_set_name -> attributeValueList(bam1, bam2...)), (sample_set2_name -> attributeValueList(bam3, bam4...))) |


### Query Planning Process

A single submission request can have many inputs, but these typically refer to only a few entity types. The expression evaluation system optimizes database access through a three-step process:

1. **Planning**: ExpressionLookups are grouped by their relation chains to minimize database queries. Multiple expressions requiring the same relation level share a single query (e.g., `this.sample.id` and `this.sample.name` both use `["sample"]` relation chain).

2. **Execution**: Each QueryPlan executes [`queryRelatedRecordsWithRelationChain`](core/src/main/scala/org/broadinstitute/dsde/rawls/dataaccess/slick/CompactEntityComponent.scala#L678) to retrieve entity data in batch operations, handling entity type transitions and complex traversals efficiently.

3. **Assembly**: Query results are transformed into `ExpressionAndResult` tuples, then processed by:
   - `constructFinalInputValues`: Takes the expression results and ANTLR parse tree to reconstruct the original input structure, returning `Map[EntityName, Try[Iterable[AttributeValue]]]`
   - `convertToSubmissionValidationValues`: Converts the reconstructed values into `SubmissionValidationValue` objects with proper error handling and type conversion for WDL compatibility

## Example Walkthrough

```scala
//Root entity type: sample
val entityExpression = "this.samples"

//Simplified representation, not the true structure of the objects
val processableInputs = Set(MethodInput("t1.name", "this.name"), MethodInput("t1.id", "this.participant.id"))

//Result of parsing:
val entityLookup = ExpressionLookup("this.samples", List(), Some("samples"))
val inputLookupsName = Seq(ExpressionLookup("t1.name", List(), Some("name")))
val inputLookupsId = Seq(ExpressionLookup("t1.id", List("participant"), Some("id")))

//Result of buildQueryPlan:
//Note that the entity expression is prepended to each relation chain
val queryPlans = List(
  QueryPlan(
    relationChain = List("samples"),
    expressionMappings = Map("this.name" -> Set("name"))
  ),
  QueryPlan(
    relationChain = List("samples", "participant"),
    expressionMappings = Map("this.participant.id" -> Set("id"))
  )
)

//Result of executing queries:
val queryResults = Seq(ExpressionAndResult("this.name", Map("sample1" -> Success(Seq("sample1_name")), "sample2" -> Success(Seq("sample2_name")))),
   ExpressionAndResult("this.participant.id", Map("sample1" -> Success(Seq("p1")), "sample2" -> Success(Seq("p2")))))

//Result of constructing final inputs and converting to SubmissionValidationEntityInputs
val submissionInputs = LazyList(
   SubmissionValidationEntityInputs("sample1", 
      Set(SubmissionValidationValue(Some("sample1_name"), None, "t1.name"), SubmissionValidationValue(Some("p1"), None, "t1.id"))),
   SubmissionValidationEntityInputs("sample2",
      Set(SubmissionValidationValue(Some("sample2_name"), None, "t1.name"), SubmissionValidationValue(Some("p2"), None, "t1.id"))))

//For comparison, if the root entity type had been a sample_set but the submission was otherwise the same, the result would be:
val submissionInputs_set = LazyList(
   SubmissionValidationEntityInputs("sample_set",
      Set(SubmissionValidationValue(Some(AttributeValueList("sample1_name", "sample2_name")), None, "t1.name"), 
         SubmissionValidationValue(Some(AttributeValueList("p1", "p2")), None, "t1.id"))))

```