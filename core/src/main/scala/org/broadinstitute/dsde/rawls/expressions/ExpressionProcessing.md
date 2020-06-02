Expressions are processed in three ways.

Validation

Is this expression in the right format, given the context? e.g.
* an empty string is fine for Output Expressions but not Input Expressions (except Optionals)
* JSON can be used to specify Input Expressions but not Output Expressions
* JSON input with attribute reference expressions is valid. So for example ```{"exampleRef":this.validRef}``` is valid
* Attribute references are valid inside an array input

Parsing

Can this (Validated) expression be parsed into a query path?
* this..is.not.valid because it has empty elements
* foo.bar doesn't have a valid root: must be JSON, this, or workspace
* this.#@$#$ uses illegal characters
* If the input was a JSON or an array that contained attribute references, each attribute reference is checked to 
see if it can be converted into query path or not

Evaluation

Resolve zero or more Attributes or Entities from a (Parsed) expression's query path
* workspace.reference_index -> select this Workspace's Attribute **reference_index**
* this.pairs.tumor -> traverse the starting PairSet, selecting each Pair's Sample referenced by its **tumor** Attribute
* If the input was a JSON (or an array) that contained attribute references, then after successfully validating and parsing each attribute reference 
in the input it is then evaluated. And the evaluated values are substituted back into the input expression. So for example, if the
input expression was ```{"exampleRef":this.bam, "index":this.index}```, expressions ```this.bam and this.index``` are individually
validated and parsed. If both the steps are successful, then the references are evaluated and in the end the evaluated values 
for each expression is replaced in the input. So if in our example, ``this.bam`` evaluated to ``gs://abc`` and ``this.index``
evaluated to ``123``, the input JSON would become ```{"exampleRef":"gs://abc", "index":123}```.
