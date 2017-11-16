Expressions are processed in three ways.

Validation

Is this expression in the right format, given the context? e.g.
* an empty string is fine for Output Expressions but not Input Expressions (except Optionals)
* JSON can be used to specify Input Expressions but not Output Expressions

Parsing

Can this (Validated) expression be parsed into a query path?
* this..is.not.valid because it has empty elements
* foo.bar doesn't have a valid root: must be JSON, this, or workspace
* this.#@$#$ uses illegal characters

Evaluation

Resolve zero or more Attributes or Entities from a (Parsed) expression's query path
* workspace.reference_index -> select this Workspace's Attribute **reference_index**
* this.pairs.tumor -> traverse the starting PairSet, selecting each Pair's Sample referenced by its **tumor** Attribute
