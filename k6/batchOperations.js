/**
 * Generate a batchUpsert payload for the specified entityType and number of entities. 
 */
export const generateBatchUpsert = (entityType, numEntities) => {
  // Create a batch of upsert operations
  const batchUpsert = [];
  for (let i = 0; i < numEntities; i++) {
    const entityName = `entity-${i}`;
    const entityUpdateDefinition = generateUpdateDefinition(entityType, entityName);
    batchUpsert.push(entityUpdateDefinition);
  }
  return batchUpsert;
}

/**
 * Create a new EntityUpdateDefinition using specified parameters.
 */
const generateUpdateDefinition = (entityType, entityName) => {
  const operations = constantOperations
                      .concat(semiConstantOperations())
                      .concat(customizedOperations(entityName));

  return {
    "entityType": entityType,
    "name": entityName,
    "operations": operations
  };
}

/**
 * Generate a few operations whose values depend on the input.
 */
const customizedOperations = (id) => {
  // return operations customized to the input id
  return [{
    "op": "AddUpdateAttribute",
    "attributeName": "bai",
    "addUpdateAttribute": `gs://fake-bucket-name/sample-${id}.bai`
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "bam",
    "addUpdateAttribute": `gs://fake-bucket-name/sample-${id}.bam`
  },
  {
    "op": "AddUpdateAttribute",
    "attributeName": "fastq1",
    "addUpdateAttribute": `gs://fake-bucket-name/${id}_1.fastq`
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "fastq2",
    "addUpdateAttribute": `gs://fake-bucket-name/${id}_2.fastq`
  },
  {
    "op": "AddUpdateAttribute",
    "attributeName": "participant",
    "addUpdateAttribute": `${id}`
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "sample",
    "addUpdateAttribute": `${id}`
  }];
};


/**
 * Generate a few operations which choose randomly from a small set of possible values.
 */
const semiConstantOperations = () => {
  // Randomly select a few values from a small set of possible values
  const activity = ["Indexing", "Alignment", "Variant Calling", "Variant Annotation"][Math.floor(Math.random() * 4)];
  const sampleType = ["Tumor", "Normal"][Math.floor(Math.random() * 2)];
  const age = Math.floor(Math.random() * 100);
  const qcPassed = ["true", "false"][Math.floor(Math.random() * 2)];
  // return operations using the selected values
  return [{
    "op": "AddUpdateAttribute",
    "attributeName": "activity",
    "addUpdateAttribute": activity
  },
  {
    "op": "AddUpdateAttribute",
    "attributeName": "sampleType",
    "addUpdateAttribute": sampleType
  },
  {
    "op": "AddUpdateAttribute",
    "attributeName": "age",
    "addUpdateAttribute": age
  },
{
    "op": "AddUpdateAttribute",
    "attributeName": "qcPassed",
    "addUpdateAttribute": qcPassed
  }];
};

/** Truly constant operations; these never change */
const constantOperations = [
  {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_amb",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta.64.amb"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_ann",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta.64.ann"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_bwt",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta.64.bwt"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_dict",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.dict"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_fasta",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_intervals",
    "addUpdateAttribute": "gs://fake-bucket-name/panel_100_genes.interval_list"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_pac",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta.64.pac"
  }, {
    "op": "AddUpdateAttribute",
    "attributeName": "ref_sa",
    "addUpdateAttribute": "gs://fake-bucket-name/Homo_sapiens_assembly19.fasta.64.sa"
  }
];
