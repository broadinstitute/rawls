#
# Note -- this is the source for the materialized view that summarizes Google billing 
# data exported to BigQuery.  Materialized views need to be created in the same dataset
# as their source table, so for Broad is `broad-gcp-billing.gcp_billing_export`.  Because
# that dataset for all of Broad, it is controlled by BITS who needs to create or modify 
# this materialized view on our behalf.  Lukas Karlsson did this for us originally
#
CREATE MATERIALIZED VIEW `broad-gcp-billing.gcp_billing_export.terra_cromwell_workflow_costs` 
PARTITION BY billing_date
CLUSTER BY submission_id, workflow_id
OPTIONS( enable_refresh=true,
         refresh_interval_minutes=20
)
AS
SELECT _PARTITIONDATE as billing_date, 
        project.id as project_id, 
        REPLACE(CASE 'terra-submission-id' 
            WHEN labels[SAFE_OFFSET(0)].key THEN labels[SAFE_OFFSET(0)].value
            WHEN labels[SAFE_OFFSET(1)].key THEN labels[SAFE_OFFSET(1)].value
            WHEN labels[SAFE_OFFSET(2)].key THEN labels[SAFE_OFFSET(2)].value
            WHEN labels[SAFE_OFFSET(3)].key THEN labels[SAFE_OFFSET(3)].value
            WHEN labels[SAFE_OFFSET(4)].key THEN labels[SAFE_OFFSET(4)].value
            WHEN labels[SAFE_OFFSET(5)].key THEN labels[SAFE_OFFSET(5)].value
            WHEN labels[SAFE_OFFSET(6)].key THEN labels[SAFE_OFFSET(6)].value
            WHEN labels[SAFE_OFFSET(7)].key THEN labels[SAFE_OFFSET(7)].value
            WHEN labels[SAFE_OFFSET(8)].key THEN labels[SAFE_OFFSET(8)].value
            WHEN labels[SAFE_OFFSET(9)].key THEN labels[SAFE_OFFSET(9)].value
            WHEN labels[SAFE_OFFSET(10)].key THEN labels[SAFE_OFFSET(10)].value
            WHEN labels[SAFE_OFFSET(11)].key THEN labels[SAFE_OFFSET(11)].value
            WHEN labels[SAFE_OFFSET(12)].key THEN labels[SAFE_OFFSET(12)].value
            WHEN labels[SAFE_OFFSET(13)].key THEN labels[SAFE_OFFSET(13)].value
            WHEN labels[SAFE_OFFSET(14)].key THEN labels[SAFE_OFFSET(14)].value
            WHEN labels[SAFE_OFFSET(15)].key THEN labels[SAFE_OFFSET(15)].value
            WHEN labels[SAFE_OFFSET(16)].key THEN labels[SAFE_OFFSET(16)].value
            WHEN labels[SAFE_OFFSET(17)].key THEN labels[SAFE_OFFSET(17)].value
            WHEN labels[SAFE_OFFSET(18)].key THEN labels[SAFE_OFFSET(18)].value
            WHEN labels[SAFE_OFFSET(19)].key THEN labels[SAFE_OFFSET(19)].value
            WHEN labels[SAFE_OFFSET(20)].key THEN labels[SAFE_OFFSET(20)].value
            ELSE 'Unknown!'
        END, "terra-", "") as submission_id, 
        REPLACE(CASE 'cromwell-workflow-id' 
            WHEN labels[SAFE_OFFSET(0)].key THEN labels[SAFE_OFFSET(0)].value
            WHEN labels[SAFE_OFFSET(1)].key THEN labels[SAFE_OFFSET(1)].value
            WHEN labels[SAFE_OFFSET(2)].key THEN labels[SAFE_OFFSET(2)].value
            WHEN labels[SAFE_OFFSET(3)].key THEN labels[SAFE_OFFSET(3)].value
            WHEN labels[SAFE_OFFSET(4)].key THEN labels[SAFE_OFFSET(4)].value
            WHEN labels[SAFE_OFFSET(5)].key THEN labels[SAFE_OFFSET(5)].value
            WHEN labels[SAFE_OFFSET(6)].key THEN labels[SAFE_OFFSET(6)].value
            WHEN labels[SAFE_OFFSET(7)].key THEN labels[SAFE_OFFSET(7)].value
            WHEN labels[SAFE_OFFSET(8)].key THEN labels[SAFE_OFFSET(8)].value
            WHEN labels[SAFE_OFFSET(9)].key THEN labels[SAFE_OFFSET(9)].value
            WHEN labels[SAFE_OFFSET(10)].key THEN labels[SAFE_OFFSET(10)].value
            WHEN labels[SAFE_OFFSET(11)].key THEN labels[SAFE_OFFSET(11)].value
            WHEN labels[SAFE_OFFSET(12)].key THEN labels[SAFE_OFFSET(12)].value
            WHEN labels[SAFE_OFFSET(13)].key THEN labels[SAFE_OFFSET(13)].value
            WHEN labels[SAFE_OFFSET(14)].key THEN labels[SAFE_OFFSET(14)].value
            WHEN labels[SAFE_OFFSET(15)].key THEN labels[SAFE_OFFSET(15)].value
            WHEN labels[SAFE_OFFSET(16)].key THEN labels[SAFE_OFFSET(16)].value
            WHEN labels[SAFE_OFFSET(17)].key THEN labels[SAFE_OFFSET(17)].value
            WHEN labels[SAFE_OFFSET(18)].key THEN labels[SAFE_OFFSET(18)].value
            WHEN labels[SAFE_OFFSET(19)].key THEN labels[SAFE_OFFSET(19)].value
            WHEN labels[SAFE_OFFSET(20)].key THEN labels[SAFE_OFFSET(20)].value
            ELSE 'Unknown!'
        END, "cromwell-", "")  as workflow_id,
        sum(billing.cost) as cost
FROM `broad-gcp-billing.gcp_billing_export.gcp_billing_export_v1_001AC2_2B914D_822931` as billing
WHERE "cromwell-workflow-id" IN (labels[SAFE_OFFSET(0)].key, 
                               labels[SAFE_OFFSET(1)].key,
                               labels[SAFE_OFFSET(2)].key,
                               labels[SAFE_OFFSET(3)].key,
                               labels[SAFE_OFFSET(4)].key,
                               labels[SAFE_OFFSET(5)].key,
                               labels[SAFE_OFFSET(6)].key,
                               labels[SAFE_OFFSET(7)].key,
                               labels[SAFE_OFFSET(8)].key,
                               labels[SAFE_OFFSET(9)].key,
                               labels[SAFE_OFFSET(10)].key,
                               labels[SAFE_OFFSET(11)].key,
                               labels[SAFE_OFFSET(12)].key,
                               labels[SAFE_OFFSET(13)].key,
                               labels[SAFE_OFFSET(14)].key,
                               labels[SAFE_OFFSET(15)].key,
                               labels[SAFE_OFFSET(16)].key,
                               labels[SAFE_OFFSET(17)].key,
                               labels[SAFE_OFFSET(18)].key,
                               labels[SAFE_OFFSET(19)].key,
                               labels[SAFE_OFFSET(20)].key
                               )
AND "terra-submission-id" IN (labels[SAFE_OFFSET(0)].key, 
                               labels[SAFE_OFFSET(1)].key,
                               labels[SAFE_OFFSET(2)].key,
                               labels[SAFE_OFFSET(3)].key,
                               labels[SAFE_OFFSET(4)].key,
                               labels[SAFE_OFFSET(5)].key,
                               labels[SAFE_OFFSET(6)].key,
                               labels[SAFE_OFFSET(7)].key,
                               labels[SAFE_OFFSET(8)].key,
                               labels[SAFE_OFFSET(9)].key,
                               labels[SAFE_OFFSET(10)].key,
                               labels[SAFE_OFFSET(11)].key,
                               labels[SAFE_OFFSET(12)].key,
                               labels[SAFE_OFFSET(13)].key,
                               labels[SAFE_OFFSET(14)].key,
                               labels[SAFE_OFFSET(15)].key,
                               labels[SAFE_OFFSET(16)].key,
                               labels[SAFE_OFFSET(17)].key,
                               labels[SAFE_OFFSET(18)].key,
                               labels[SAFE_OFFSET(19)].key,
                               labels[SAFE_OFFSET(20)].key
                               )
GROUP BY 1,2,3,4