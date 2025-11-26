import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

# Load GE context
context = gx.get_context()

# Batch request for final table
batch_request = RuntimeBatchRequest(
    datasource_name="mysql_src",
    data_connector_name="default_runtime_data_connector",
    data_asset_name="fct_orders_summary",
    runtime_parameters={"query": "SELECT * FROM fct_orders_summary"},
    batch_identifiers={"default_id": "fct_orders_summary_run"},
)

# Checkpoint
checkpoint = SimpleCheckpoint(
    name="fct_orders_summary_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "fct_orders_summary_suite",
        }
    ],
)

# Run validation
result = checkpoint.run()
print("Validation SUCCESS?" , result["success"])