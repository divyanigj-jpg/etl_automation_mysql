import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

# Load GE context from gx directory
context = gx.get_context()

# Batch request for stg_orders
batch_request = RuntimeBatchRequest(
    datasource_name="mysql_src",                         # from gx.yml
    data_connector_name="default_runtime_data_connector",
    data_asset_name="stg_orders",
    runtime_parameters={"query": "SELECT * FROM stg_orders"},
    batch_identifiers={"default_id": "stg_orders_run"},
)

# Checkpoint for stg_orders
checkpoint = SimpleCheckpoint(
    name="stg_orders_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "stg_orders_suite",
        }
    ],
)

# Run it
result = checkpoint.run()
print("Validation SUCCESS?" , result["success"])