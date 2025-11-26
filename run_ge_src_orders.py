import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest

# 1. Load GE context (it will use venv/gx automatically)
context = gx.get_context()

# 2. Define a RuntimeBatchRequest: tells GE how to fetch data
batch_request = RuntimeBatchRequest(
    datasource_name="mysql_src",                         # from gx.yml
    data_connector_name="default_runtime_data_connector",  # from gx.yml
    data_asset_name="src_orders",                        # just a label
    runtime_parameters={"query": "SELECT * FROM src_orders"},
    batch_identifiers={"default_id": "src_orders_run"},  # required by connector
)

# 3. Define a SimpleCheckpoint using that BatchRequest
checkpoint = SimpleCheckpoint(
    name="src_orders_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "src_orders_suite",
        }
    ],
)

# 4. Run the checkpoint
result = checkpoint.run()
print("Checkpoint finished. Success:", result['success'])