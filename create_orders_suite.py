from great_expectations.data_context import DataContext
from great_expectations.core.batch import RuntimeBatchRequest

context = DataContext()

suite_name = "orders_suite"

# 1) Create/overwrite the suite
context.create_expectation_suite(suite_name, overwrite_existing=True)

# 2) Build batch request using the CORRECT data_connector_name
batch_request = RuntimeBatchRequest(
    datasource_name="mysql_src",
    data_connector_name="default_runtime_data_connector",  # <- must match YAML
    data_asset_name="orders_query",  # just a label
    runtime_parameters={"query": "SELECT * FROM src_orders"},
    batch_identifiers={"default_id": "suite_build"},       # <- key matches YAML
)

# 3) Get validator bound to this suite
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

# 4) Add expectations
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("amount", min_value=0)

# 5) Save suite
validator.save_expectation_suite(discard_failed_expectations=False)

print(f"Expectation suite '{suite_name}' created and saved.")
