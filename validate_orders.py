from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest

context = FileDataContext(context_root_dir="gx")

batch_request = RuntimeBatchRequest(
    datasource_name="mysql_src",
    data_connector_name="default_runtime_data_connector",
    data_asset_name="orders",
    runtime_parameters={"query": "SELECT * FROM src_orders"},
    batch_identifiers={"default_id": "test"}
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="orders_suite"
)

validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("amount", min_value=0)

results = validator.validate()
print(results)
