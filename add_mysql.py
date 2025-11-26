from great_expectations.data_context import FileDataContext

context = FileDataContext(context_root_dir="gx")

# Add MySQL datasource
datasource_config = {
    "name": "mysql_src",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "mysql+pymysql://root:Infy@1234@127.0.0.1:3306/etl_demo"
    },
    "data_connectors": {
        "default_runtime_data_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_id"]
        }
    }
}

context.add_datasource(**datasource_config)
print("MySQL datasource added!")
