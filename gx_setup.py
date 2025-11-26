from great_expectations.data_context import BaseDataContext
from great_expectations.datasource.fluent import PandasDatasource

# Create context in-memory (no project folder needed)
context = BaseDataContext()

# Add a datasource (MySQL example later)
print("GX context created successfully!")