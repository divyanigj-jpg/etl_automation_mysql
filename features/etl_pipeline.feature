Feature: ETL data quality and performance checks

  Background:
    Given the ETL pipeline has run

  # 1) Metadata check
  Scenario: Metadata is correct in src_orders
    Then table "src_orders" should have columns "order_id, customer_id, order_date, amount"

  # 2) Count check
  Scenario: Row count is non-zero in all tables
    Then table "src_orders" should have at least 1 rows
    And table "stg_orders" should have at least 1 rows
    And table "fct_orders_summary" should have at least 1 rows

  # 3) Null check
  Scenario: customer_id is not null in src and stg
    Then column "customer_id" in table "src_orders" should not contain nulls
    And column "customer_id" in table "stg_orders" should not contain nulls

  # 4) Duplicate check
  Scenario: order_id has no duplicates in src_orders
    Then table "src_orders" should have no duplicate rows on column "order_id"

  # 5) No negative amount columns check
  Scenario: amount and total_amount have no negative values
    Then column "amount" in table "src_orders" should have no negative values
    And column "amount" in table "stg_orders" should have no negative values
    And column "total_amount" in table "fct_orders_summary" should have no negative values

  # 6) Valid date columns check
  Scenario: order_date columns contain only valid dates
    Then column "order_date" in table "src_orders" should contain only valid dates
    And column "order_date" in table "stg_orders" should contain only valid dates

  # 7) DQ scorecard check
  Scenario: All tables have PASS status in DQ scorecard
    Then DQ scorecard status for table "src_orders" should be "PASS"
    And DQ scorecard status for table "stg_orders" should be "PASS"
    And DQ scorecard status for table "fct_orders_summary" should be "PASS"

  # 8) Performance metrics check
  Scenario: Performance metrics are captured
    Then performance metric "dbt_run" should be recorded
    And performance metric "dbt_test" should be recorded
    And performance metric "ge_src_orders" should be recorded
    And performance metric "ge_stg_orders" should be recorded
    And performance metric "ge_fct_orders" should be recorded
    And performance metric "total_pipeline" should be recorded