
test-postgres:
	dbt deps --target postgres
	dbt seed --target postgres --full-refresh
	dbt run --target postgres
	dbt test --target postgres

test-redshift:
	dbt deps --target redshift
	dbt seed --target redshift --full-refresh
	dbt run --target redshift
	dbt test --target redshift

test-snowflake:
	dbt deps --target snowflake
	dbt seed --target snowflake --full-refresh
	dbt run --target snowflake --exclude test_unpivot
	dbt test --target snowflake --exclude test_unpivot

test-bigquery:
	dbt deps --target bigquery
	dbt seed --target bigquery --full-refresh
	dbt run --target bigquery
	dbt test --target bigquery

test-all: test-postgres test-redshift test-snowflake test-bigquery
	echo "Completed successfully"
