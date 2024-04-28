import great_expectations as ge

url: str = f"postgresql://postgres:password@localhost:5432/"

_context: ge.DataContext = ge.get_context()

# _context.sources.add_postgres(name='postgresql', connection_string=str(url))

_datasource = _context.sources.add_or_update_sql(name="my_gx", connection_string=url)

_table_asset = _datasource.add_table_asset(name="my_gx_asset", table_name="films")

_context.add_or_update_expectation_suite(expectation_suite_name="my_expectation_suite_name")

_br = _table_asset.build_batch_request()

# _batches = _datasource.get_batch_list_from_batch_request(_br)

# play with expectations
_validator = _context.get_validator(
    datasource_name="my_gx"
    , data_asset_name="my_gx_asset"
    , batch_request=_br
    , expectation_suite_name="my_expectation_suite_name"
)
print(_validator.head())

_validator.expect_column_values_to_not_be_null("code")
_validator.save_expectation_suite()
_checkpoint = _context.add_or_update_checkpoint(name="my_checkpoint"
                                                , validations=[
        {
            "batch_request": _br,
            "expectation_suite_name": _validator.expectation_suite_name
        }])

_checkpoint_result = _checkpoint.run()
_context.build_data_docs()