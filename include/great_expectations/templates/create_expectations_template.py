import json
import os
import sys
import great_expectations as ge

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(path)
import ge_prod.ge_data_access as gda
from ge_prod.queries import queries


def generate_suite_and_batch_data(table_name: str, suite_name=None, rule_query_type='create_expectations_2019Q4'):
	rule_logger = gda.get_logger()
	
	rule_logger.info('creating expectations...')
	
	# Getting DataContext object
	context = ge.data_context.DataContext(context_root_dir=path)
	
	data_asset_name = table_name
	normalized_data_asset_name = context.normalize_data_asset_name(data_asset_name)
	rule_logger.info(f"expectation suite keys: {normalized_data_asset_name}")
	
	# creating expectation_suite if 'warnings' not available otherwise loading batch to create expectations on
	for key in context.list_expectation_suite_keys():
		data_assets = os.path.basename(str(key.data_asset_name))
		if (data_asset_name in data_assets) and (suite_name is None):
			context.create_expectation_suite(data_asset_name, expectation_suite_name='default', overwrite_existing=True)
		else:
			context.create_expectation_suite(data_asset_name, expectation_suite_name=suite_name,
			                                 overwrite_existing=True)
	
	print(context.list_expectation_suite_keys())
	rule_logger.info(f"expectation suites: {context.list_expectation_suite_keys()}")
	
	# TODO: Write conditions to handle rule_query_type argument passing
	# Getting batch df to create expectations against
	rule_query = queries.get(table_name).get(rule_query_type)
	df = gda.snowflake_connector_to_df(rule_query)
	
	# generating data asset batch data from great expectations
	b_kwargs = {'dataset': df}
	b_df = context.get_batch(normalized_data_asset_name, expectation_suite_name=suite_name, batch_kwargs=b_kwargs)
	print('row count of batch: ', b_df.get_row_count())
	
	return b_df


def generate_expectations(table_name: str, batch: ge.dataset.Dataset):
	# getting batchId & fingerprint
	rule_batch_fingerprint = batch.batch_fingerprint
	rule_batch_id = batch.batch_id
	print('rule_batch_fingerprint: ', '\n', rule_batch_fingerprint)
	print('rule_batch_id: ', '\n', rule_batch_id)
	
	# Validating to see if columns exists
	column_names = batch.get_table_columns()
	# master column names identified from initial table creation
	master_col_names = [
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> development
		'CASE_NUMBER', 'TASK_NUMBER', 'PROVIDER_NUMBER', 'PROVIDER_ADDRESS_NUMBER', 'PROVIDER_CATEGORY',
		'PROVIDER_CANDIDATE_NUMBER', 'DISPATCH_RESPONSE', 'DISPATCH_RESPONSE_REASON', 'DISPATCH_TYPE',
		'ENROUTE_MILES', 'SERVICE_TIME_EASTERN', 'SERVICE_TIME_UTC', 'DISPATCH_REQUEST_TIME_EASTERN',
		'DISPATCH_REQUEST_TIME_UTC', 'VENDOR_NAME', 'VENDOR_ADDRESS_1', 'VENDOR_ADDRESS_2',
		'PROVIDER_ZIP', 'VENDOR_PHONE', 'VENDOR_FAX', 'ETA_MINUTES', 'TOTAL_ETA_EXTENSION_MINUTES',
		'TOTAL_ETA_EXTENSION_COUNT', 'IS_DISPATCHED_PROVIDER'
<<<<<<< HEAD
=======
		'CASE_ID', 'TASK_ID', 'VENDOR_ID', 'VENDOR_ADDRESS_ID', 'VENDOR_CATEGORY',
		'VENDOR_CANDIDATE_NUMBER', 'DISPATCH_RESPONSE', 'DISPATCH_RESPONSE_REASON', 'DISPATCH_TYPE',
		'ENROUTE_MILES', 'SERVICE_TIME_EASTERN', 'SERVICE_TIME_UTC', 'DISPATCH_REQUEST_TIME_EASTERN',
		'DISPATCH_REQUEST_TIME_UTC', 'VENDOR_NAME', 'VENDOR_ADDRESS_1', 'VENDOR_ADDRESS_2',
		'VENDOR_ZIP', 'VENDOR_PHONE', 'VENDOR_FAX', 'ETA_MINUTES', 'TOTAL_ETA_EXTENSION_MINUTES',
		'TOTAL_ETA_EXTENSION_COUNT', 'IS_DISPATCHED_VENDOR'
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
>>>>>>> development
	]
	
	print('# of columns: ', len(column_names))
	print('column names: ', '\n', column_names)
	
	# Rule 1: expect columns to exists
	for col in master_col_names:
		print(col + ':', batch.expect_column_to_exist(col, result_format="BASIC", catch_exceptions=True))
	
	# Rule 3: Checking which columns should not have null values
	print('Viewing column null value counts: ', batch.isnull().sum(), sep='\n')
	not_null_cols = [
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> development
		'CASE_NUMBER', 'TASK_NUMBER', 'PROVIDER_NUMBER', 'PROVIDER_ADDRESS_NUMBER', 'PROVIDER_CATEGORY',
		'PROVIDER_CANDIDATE_NUMBER', 'DISPATCH_RESPONSE', 'DISPATCH_RESPONSE_REASON', 'DISPATCH_TYPE',
		'SERVICE_TIME_EASTERN', 'SERVICE_TIME_UTC', 'DISPATCH_REQUEST_TIME_EASTERN',
		'DISPATCH_REQUEST_TIME_UTC', 'TOTAL_ETA_EXTENSION_MINUTES',
		'TOTAL_ETA_EXTENSION_COUNT', 'IS_DISPATCHED_PROVIDER'
<<<<<<< HEAD
=======
		'CASE_ID', 'TASK_ID', 'VENDOR_ID', 'VENDOR_ADDRESS_ID', 'VENDOR_CATEGORY',
		'VENDOR_CANDIDATE_NUMBER', 'DISPATCH_RESPONSE', 'DISPATCH_RESPONSE_REASON', 'DISPATCH_TYPE',
		'SERVICE_TIME_EASTERN', 'SERVICE_TIME_UTC', 'DISPATCH_REQUEST_TIME_EASTERN',
		'DISPATCH_REQUEST_TIME_UTC', 'TOTAL_ETA_EXTENSION_MINUTES',
		'TOTAL_ETA_EXTENSION_COUNT', 'IS_DISPATCHED_VENDOR'
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
>>>>>>> development
	]
	for col in not_null_cols:
		print(col, '\n', batch.expect_column_values_to_not_be_null(col, result_format='BASIC', catch_exceptions=True))
	
	# Rule 4: adjusting weights to mostly at this minimum
	
	# not_null_weights = batch.count() / len(batch)
	# batch.notnull().mean()
	# percent_missing = 1 - (batch.isnull().sum() / len(batch))
	# data[pd.notnull(data.column)]
	# data[data.column.notnull()]
	
	# df_date_normalized = rule_df.groupby(pd.DatetimeIndex(rule_df['DISPATCH_REQUEST_TIME_UTC']).normalize())
<<<<<<< HEAD
<<<<<<< HEAD
	# df_date_normalized.apply(lambda x: x['PROVIDER_CATEGORY'].notnull().mean())
=======
	# df_date_normalized.apply(lambda x: x['VENDOR_CATEGORY'].notnull().mean())
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
	# df_date_normalized.apply(lambda x: x['PROVIDER_CATEGORY'].notnull().mean())
>>>>>>> development
	
	# g = df.groupby(df.DISPATCH_REQUEST_TIME_UTC.dt.week)
	# g.apply(lambda x: x['ENROUTE_MILES'].notnull().mean())
	
<<<<<<< HEAD
<<<<<<< HEAD
	weights = {'ETA_MINUTES': 0.6, 'PROVIDER_CATEGORY': 0.97, 'ENROUTE_MILES': 0.95}
=======
	weights = {'ETA_MINUTES': 0.6, 'VENDOR_CATEGORY': 0.97, 'ENROUTE_MILES': 0.95}
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
	weights = {'ETA_MINUTES': 0.6, 'PROVIDER_CATEGORY': 0.97, 'ENROUTE_MILES': 0.95}
>>>>>>> development
	for col, weight in weights.items():
		print(col, '\n', batch.expect_column_values_to_not_be_null(col, mostly=weight, include_config=True,
		                                                           catch_exceptions=True,
		                                                           result_format='BASIC'))
	
	# Expecting columns to be in an unique set at least X % of the time (X given by dict value)
<<<<<<< HEAD
<<<<<<< HEAD
	category_cols = {'PROVIDER_CATEGORY': 0.8, 'DISPATCH_RESPONSE_REASON': 0.8, 'DISPATCH_RESPONSE': 0.99,
=======
	category_cols = {'VENDOR_CATEGORY': 0.8, 'DISPATCH_RESPONSE_REASON': 0.8, 'DISPATCH_RESPONSE': 0.99,
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
	category_cols = {'PROVIDER_CATEGORY': 0.8, 'DISPATCH_RESPONSE_REASON': 0.8, 'DISPATCH_RESPONSE': 0.99,
>>>>>>> development
	                 'DISPATCH_TYPE': 0.99}
	val_set_list = []
	
	for col in category_cols:
		list_sets = list(batch.get_column_value_counts(col, sort='value').keys())
		val_set_list.append(list_sets)
	
	for col, vals in zip(category_cols, val_set_list):
		print('{} unique values: '.format(col), '\n', vals, '\n')
	
	# Rule 5: expecting set values in specified columns
	for col, vals in zip(category_cols, val_set_list):
		print(col, '\n',
		      batch.expect_column_values_to_be_in_set(col, vals, mostly=category_cols[col], result_format='BASIC',
		                                              include_config=True, catch_exceptions=True), '\n')
	# TODO: dispatch_type in complete set - don't need weight
	# TODO: Get real set values for vendor_category & task_status_code in edw
	
	# Rule 6: Checking eta values to be in certain range
	print('ETA VALUE RANGES')
	print(
		batch.expect_column_values_to_be_between('ETA_MINUTES', min_value=0, max_value=180, mostly=0.98,
		                                         result_format='BASIC',
		                                         catch_exceptions=True))
	print(batch.expect_column_min_to_be_between('ETA_MINUTES', min_value=0, catch_exceptions=True,
	                                            result_format='SUMMARY'))
	
	print('ENROUTE VALUE RANGES')
	print(
		batch.expect_column_values_to_be_between('ENROUTE_MILES', min_value=0, max_value=100, mostly=0.98,
		                                         result_format='BASIC',
		                                         catch_exceptions=True))
	print(batch.expect_column_min_to_be_between('ENROUTE_MILES', min_value=0))
	
	# Rule 7: Looking dispatch_request_time to be greater than service_time
	print(batch.expect_column_pair_values_A_to_be_greater_than_B('DISPATCH_REQUEST_TIME_UTC', 'SERVICE_TIME_UTC',
	                                                             mostly=0.7, ignore_row_if='either_value_is_missing',
	                                                             result_format='BASIC', catch_exceptions=True))
	
	# Matches any integer, including negative leading with -, and things with leading 0
<<<<<<< HEAD
<<<<<<< HEAD
	print(batch.expect_column_values_to_match_regex('PROVIDER_NUMBER', '^([-]?\d+)$'))
	# Matches US zip codes (including +4) and Canadian zip codes
	print(batch.expect_column_values_to_match_regex('PROVIDER_ZIP',
=======
	print(batch.expect_column_values_to_match_regex('VENDOR_ID', '^([-]?\d+)$'))
	# Matches US zip codes (including +4) and Canadian zip codes
	print(batch.expect_column_values_to_match_regex('VENDOR_ZIP',
>>>>>>> 79e5d6536fef5fca5f5bed02784b1b092bc19655
=======
	print(batch.expect_column_values_to_match_regex('PROVIDER_NUMBER', '^([-]?\d+)$'))
	# Matches US zip codes (including +4) and Canadian zip codes
	print(batch.expect_column_values_to_match_regex('PROVIDER_ZIP',
>>>>>>> development
	                                                '^(\d{5}(-\d{4})?|[A-CEGHJ-NPRSTVXY]\d[A-CEGHJ-NPRSTV-Z] ?\d[A-CEGHJ-NPRSTV-Z]\d)$'))
	
	# Rule 8: column to be of data type
	for col, typ in batch.dtypes.items():
		print(col,
		      batch.expect_column_values_to_be_of_type(col, str(typ), result_format='BASIC', catch_exceptions=True),
		      sep='\n')
	
	print('validation rules captured for {} suite: '.format(batch.get_expectation_suite_name()))
	print(json.dumps(batch.get_expectation_suite(), indent=4))
	print(f"{batch.get_expectation_suite_name()} expectation suite saved.")
	
	batch.save_expectation_suite()


# TODO: add command line arguments to declare table_name, suite_name, and rule_query_type when calling in terminal

if __name__ == "__main__":
	batch_df = generate_suite_and_batch_data(table_name='provider_network', suite_name='warnings_2019dec',
	                                         rule_query_type='create_expectations_dec')
	print('here')
	generate_expectations(table_name='provider_network', batch=batch_df)
