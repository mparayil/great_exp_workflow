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


# TODO: wrap logging around logic below
def generate_expectations(table_name: str, batch):
	# getting logger
	rlog = gda.get_logger()
	
	# getting batchId & fingerprint
	rule_batch_fingerprint = batch.batch_fingerprint
	rule_batch_id = batch.batch_id
	print('rule_batch_fingerprint: ', '\n', rule_batch_fingerprint)
	print('rule_batch_id: ', '\n', rule_batch_id)
	
	# Validating to see if columns exists
	column_names = batch.get_table_columns()
	# master column names identified from initial table creation
	master_col_names = ['CASE_NUMBER', 'SURVEY_RESPONSE', 'SURVEY_COMMENTS', 'SURVEY_SOURCE', 'SURVEY_TIME_EASTERN',
	                    'SURVEY_TIME_UTC']
	
	print('# of columns: ', len(column_names))
	print('column names: ', '\n', column_names)
	
	# Rule 1: expect columns to exists
	for col in master_col_names:
		print(col + ':', batch.expect_column_to_exist(col, result_format='BASIC', catch_exceptions=True), sep='\n')
	
	# Rule 2: Validating column count in table matches master column names
	print('# of columns in {}: '.format(table_name), len(master_col_names), '\n')
	if len(column_names) == len(master_col_names):
		print(batch.expect_table_column_count_to_equal(len(column_names)))
	else:
		print(batch.expect_table_column_count_to_equal(len(master_col_names)))
	
	# Checking which columns should not have null values
	print('Viewing column null value counts: ', batch.isnull().sum(), sep='\n')
	not_null_cols = list(batch.isnull().sum()[batch.isnull().sum() == 0].keys())
	
	# Rule 3: checking for all columns that shouldn't be null are not
	for col in not_null_cols:
		print(col, '\n', batch.expect_column_values_to_not_be_null(col, result_format='BASIC'))
	
	# calculating weight for columns of how often they should be null
	
	# Rule 4: validating columns that should be null - SURVEY_COMMENTS
	# w = gda.get_df_not_null_weights(batch, 'SURVEY_TIME_UTC', 'SURVEY_COMMENTS')
	survey_comments_not_null_weight = 0.06
	print('SURVEY_COMMENTS', batch.expect_column_values_to_not_be_null("SURVEY_COMMENTS",
	                                                                   mostly=survey_comments_not_null_weight,
	                                                                   catch_exceptions=True,
	                                                                   result_format='SUMMARY'), sep='\n')
	
	# Rule 5: Expecting column values to be in set or range
	col_set_vals = {'SURVEY_SOURCE': ['web', 'sms']}
	for col, val_set in col_set_vals.items():
		print('SURVEY_SOURCE',
		      batch.expect_column_values_to_be_in_set('SURVEY_SOURCE', val_set, result_format='SUMMARY',
		                                              include_config=True, catch_exceptions=True), '\n')
	print('SURVEY_RESPONSE', batch.expect_column_values_to_be_between('SURVEY_RESPONSE', 0, 10, result_format='SUMMARY',
	                                                                  include_config=True, catch_exceptions=True), '\n')
	
	# Rule 6: Determine if CASE_NUMBER is unique
	# TODO: verify if unique, according to notes from Austin & Jody it is.
	batch.expect_column_values_to_be_unique('CASE_NUMBER', result_format='SUMMARY', catch_exceptions=True)
	
	# Rule 9: Looking at column A being larger than column B
	# SURVEY_TIME_UTC > SURVEY_TIME_EASTERN
	print(batch.expect_column_pair_values_A_to_be_greater_than_B('SURVEY_TIME_UTC', 'SURVEY_TIME_EASTERN',
	                                                             ignore_row_if='either_value_is_missing',
	                                                             result_format='SUMMARY', catch_exceptions=True))
	# Rule 10: Looking at columns to be of said data types
	customer_experience_data_types = {'CASE_NUMBER': 'int64', 'SURVEY_RESPONSE': 'int8', 'SURVEY_COMMENTS': 'object',
	                                  'SURVEY_SOURCE': 'object', 'SURVEY_TIME_EASTERN': 'datetime64[ns]',
	                                  'SURVEY_TIME_UTC': 'datetime64[ns]'}
	for col, typ in customer_experience_data_types.items():
		print(col, batch.expect_column_values_to_be_of_type(col, typ, catch_exceptions=True), sep='\n')
	
	# Rule 10: looking at survey mean response to be in set range
	batch_groupby = batch.groupby(batch["SURVEY_TIME_UTC"].dt.date)
	batch_groupby = batch_groupby.apply(lambda x: x['SURVEY_RESPONSE'].mean())
	print(batch_groupby.quantile([0., .05, .1, .33, .667, 1.0]))
	
	print(batch.expect_column_mean_to_be_between('SURVEY_RESPONSE', min_value=8.9, max_value=9.7,
	                                             result_format='COMPLETE'))
	
	print('validation rules captured for {} suite: '.format(batch.get_expectation_suite_name()))
	print(json.dumps(batch.get_expectation_suite(), indent=4))
	print(f"{batch.get_expectation_suite_name()} expectation suite saved.")
	
	batch.save_expectation_suite()


# TODO: add command line arguments to declare table_name, suite_name, and rule_query_type when calling in terminal
if __name__ == "__main__":
	batch_df = generate_suite_and_batch_data(table_name='surveys', suite_name='warnings_2019Q3-Q4',
	                                         rule_query_type='create_expectations_2019Q3-Q4')
	print('here')
	generate_expectations(table_name='surveys', batch=batch_df)
