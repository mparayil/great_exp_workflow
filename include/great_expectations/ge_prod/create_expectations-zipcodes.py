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
			context.create_expectation_suite(data_asset_name, expectation_suite_name=suite_name, overwrite_existing=True)
	
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
	master_col_names = ['ZIP_CODE', 'CITY', 'STATE_CODE', 'METRO_NAME', 'METRO_TYPE', 'COUNTY', 'LATITUDE', 'LONGITUDE',
	                    'TIMEZONE']
	
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
	
	# Rule 3: checking for all columns that shouldn't be null at all
	for col in not_null_cols:
		print(col, '\n', batch.expect_column_values_to_not_be_null(col, result_format='BASIC'))
	
	# calculating weight for columns of how often they should be null
	
	# Rule 4: validating columns that should be null
	default_not_null_weights = {'COUNTY': 0.919, 'LATITUDE': 0.952, 'LONGITUDE': 0.952, 'TIMEZONE': 0.945}
	for col, weight in default_not_null_weights.items():
		print(col, batch.expect_column_values_to_not_be_null(col, mostly=weight, catch_exceptions=True,
		                                                     result_format='SUMMARY'), sep='\n')
	
	# Rule 5: Expecting column values to be in set
	col_set_vals = {
		'STATE_CODE': ['MA', 'RI', 'NH', 'NY', 'PA', 'VA', 'ME', 'NJ', 'AE', 'NC', 'GA', 'FL', 'IA', 'MN', 'NE', 'AL',
		               'OH', 'IN', 'MI', 'WI', 'DC', 'WA', 'KY', 'AZ', 'TN', 'MS', 'DE', 'LA', 'AR', 'TX', 'NM', 'NV',
		               'ND', 'CO', 'OR', 'AS', 'GU', 'PW', 'MP', 'MH', 'PR', 'MO', 'CT', 'WV', 'KS', 'UT', 'CA', 'IL',
		               'OK', 'ID', 'WY', 'SD', 'MT', 'HI', 'FM', 'AP', 'AK', 'VI', 'SC', 'VT', 'AA', 'MD'],
		'METRO_TYPE': ['Tertiary', 'Secondary', 'Primary'],
		'TIMEZONE': ['America/New_York', 'Europe/Berlin', 'America/Chicago', 'America/Denver',
		             'America/Indiana/Indianapolis', 'America/Detroit', 'America/Menominee', 'Europe/Oslo',
		             'Europe/Brussels', 'America/Godthab', 'Europe/Copenhagen', 'Europe/Rome', 'Atlantic/Azores',
		             'Europe/Helsinki', 'Atlantic/Reykjavik', 'Europe/Lisbon', 'America/Halifax', 'America/Toronto',
		             'Asia/Karachi', 'Africa/Monrovia', 'Africa/Kinshasa', 'Asia/Jerusalem', 'Africa/Cairo',
		             'Europe/Athens', 'Asia/Amman', 'America/Phoenix', 'America/North_Dakota/Beulah', 'Asia/Seoul',
		             'America/Adak', 'Pacific/Auckland', 'Asia/Bangkok', 'America/Nome', 'America/Kentucky/Louisville',
		             'America/Indiana/Marengo', 'America/Costa_Rica', 'America/La_Paz', 'America/Caracas',
		             'America/Santo_Domingo', 'Africa/Nairobi', 'America/North_Dakota/Center', 'Pacific/Honolulu',
		             'Australia/Darwin', 'uninhabited', 'Europe/Madrid', 'America/Thule', 'America/Indiana/Knox',
		             'America/Santiago', 'Australia/Adelaide', 'America/Anchorage', 'America/Sao_Paulo',
		             'Africa/Casablanca', 'Indian/Mahe', 'Africa/Khartoum', 'America/Los_Angeles', 'Pacific/Wake',
		             'America/Winnipeg', 'Asia/Singapore', 'America/Indiana/Winamac', 'America/Panama',
		             'America/Guatemala', 'Europe/Amsterdam', 'Europe/Paris', 'Europe/Istanbul', 'Asia/Nicosia',
		             'America/Indiana/Tell_City', 'America/Boise', 'Asia/Tokyo', 'Asia/Jakarta', 'Pacific/Guam',
		             'America/Montreal', 'America/Indiana/Vevay', 'America/Managua', 'America/Lima',
		             'America/Argentina/Buenos_Aires', 'America/Montevideo', 'America/Asuncion', 'America/Puerto_Rico',
		             'America/Nassau', 'America/Yakutat', 'America/Metlakatla', 'America/Indiana/Vincennes',
		             'Asia/Hong_Kong', 'America/Guayaquil', 'America/Tegucigalpa', 'America/Juneau',
		             'America/Kentucky/Monticello', 'Asia/Riyadh', 'Australia/Melbourne', 'America/El_Salvador',
		             'America/Indiana/Petersburg', 'Australia/Sydney', 'Europe/London', 'America/Antigua',
		             'America/Sitka', 'America/North_Dakota/New_Salem', 'America/Bogota']}
	for col, val_set in col_set_vals.items():
		print(col, batch.expect_column_values_to_be_in_set(col, val_set, result_format='SUMMARY',
		                                                   include_config=True, catch_exceptions=True), '\n')
	
	# Rule 6: Determine if CASE_ID is unique
	# TODO: verify if unique, according to notes from Austin & Jody it is.
	print('ZIP_CODE', batch.expect_column_values_to_be_unique('ZIP_CODE', result_format='SUMMARY', catch_exceptions=True))
	print(batch.expect_column_values_to_match_regex('ZIP_CODE', '^(\d{5}(-\d{4})?|[A-CEGHJ-NPRSTVXY]\d[A-CEGHJ-NPRSTV-Z] ?\d[A-CEGHJ-NPRSTV-Z]\d)$'))
	
	# Rule 10: Looking at columns to be of said data types
	zip_code_lookup_data_types = {'ZIP_CODE': 'object', 'CITY': 'object', 'STATE_CODE': 'object',
	                              'METRO_NAME': 'object', 'METRO_TYPE': 'object', 'COUNTY': 'object',
	                              'LATITUDE': 'float64', 'LONGITUDE': 'float64', 'TIMEZONE': 'object'}
	for col, typ in zip_code_lookup_data_types.items():
		print(col, batch.expect_column_values_to_be_of_type(col, typ, catch_exceptions=True), sep='\n')
		
	# Rule 10: looking at survey mean response to be in set range
	
	print('validation rules captured for {} suite: '.format(batch.get_expectation_suite_name()))
	print(f"{batch.get_expectation_suite_name()} expectation suite saved.")
	
	batch.save_expectation_suite()


# TODO: add command line arguments to declare table_name, suite_name, and rule_query_type when calling in terminal
if __name__ == "__main__":
	batch_df = generate_suite_and_batch_data(table_name='zipcodes', suite_name='warnings',
	                                         rule_query_type='create_expectations')
	print('here')
	generate_expectations(table_name='zipcodes', batch=batch_df)