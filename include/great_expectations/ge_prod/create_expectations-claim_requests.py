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
	rule_logger = gda.get_logger()
	
	# getting batchId & fingerprint
	rule_batch_fingerprint = batch.batch_fingerprint
	rule_batch_id = batch.batch_id
	print('rule_batch_fingerprint: ', '\n', rule_batch_fingerprint)
	print('rule_batch_id: ', '\n', rule_batch_id)
	
	# Validating to see if columns exists
	column_names = batch.get_table_columns()
	# master column names identified from initial table creation
	master_col_names = ['ADDCHARGE_AMOUNT', 'ADDCHARGE_COUNT', 'ADDCHARGE_DETAILS',
	                    'ADDPAY_APPROVED_DATE_EASTERN', 'ADDPAY_APPROVED_DATE_UTC',
	                    'ADDPAY_APPROVED_PAYMENT', 'ADDPAY_COUNT', 'ADDPAY_DETAILS',
	                    'ADDPAY_PAYMENT_AMOUNT', 'ADDPAY_SUBMITTED_DATE_EASTERN', 'ADDPAY_SUBMITTED_DATE_UTC',
	                    'BASE_TOTAL_FROM_RATES', 'CASE_NUMBER', 'CLAIM_EQUIPMENT_CODE', 'CLAIM_NUMBER',
	                    'CLAIM_PO_NUMBER_ENTERED', 'COMPLETE_APPROVED_PAYMENT', 'IS_CLAIM_APPROVED', 'IS_VCC',
	                    'MODIFIED_DATE_EASTERN', 'MODIFIED_DATE_UTC', 'ORIGINAL_CLAIM_APPROVED_DATE_EASTERN',
	                    'ORIGINAL_CLAIM_APPROVED_DATE_UTC', 'ORIGINAL_CLAIM_APPROVED_PAYMENT',
	                    'ORIGINAL_CLAIM_PAYMENT_AMOUNT', 'ORIGINAL_CLAIM_STATUS_CODE',
	                    'ORIGINAL_CLAIM_SUBMITTED_DATE_EASTERN',
	                    'ORIGINAL_CLAIM_SUBMITTED_DATE_UTC', 'ORIGINAL_CLAIM_TYPE_CODE',
	                    'SERVICE_DATE_EASTERN', 'SERVICE_DATE_UTC', 'SERVICE_ID',
	                    'SUBMITTED_CLAIM_AMOUNT', 'SUBMITTED_ENROUTE_MILES',
	                    'SUBMITTED_LABOR_HOURS', 'SUBMITTED_TOW_MILES', 'TICKET_NUMBER',
	                    'PROVIDER_ADDRESS_NUMBER', 'PROVIDER_NUMBER', 'WAS_AUDITED']
	
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
	null_percents = (1 - (batch.isnull().sum() / len(batch))[batch.isnull().sum() / len(batch) > 0])
	for x, y in null_percents.items():
		print(x, y, sep='\n')
	
	not_null_weights = dict(null_percents)
	
	# lowering weights by one thousandth of decimal
	for key, weight in not_null_weights.items():
		not_null_weights[key] = round(weight - 0.001, 3)
	
	print('---------------------------------------')
	print('not null weights:')
	for x, y in not_null_weights.items():
		print(x, y)
	
	# Rule 4: validating columns that should be null
	# updated not_null_weights to take into account addpays over daily batch data
	# TODO: add logic to capture not null weights when scaled, function in ge_data_access existing
	not_null_weights = {'PROVIDER_ADDRESS_NUMBER': 0.988, 'CLAIM_PO_NUMBER_ENTERED': 0.989, 'SUBMITTED_TOW_MILES': 0.968,
	                    'SUBMITTED_ENROUTE_MILES': 0.984, 'SUBMITTED_LABOR_HOURS': 0.927, 'ADDCHARGE_AMOUNT': 0.185,
	                    'ADDCHARGE_DETAILS': 0.185, 'BASE_TOTAL_FROM_RATES': 0.642, 'ADDPAY_DETAILS': 0.005,
	                    'ADDPAY_COUNT': 0.005, 'ADDPAY_PAYMENT_AMOUNT': 0.005, 'ADDPAY_APPROVED_PAYMENT': 0.005,
	                    'ORIGINAL_CLAIM_APPROVED_DATE_EASTERN': 0.966, 'ORIGINAL_CLAIM_APPROVED_DATE_UTC': 0.966,
	                    'SERVICE_DATE_EASTERN': 0.99, 'SERVICE_DATE_UTC': 0.99,
	                    'ORIGINAL_CLAIM_SUBMITTED_DATE_EASTERN': 0.989,
	                    'ORIGINAL_CLAIM_SUBMITTED_DATE_UTC': 0.989, 'ADDPAY_APPROVED_DATE_EASTERN': 0.002,
	                    'ADDPAY_APPROVED_DATE_UTC': 0.002,
	                    'ADDPAY_SUBMITTED_DATE_EASTERN': 0.005, 'ADDPAY_SUBMITTED_DATE_UTC': 0.005}
	for col, weight in not_null_weights.items():
		print(col, batch.expect_column_values_to_not_be_null(col, mostly=weight, catch_exceptions=True,
		                                                     result_format='SUMMARY'), sep='\n')
	
	# Rule 5: Expecting column values to be in set
	column_value_sets = {'ORIGINAL_CLAIM_STATUS_CODE': ["AA", "SP", "FP", "DP", "HD", "RB", "IN", "WO", "PJ", "PA",
	                                                    "XX", "PD", "PC", "AP", "FC" "VD"],
	                     'ORIGINAL_CLAIM_TYPE_CODE': ['DLR', 'VND', 'OWN'],
	                     'SERVICE_ID': ["ROAD", "LOCK", "INTR", "Goa", "tow", "TWRBT", "winch", "TOW", "Tow", "road",
	                                    "Winch", "GOA", "Road", "REPO", "goa", "TRPLC", 'NA', "REU", "Lock", "TFIX",
	                                    "Jump"]
	                     }
	for col, val_set in column_value_sets:
		print(col, batch.expect_column_values_to_be_in_set(col, val_set, result_format='SUMMARY',
		                                                   catch_exceptions=True), '\n')
	
	# Rule 6: Determine if variant columns are parseable
	json_columns = ["ADDCHARGE_DETAILS", "ADDPAY_DETAILS"]
	for col in json_columns:
		print(col, batch.expect_column_values_to_be_json_parseable(col, result_format='BOOLEAN_ONLY', mostly=0.98))
	
	# Rule 7: Determine if variant columns match json_schema
	addpay_schema = {
		"definitions": {},
		"$schema": "http://json-schema.org/draft-07/schema#",
		"$id": "http://example.com/root.json",
		"type": [
			"array",
			"null"
		],
		"title": "The Root Schema",
		"items": {
			"$id": "#/items",
			"type": [
				"object",
				"null"
			],
			"title": "The Items Schema",
			"required": [
				"addpay_approved_date",
				"addpay_approved_payment_amount",
				"addpay_claim_id",
				"addpay_is_approved",
				"addpay_payment_amount",
				"addpay_submitted_date",
				"addpay_task_id",
				"addpay_vendor_id",
				"original_claim_amount_paid",
				"original_claim_amount_requested",
				"original_claim_id",
				"original_claim_vendor_id",
				"service_date"
			],
			"properties": {
				"addpay_approved_date": {
					"$id": "#/items/properties/addpay_approved_date",
					"type": [
						"string",
						"null"
					],
					"format": "date-time",
					"title": "The Addpay_approved_date Schema",
					"default": "",
					"examples": [
						"2019-02-20T04:02:00"
					],
					"pattern": "^(.*)$"
				},
				"addpay_approved_payment_amount": {
					"$id": "#/items/properties/addpay_approved_payment_amount",
					"type": [
						"number",
						"null"
					],
					"title": "The Addpay_approved_payment_amount Schema",
					"default": 0,
					"examples": [
						27
					]
				},
				"addpay_claim_id": {
					"$id": "#/items/properties/addpay_claim_id",
					"type": [
						"number",
						"null"
					],
					"title": "The Addpay_claim_id Schema",
					"default": 0,
					"examples": [
						88551584
					]
				},
				"addpay_is_approved": {
					"$id": "#/items/properties/addpay_is_approved",
					"type": [
						"boolean",
						"null"
					],
					"title": "The Addpay_is_approved Schema",
					"default": False,
					"examples": [
						True
					]
				},
				"addpay_payment_amount": {
					"$id": "#/items/properties/addpay_payment_amount",
					"type": [
						"number",
						"null"
					],
					"title": "The Addpay_payment_amount Schema",
					"default": 0,
					"examples": [
						27
					]
				},
				"addpay_submitted_date": {
					"$id": "#/items/properties/addpay_submitted_date",
					"type": [
						"string",
						"null"
					],
					"format": "date-time",
					"title": "The Addpay_submitted_date Schema",
					"default": "",
					"examples": [
						"2019-02-19T00:00:00"
					],
					"pattern": "^(.*)$"
				},
				"addpay_task_id": {
					"$id": "#/items/properties/addpay_task_id",
					"type": [
						"number",
						"null"
					],
					"title": "The Addpay_task_id Schema",
					"default": 0,
					"examples": [
						2
					]
				},
				"addpay_vendor_id": {
					"$id": "#/items/properties/addpay_vendor_id",
					"type": [
						"string",
						"null"
					],
					"title": "The Addpay_vendor_id Schema",
					"default": "",
					"examples": [
						"133573"
					],
					"pattern": "^(.*)$"
				},
				"original_claim_amount_paid": {
					"$id": "#/items/properties/original_claim_amount_paid",
					"type": [
						"number",
						"null"
					],
					"title": "The Original_claim_amount_paid Schema",
					"default": 0.0,
					"examples": [
						57.95
					]
				},
				"original_claim_amount_requested": {
					"$id": "#/items/properties/original_claim_amount_requested",
					"type": [
						"number",
						"null"
					],
					"title": "The Original_claim_amount_requested Schema",
					"default": 0,
					"examples": [
						59
					]
				},
				"original_claim_id": {
					"$id": "#/items/properties/original_claim_id",
					"type": [
						"number",
						"null"
					],
					"title": "The Original_claim_id Schema",
					"default": 0,
					"examples": [
						195082625
					]
				},
				"original_claim_vendor_id": {
					"$id": "#/items/properties/original_claim_vendor_id",
					"type": [
						"string",
						"null"
					],
					"title": "The Original_claim_vendor_id Schema",
					"default": "",
					"examples": [
						"133573"
					],
					"pattern": "^(.*)$"
				},
				"service_date": {
					"$id": "#/items/properties/service_date",
					"type": [
						"string",
						"null"
					],
					"format": "date-time",
					"title": "The Service_date Schema",
					"default": "",
					"examples": [
						"2019-02-14T11:50:21"
					],
					"pattern": "^(.*)$"
				}
			}
		}
	}
	print(batch.expect_column_values_to_match_json_schema('ADDPAY_DETAILS', addpay_schema, mostly=0.92,
	                                                      result_format='BOOLEAN_ONLY'))
	# TODO: need to validate and add json_schema for addcharge_details field
	addcharge_schema = {
		"definitions": {},
		"$schema": "http://json-schema.org/draft-07/schema#",
		"$id": "http://example.com/root.json",
		"type": [
			"array",
			"null"
		],
		"title": "The Root Schema",
		"items": {
			"$id": "#/items",
			"type": [
				"object",
				"null"
			],
			"title": "The Items Schema",
			"properties": {
				"addcharge_amount": {
					"$id": "#/items/properties/addcharge_amount",
					"type": ["number", "null"],
					"title": "The Addcharge_amount Schema"
				},
				"addcharge_type": {
					"$id": "#/items/properties/addcharge_type",
					"type": ["string", "null"],
					"title": "The Addcharge_type Schema"
				}
			},
			'additionalProperties': {"type": "string"}
		}
	}
	print(batch.expect_column_values_to_match_json_schema('ADDCHARGE_DETAILS', addcharge_schema, mostly=0.8))
	
	# Rule 8: Determine columns are unique per row
	print(batch.expect_multicolumn_values_to_be_unique(column_list=['CASE_NUMBER', 'TICKET_NUMBER'], catch_exceptions=True))
	print(batch.expect_multicolumn_values_to_be_unique(column_list=['CASE_NUMBER', 'TICKET_NUMBER', 'PROVIDER_NUMBER'],
	                                                   catch_exceptions=True))
	print(batch.expect_column_values_to_be_unique('CLAIM_NUMBER', catch_exceptions=True))
	
	# Rule 9: Looking at column A being larger than column B
	# ORIGINAL_CLAIM_SUBMITTED_DATE_UTC > SERVICE_DATE_UTC
	# ORIGINAL_CLAIM_APPROVED_DATE_UTC > ORIGINAL_CLAIM_SUBMITTED_DATE_UTC
	# MODIFIED_DATE_UTC > ADDPAY_APPROVED_DATE_UTC
	print(
		batch.expect_column_pair_values_A_to_be_greater_than_B('ORIGINAL_CLAIM_SUBMITTED_DATE_UTC', 'SERVICE_DATE_UTC',
		                                                       mostly=0.99, ignore_row_if='either_value_is_missing',
		                                                       catch_exceptions=True))
	print(batch.expect_column_pair_values_A_to_be_greater_than_B('ORIGINAL_CLAIM_APPROVED_DATE_UTC',
	                                                             'ORIGINAL_CLAIM_SUBMITTED_DATE_UTC',
	                                                             mostly=0.99, ignore_row_if='either_value_is_missing',
	                                                             catch_exceptions=True))
	print(batch.expect_column_pair_values_A_to_be_greater_than_B('MODIFIED_DATE_UTC', 'ADDPAY_APPROVED_DATE_UTC',
	                                                             or_equal=True, ignore_row_if='either_value_is_missing',
	                                                             result_format='SUMMARY', catch_exceptions=True))
	# Rule 10: Looking at columns to be of said data types
	# network_claims_data_types = dict(batch.dtypes.iteritems())
	# for key, val in network_claims_data_types.items():
	# 	network_claims_data_types[key] = str(val)
	network_claims_data_types = {'CASE_NUMBER': 'int64', 'TICKET_NUMBER': 'int8', 'PROVIDER_NUMBER': 'object', 'CLAIM_NUMBER': 'int32',
	                             'PROVIDER_ADDRESS_NUMBER': 'float64', 'CLAIM_PO_NUMBER_ENTERED': 'float64',
	                             'ORIGINAL_CLAIM_PAYMENT_AMOUNT': 'float64', 'ORIGINAL_CLAIM_STATUS_CODE': 'object',
	                             'ORIGINAL_CLAIM_TYPE_CODE': 'object', 'SUBMITTED_CLAIM_AMOUNT': 'float64',
	                             'SUBMITTED_TOW_MILES': 'float64', 'SUBMITTED_ENROUTE_MILES': 'float64',
	                             'SUBMITTED_LABOR_HOURS': 'float64', 'SERVICE_ID': 'object', 'WAS_AUDITED': 'bool',
	                             'ADDCHARGE_AMOUNT': 'float64', 'IS_CLAIM_APPROVED': 'bool',
	                             'ORIGINAL_CLAIM_APPROVED_PAYMENT': 'float64', 'ADDCHARGE_DETAILS': 'object',
	                             'ADDCHARGE_COUNT': 'int16', 'BASE_TOTAL_FROM_RATES': 'float64',
	                             'ADDPAY_DETAILS': 'object', 'ADDPAY_COUNT': 'float64',
	                             'ADDPAY_PAYMENT_AMOUNT': 'float64', 'CLAIM_EQUIPMENT_CODE': 'object',
	                             'ADDPAY_APPROVED_PAYMENT': 'float64', 'COMPLETE_APPROVED_PAYMENT': 'float64',
	                             'IS_VCC': 'bool', 'ORIGINAL_CLAIM_APPROVED_DATE_EASTERN': 'datetime64[ns]',
	                             'ORIGINAL_CLAIM_APPROVED_DATE_UTC': 'datetime64[ns]',
	                             'SERVICE_DATE_EASTERN': 'datetime64[ns]', 'SERVICE_DATE_UTC': 'datetime64[ns]',
	                             'ORIGINAL_CLAIM_SUBMITTED_DATE_EASTERN': 'datetime64[ns]',
	                             'ORIGINAL_CLAIM_SUBMITTED_DATE_UTC': 'datetime64[ns]',
	                             'ADDPAY_APPROVED_DATE_EASTERN': 'datetime64[ns]',
	                             'ADDPAY_APPROVED_DATE_UTC': 'datetime64[ns]',
	                             'ADDPAY_SUBMITTED_DATE_EASTERN': 'datetime64[ns]',
	                             'ADDPAY_SUBMITTED_DATE_UTC': 'datetime64[ns]',
	                             'MODIFIED_DATE_EASTERN': 'datetime64[ns]',
	                             'MODIFIED_DATE_UTC': 'datetime64[ns]'}
	for col, typ in network_claims_data_types.items():
		print(col, batch.expect_column_values_to_be_of_type(col, typ, catch_exceptions=True), sep='\n')
	
	print('validation rules captured for {} suite: '.format(batch.get_expectation_suite_name()))
	print(json.dumps(batch.get_expectation_suite(), indent=4))
	print(f"{batch.get_expectation_suite_name()} expectation suite saved.")
	
	batch.save_expectation_suite()


# TODO: add command line arguments to declare table_name, suite_name, and rule_query_type when calling in terminal
if __name__ == "__main__":
	batch_df = generate_suite_and_batch_data(table_name='claim_requests', suite_name='warnings_dec2019',
	                                         rule_query_type='create_expectations_dec2019')
	print('here')
	generate_expectations(table_name='claim_requests', batch=batch_df)
