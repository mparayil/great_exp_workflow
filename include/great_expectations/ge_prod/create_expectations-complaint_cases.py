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
	master_col_names = ['COMMENTS', 'CASE_NUMBER', 'CONCLUSION', 'SUMMARY_RESULTS',
	                    'CLIENT_NUMBER', 'COMPLAINT_CASE_NUMBER', 'GROUPING',
	                    'SOURCE', 'REASON', 'DETAILS',
	                    'COMPLAINT_SUBJECT', 'COMPLAINT_TYPE', 'CREATE_TIME_EASTERN',
	                    'CREATE_TIME_UTC', 'IS_CASE_CLOSED', 'IS_EXECUTIVE_ESCALATION',
	                    'IS_EXTERNAL_COMPLAINT', 'IS_FROM_SOCIAL_MEDIA',
	                    'LAST_MODIFIED_TIME_EASTERN', 'LAST_MODIFIED_TIME_UTC', 'NAME', 'TICKET_NUMBER']
	
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
	null_cols = list(batch.isnull().sum()[batch.isnull().sum() > 0].keys())
	
	# Original columns that are supposed to have null values
	master_not_null_cols = ['COMPLAINT_CASE_NUMBER', 'SOURCE', 'CREATE_TIME_EASTERN', 'CREATE_TIME_UTC',
	                        'IS_CASE_CLOSED', 'IS_EXECUTIVE_ESCALATION', 'IS_EXTERNAL_COMPLAINT',
	                        'IS_FROM_SOCIAL_MEDIA', 'LAST_MODIFIED_TIME_EASTERN', 'LAST_MODIFIED_TIME_UTC', 'NAME']
	
	master_null_cols = ['COMMENTS', 'CASE_NUMBER', 'CONCLUSION', 'SUMMARY_RESULTS', 'CLIENT_NUMBER',
	                    'GROUPING', 'REASON', 'DETAILS', 'COMPLAINT_SUBJECT',
	                    'COMPLAINT_TYPE', 'TICKET_NUMBER']
	
	# Rule 3: checking for all columns that shouldn't be null are not
	for col in master_not_null_cols:
		print(col, batch.expect_column_values_to_not_be_null(col, result_format='BASIC'), sep="\n")
	
	# Rule 4: Checking null columns how often they should be
	# TODO: build out not null weight logic in data_access with existing function
	null_col_weights = {'COMMENTS': 0.99, 'CASE_NUMBER': 0.94, 'CONCLUSION': 0.17, 'SUMMARY_RESULTS': 0.17,
	                    'CLIENT_NUMBER': 0.99, 'GROUPING': 0.17, 'REASON': 0.17,
	                    'DETAILS': 0.03, 'COMPLAINT_SUBJECT': 0.15, 'COMPLAINT_TYPE': 0.97,
	                    'TICKET_NUMBER': 0.93}
	for col, weight in null_col_weights.items():
		print(col,
		      batch.expect_column_values_to_not_be_null(col, weight, catch_exceptions=True, result_format='SUMMARY'),
		      sep='\n')
	
	# Rule 5: Expecting column values to be in set (null values not included in checks)
	# TODO: function to infer cat cols and vals in ge_data_access
	master_category_column_values = {
		'CONCLUSION': ['Credit Denied', 'Escalated to Legal - Demand paid in full', 'Verbal Apology',
		                    'Voicemail Apology', 'Escalated to Legal', 'Proof of service/receipt issued',
		                    'Not a Damage - Sent Final Resolution', 'Claim Paid - Sent Final Resolution',
		                    'DMS - renewal conversion letter sent', 'Damage Repaired - Sent Final Resolution',
		                    'Goodwill – Sent Final Resolution', 'Pending - Sent Final Resolution',
		                    'Compensation for incurred costs', 'PVT Credit Denied',
		                    'Provided client summary/resolution', 'PVT Credit Paid', 'Closed- IVR no contact',
		                    'Rental expense Paid - Partial', 'Claim Paid - Full', 'Rental expense Paid - Full',
		                    'Sent Final Resolution', 'Pending - Customer Documents', 'Goodwill provided',
		                    'No Customer Contact', 'Reimbursement for RS services - Denied',
		                    'Reimbursement for RS services - Paid', 'Text/Email apology', 'Damage Claim Withdrawn',
		                    'Deferred - Third Party Claim', 'Claim Denied', 'DVR - Not a Damage',
		                    'Re-Submit Customer Documents', 'Claim Paid - Partial',
		                    'Reimbursement - Sent Final Resolution', 'Hard copy mail apology', 'Credit Issued',
		                    'Compliment', 'Escalated to Legal - Response Completed', 'No Action Required',
		                    'Escalated to Legal - Negotiated Settleme', 'Customer Replied',
		                    'Damage settled with Insurance'],
		'GROUPING': ['Missing vehicle', 'System issue', 'Impound', 'Inquiry', 'Damage', 'PVT', 'Legal Claim',
		                       'Contact Center', 'Program process', 'Network', 'Compliment'],
		'SOURCE': ['contact center', 'acct manager', 'command center', 'customer care', 'client', 'web',
		                     'survey', 'bbb/ag', 'client portal', 'customer', 'oneroad dvr', 'external',
		                     'dvr application', 'ivr survey', 'pvt', 'internal', 'alert', 'text survey', 'intellra dvr',
		                     'ncc', 'service provider', 'spst', 'cair'],
		'REASON': ['Missed ETA', 'Did not perform service', 'Client reimbursement', 'Missing vehicle',
		                     'Missed ETA - Location issue', 'Steering', 'Did not perform service - Incorrect service',
		                     'Script/Procedures - Coverage related', 'Stolen/Missing Items',
		                     'Missing Vehicle - Storage', 'Credit request - Overages/Charges denied', 'Credit request',
		                     'Credit request - Service unsuccessful', 'Performance', 'Input error',
		                     'Customer Service - Rude/Tone', 'Did not perform service - Capacity/priority',
		                     'Mishandled Coverage/Overages', 'Missed ETA - Weather/Traffic',
		                     'Input error - Customer/Coverage/Vehicle information',
		                     'Long ETA - SP Coverage/Availability', 'Assault', 'Fatality', 'Charge back',
		                     'Unprofessional', 'Queue time', 'IVR/text/Email/Fax', 'Long ETA - Location issue',
		                     'Input error - VIN/Policy/Callback', 'Coverage', 'Mobile app',
		                     'Did not perform service - SP Cancelled', 'Script/Procedures- Dispatch related',
		                     'Missed ETA - Equipment issue', 'Customer Service - Empathy/Attentive listening',
		                     'Script/Procedures - Reimbursement related', 'Coverage - Incorrect processing',
		                     'Sexual Harassment', 'Did not perform service - Restricted roadway', 'DVR Process Error',
		                     'Credit request - Customer cancel', 'Damage', 'Did not perform service - Equipment',
		                     "Customer doesn't have information needed", 'SP appearance',
		                     'Input error - Disablement/Tow to location', 'Script/Procedures - Overage related',
		                     'Bodily Injury', 'Service Provider', 'Threats', 'Long ETA - Weather/Traffic',
		                     'Audio request', 'Lawsuit Filed/Demand Letters', 'Credit request - Authorization Only',
		                     'Better Business Bureau Complaint', 'Subpoena Requests', 'Customer perception',
		                     'Missed ETA - Delay/Capacity', 'Call history',
		                     'Did not perform service - No service available', 'Coverage - System issue',
		                     'Contact Center', 'Harassment', 'Discrimination', 'Sexual Assault', 'Agent',
		                     'Social Media', 'Billing', 'Script/Procedures', 'Input error - Problem/Service',
		                     'SP steering', 'Customer Service - Unprofessional conduct', 'Coverage - Coverage limits',
		                     'Customer perception - Case handling', 'Script/Procedures - Location related',
		                     'Data Call/Data Failure', 'Missing Vehicle - Unknown',
		                     'Service Provider Background Complaint', 'Credit request - No service completed',
		                     'Customer perception - Dispatch process', 'Did not perform service - Knowledge related',
		                     'Missing Vehicle - Incorrect tow location', 'Queue time - Long wait times',
		                     'Billing - Client requested', 'Receipt request', 'Check Fraud',
		                     'Law Enforcement Inquiries', 'Attorney General Complaint',
		                     'Department of Insurance Complaint'],
		'DETAILS': ['Overages', '< 60 mins', 'Did not say evaluation of reimbursement', 'Dispatch fee',
		                             'Called wrong roadside number', 'SP pushed customer to certain tow-to location',
		                             '> 120 mins', 'Extended ETA multiple times',
		                             'Customer not satisfied with coverage limits', 'Dispatch procedures not followed',
		                             '60-120 mins', 'Rude', 'Did not know how to complete job', 'Client system down',
		                             'Stored vehicle without notification', 'Vehicle found at tow-to location',
		                             'Restricted roadway', 'Received priority call', 'Customer canceled',
		                             'Locking lug nuts', 'Vehicle delivered to wrong location', 'Out of area',
		                             'Performed other service', 'Coded weather event', 'Late delivery',
		                             'Agero system down', 'Service not completed', 'Customer perception', 'Harassment',
		                             'Agent error', '>2 hrs, remediation', '<= 2 hrs', 'Lack of equipment',
		                             'Customer not aware of coverage', 'Policy/warranty coverage', '> 2 hrs', 'No show',
		                             'Service unsuccessful'],
		'COMPLAINT_TYPE': ['Contact Center Agent Complaint', 'Coverage Limits/Overages', 'Mechanical',
		                   'Training Exercise – No Damage', 'Body Damage - Grill/Hood', 'Drive Shaft',
		                   'Program Guidelines Complaint', 'Missing Key', 'Rear Hatch/Trunk wont clo', 'Lockout Damage',
		                   'Body Damage - Rear Bumper', 'Wheel/Tire - Hub', 'Interior - Floor',
		                   'Wheels/Tire - Tire/Rim', 'Body Damage', 'Battery/Electrical',
		                   'Wheels/Tire - Hub Cap/Lug Nut', 'Transmission', 'Body Damage - Lights', 'Chassis/Steering',
		                   'Property Damage - Structure', 'Bumper', 'Service Provider Complain', 'Missed ETA Complaint',
		                   'Mechanical - Brakes', 'Missing/Damaged Key', 'Engine', 'Coverage Limits/Overages Complaint',
		                   'Fuel Tank', 'Glass - Window', 'Interior', 'Exhaust', 'Threatening Legal Action', 'Glass',
		                   'Requests Proof of Service', 'Body Damage - Door', 'Interior - Staining',
		                   'Contact Center Agent Comp', 'No Damage', 'Body Damage - Roof', 'Chassis/Steering - Tie Rod',
		                   'Interior – Gear Shifter', 'Body Damage - Mirror', 'Glass - Windshield',
		                   'Interior – Steering Column', 'Service Provider Complaint', 'Lock',
		                   'Body Damage - Bumper/Fender/Panel', 'Property Damage - Yard', 'Wheels/Tire - Equipment',
		                   'Property Damage', 'Chassis/Steering - Control Arm', 'Body Damage - Frame',
		                   'Body Damage - Fender/Quarter Panel', 'Battery/Electrical - ECM/Fuse',
		                   'Program Guidelines  Compl', 'Body Damage - Scratches', 'No Damage - Stolen Property',
		                   'Body Damage - Undercarriage', 'Interior - Ignition', 'Engine - Oil Pan',
		                   'Body Damage -Front Bumper']}
	for col, val_set in master_category_column_values.items():
		print(col, batch.expect_column_values_to_be_in_set(col, val_set, result_format='SUMMARY',
		                                                   include_config=True, catch_exceptions=True), '\n')
	
	# Rule 6: Determine if columns to be unique - [('CASE_NUMBER', 'TICKET_NUMBER'), 'COMPLAINT_CASE_NUMBER']
	print(batch.expect_column_values_to_be_unique('COMPLAINT_CASE_NUMBER', result_format='SUMMARY',
	                                              catch_exceptions=True))
	print(batch.expect_multicolumn_values_to_be_unique(column_list=('CASE_NUMBER', 'TICKET_NUMBER'), result_format='SUMMARY',
	                                                   catch_exceptions=True, include_config=True), sep='\n')
	
	# Rule 9: Looking at column A being larger than column B
	# LAST_MODIFIED_TIME_UTC > CREATE_TIME_UTC
	batch.expect_column_pair_values_A_to_be_greater_than_B('LAST_MODIFIED_TIME_UTC', 'CREATE_TIME_UTC',
	                                                       ignore_row_if='either_value_is_missing', or_equal=True,
	                                                       result_format='SUMMARY', catch_exceptions=True)
	# Rule 10: Looking at columns to be of said data types
	customer_experience_data_types = {'COMPLAINT_CASE_NUMBER': 'object', 'CASE_NUMBER': 'float64', 'TICKET_NUMBER': 'float64',
	                                  'GROUPING': 'object', 'REASON': 'object',
	                                  'DETAILS': 'object', 'SOURCE': 'object',
	                                  'IS_EXTERNAL_COMPLAINT': 'bool', 'COMMENTS': 'object',
	                                  'IS_CASE_CLOSED': 'bool', 'CONCLUSION': 'object', 'SUMMARY_RESULTS': 'object',
	                                  'IS_EXECUTIVE_ESCALATION': 'bool', 'IS_FROM_SOCIAL_MEDIA': 'bool',
	                                  'NAME': 'object', 'COMPLAINT_TYPE': 'object', 'COMPLAINT_SUBJECT': 'object',
	                                  'CLIENT_NUMBER': 'float64', 'CREATE_TIME_EASTERN': 'datetime64[ns]',
	                                  'CREATE_TIME_UTC': 'datetime64[ns]',
	                                  'LAST_MODIFIED_TIME_EASTERN': 'datetime64[ns]',
	                                  'LAST_MODIFIED_TIME_UTC': 'datetime64[ns]'}
	for col, typ in customer_experience_data_types.items():
		print(col, batch.expect_column_values_to_be_of_type(col, typ, result_format='SUMMARY', catch_exceptions=True),
		      sep='\n')
	
	print('validation rules captured for {} suite: '.format(batch.get_expectation_suite_name()))
	print(f"{batch.get_expectation_suite_name()} expectation suite saved.")
	
	batch.save_expectation_suite()


# TODO: add command line arguments to declare table_name, suite_name, and rule_query_type when calling in terminal
if __name__ == "__main__":
	batch_df = generate_suite_and_batch_data(table_name='complaint_cases', suite_name='warnings_2019Q4',
	                                         rule_query_type='create_expectations_2019Q4')
	print('here')
	generate_expectations(table_name='complaint_cases', batch=batch_df)
