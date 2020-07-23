import os
import sys
import json
from datetime import datetime
import great_expectations as ge

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(path)
import ge_prod.ge_data_access as gda
import ge_prod.queries as queries
import click


def validate_batch_data(table_name: str, suite_name: str):
	# TODO add logging for all print statements
	vlog = gda.get_logger()
	
	print('starting validation...')
	context = ge.data_context.DataContext(context_root_dir=path)
	print('expectation suite keys: ', context.list_expectation_suite_keys())
	
	validate_run_id = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
	vlog.info(f'validate_run_id: {validate_run_id}')
	
	data_asset_name = table_name
	normalized_data_asset_name = context.normalize_data_asset_name(data_asset_name)
	
	validate_query = queries.queries.get(table_name).get('validate')
	v_df = gda.snowflake_connector_to_df(validate_query)
	batch_kwargs = {'dataset': v_df}
	batch = context.get_batch(data_asset_name=normalized_data_asset_name, expectation_suite_name=suite_name,
	                          batch_kwargs=batch_kwargs)
	
	# getting validate_df batchId & fingerprint
	validate_batch_id = batch.batch_id
	validate_batch_fingerprint = batch.batch_fingerprint
	vlog.info(f'validate_batch_fingerprint: {validate_batch_fingerprint}')
	print('------------------------------------------------------')
	vlog.info(f'validate_batch_id: {validate_batch_id}')
	
	# validating results & viewing details
	validation_results = batch.validate(run_id=validate_run_id, catch_exceptions=True, result_format='SUMMARY')
	print(json.dumps(validation_results, indent=4))
	
	vlog.info('checking validation results status')
	
	if validation_results['success']:
		print('This batch, run_id: {0} meets all expectations from a valid batch of {1:s}'.format(validate_run_id,
		                                                                                          str(data_asset_name)))
		vlog.info(
			'This batch, run_id: {0} meets all expectations from a valid batch of {1:s}'.format(validate_run_id,
			                                                                                    str(data_asset_name)))
	else:
		print('something went wrong...')
		print('This batch, run_id: {0} does not meet all expectations of {1:s}'.format(validate_run_id, data_asset_name))
		vlog.warning(
			'This batch, run_id: {0} does not meet all expectations of {1:s}'.format(validate_run_id, data_asset_name))
		for test in validation_results['results']:
			if not test['success']:
				validation_rule = test['expectation_config']['expectation_type']
				column = test['expectation_config']['kwargs']
				print(validation_rule, column, sep='\n')
	results = context.run_validation_operator(assets_to_validate=[batch], run_id=validate_run_id,
	                                          validation_operator_name='action_list_operator')
	vlog.info(results)
	context.open_data_docs()


@click.command()
@click.argument('table_name')
@click.option('-s', '--suite_name', default='warnings_2019Q4')
def cli(table_name, suite_name):
	'''
	Run a validation for a given table and suite of rules.
	'''
	validate_batch_data(table_name, suite_name)


if __name__ == "__main__":
	cli()
