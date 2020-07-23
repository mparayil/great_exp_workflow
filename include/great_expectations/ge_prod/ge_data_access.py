import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import engine
from snowflake.sqlalchemy import URL
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
import yaml
import logging
from great_expectations.dataset import PandasDataset
from typing import Union

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def load_credentials(config_path=path + '/uncommitted/config_variables.yml', key_val='snowflake_datasource'):
	with open(config_path, 'r') as f:
		try:
			return yaml.safe_load(f)[key_val]
		except yaml.YAMLError as exc:
			print(exc)


def get_snowflake_sqlalchemy_eng() -> engine:
	sf_creds = load_credentials()
	eng = create_engine(URL(
		user=sf_creds['username'],
		password=sf_creds['password'],
		account=sf_creds['host'],
		database=sf_creds['database'],
		schema=sf_creds['schema'],
		warehouse=sf_creds['warehouse'],
		role=sf_creds['role']
	))
	return eng


def get_snowflake_connector():
	sf_creds = load_credentials()
	ctx = snowflake.connector.connect(
		user=sf_creds['username'],
		password=sf_creds['password'],
		account=sf_creds['host'],
		database=sf_creds['database'],
		schema=sf_creds['schema'],
		warehouse=sf_creds['warehouse'],
		role=sf_creds['role'],
		converter_class=SnowflakeNoConverterToPython
	)
	return ctx


def snowflake_connector_to_df(query: str) -> pd.DataFrame:
	sf_creds = load_credentials()
	ctx = snowflake.connector.connect(
		user=sf_creds['username'],
		password=sf_creds['password'],
		account=sf_creds['host'],
		database=sf_creds['database'],
		schema=sf_creds['schema'],
		warehouse=sf_creds['warehouse'],
		role=sf_creds['role'],
		converter_class=SnowflakeNoConverterToPython
	)
	cur = ctx.cursor()
	try:
		cur.execute(query)
		df = cur.fetch_pandas_all()
	finally:
		cur.close()
	return df


def get_logger():
	logger = logging.getLogger('ge_prod')
	
	if logger.hasHandlers():
		logger.handlers.clear()
		
	sh = logging.StreamHandler()
	lf = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	sh.setFormatter(lf)
	logger.addHandler(sh)
	logger.setLevel(logging.DEBUG)
	
	return logger


def query_to_df(conn_eng, query_string, params=None) -> pd.DataFrame:
	"""Converts snowflake_query to pandas dataframe using sqlalchemy connection.execute() method.
    
    Parameters
    -----------
    conn_eng: engine
        engine object used to connect to snowflake
    query_string: str
        query used to return dataframe
    params: dict
        dictionary of kwargs that adds values for bind parameters

    Returns
    ------------
    pd.DataFrame
        pandas dataframe
    """
	if params is None:
		results = conn_eng.execute(query_string)
		df = pd.DataFrame(results.fetchall(), columns=results.keys())
		return df
	else:
		results = conn_eng.execute(query_string, params)
		df = pd.DataFrame(results.fetchall(), columns=results.keys())
		return df


def snowflake_sqlalchemy_to_df(eng, query: str, parse_date_cols=None, chunk_size=None):
	"""Converts snowflake_query to pandas dataframe using pd.read_sql().
    Parameters
    -----------
    eng: engine
        engine object used to connect to snowflake
    query: str
        query used to return dataframe
    parse_date_cols: str
        column names to be passed to parse as datetime objects
    chunk_size: int
        TextFileReader object for iteration to process file in chunks
        
    Returns
    ------------
    pd.DataFrame
        pandas dataframe
    """
	conn = eng.connect()
	chunk_list = []
	if parse_date_cols is None or chunk_size is None:
		for chunk in pd.read_sql(query, conn, chunksize=10000):
			chunk_list.append(chunk)
	df = pd.concat(chunk_list)
	conn.close()
	eng.dispose()
	return df


# TODO expand out to get data subtypes and cast
def get_dataframe_dtypes(df: pd.DataFrame) -> dict:
	dtypes = df.dtypes
	col_names = dtypes.index
	types = [i.name for i in dtypes.values]
	col_types = dict(zip(col_names, types))
	return col_types


def get_df_not_null_weights(df: Union[pd.DataFrame, PandasDataset], groupby_col: str, not_null_col: str) -> float:
	"""
	Provides specified column's weight/percentage for it not to be null.

	Parameters
	-----------
	df: pd.DataFrame or great_expectations.dataset.PandasDataset
		dataframe object to look at
	groupby_col: str
		grouping column string to groupby dataframe on when looking at specified column in next parameter
	not_null_col: str
		column used from dataframe to calculate safe weight thresholds of when it would be not null

	Returns
	------------
	float
		Not null weight of specified column lowered by 5% after looking at the 10% quartile
	"""
	
	df_group = df.groupby(df[groupby_col].dt.date)
	df_group = df_group.apply(lambda x: x[not_null_col].notnull().mean())

	base_weight = df_group.quantile(0.1, interpolation='midpoint')
	adjusted_weight = (base_weight - 0.009)
	if adjusted_weight < 0.005:
		final_weight = base_weight.round(4)
	else:
		final_weight = adjusted_weight.round(4)
	return float(final_weight)


def get_categorical_columns_values(df: Union[pd.DataFrame, PandasDataset], cols: list, table_name: str) -> dict:
	"""
	Yields a dictionary of columns and their distinct values for a given table.

	Parameters
	-----------
	df: pd.DataFrame or great_expectations.dataset.PandasDataset
		dataframe object to look at
	cols: list
		list of columns from specified table
	table_name: str
		table used to get distinct values from

	Returns
	------------
	dict
		the unique value set for a given column chose. Boolean columns are filtered out as well.
	"""
	
	c_weights = {}
	for col in cols:
		unique_weights = df[col].value_counts(normalize=True) * 100
		c_weights[col] = unique_weights.values.mean().round(5)
	
	cat_weight_dict = {c: w for (c, w) in c_weights.items() if w > 0.9 if df[c].dtypes != bool
	                   if c not in ['TASK_ID', 'task_id', 'CLIENT_ID', 'client_id', 'equipment_count', 'EQUIPMENT_COUNT',
	                                'BILL_GROUP_ID', 'bill_group_id']}
	
	execute_strings = ' '.join(f"SELECT DISTINCT {c_name} FROM {table_name};" for c_name in cat_weight_dict.keys())
	ctx = get_snowflake_connector()
	
	cursor_list = ctx.execute_string(execute_strings, remove_comments=True, return_cursors=True)
	category_col_values = {}
	for cur in cursor_list:
		col_names = ','.join([col[0] for col in cur.description])
		cat_values = [x[0] for x in cur.fetchall() if x[0]]
		category_col_values[col_names] = cat_values
	return category_col_values
