queries = {
	"provider_network": {
		"create_expectations_2019Q4": "SELECT * FROM provider_network WHERE dispatch_request_time_utc >= "
		                              "to_date('2019-10-01') and dispatch_request_time_utc <= to_date("
		                              "'2019-12-31');",
		"create_expectations_dec2019": "SELECT * FROM provider_network WHERE dispatch_request_time_utc >= "
		                               "to_date('2019-12-01') and dispatch_request_time_utc <= to_date("
		                               "'2019-12-31');",
		"validate": "SELECT * FROM provider_network WHERE dispatch_request_time_utc >= cast("
		            "current_date() -1 AS TIMESTAMP) AND dispatch_request_time_utc <= DATEADD(SECOND, "
		            "-1, CAST(current_date() AS TIMESTAMP));"},
	"claim_requests": {
		"create_expectations_2019Q4": "SELECT * FROM claim_requests WHERE modified_date_utc >= "
		                              "to_date('2019-10-01') and modified_date_utc <= to_date("
		                              "'2019-12-31');",
		"create_expectations_dec2019": "SELECT * FROM claim_requests WHERE modified_date_utc >= "
		                               "to_date('2019-12-01') and modified_date_utc <= to_date("
		                               "'2019-12-31');",
		"validate": "SELECT * FROM claim_requests WHERE modified_date_utc >= cast(current_date() -1 AS TIMESTAMP)"
		            "AND modified_date_utc <= DATEADD(SECOND, -1, CAST(current_date() AS TIMESTAMP));"
	},
	"surveys": {
		"create_expectations_2019Q4": "SELECT * FROM surveys WHERE survey_time_utc >= to_date('2019-10-01')"
		                              "and survey_time_utc <= to_date('2019-12-31');",
		"create_expectations_dec2019": "SELECT * FROM surveys WHERE survey_time_utc >= "
		                               "to_date('2019-12-01') and survey_time_utc <= to_date('2019-12-31');",
		"create_expectations_2019Q3-Q4": "SELECT * FROM surveys WHERE survey_time_utc >= to_date('2019-07-01')"
		                                 "and survey_time_utc <= to_date('2019-12-31');",
		"validate": "SELECT * FROM surveys where survey_time_utc >= cast(current_date() "
		            "-1 AS TIMESTAMP) AND survey_time_utc <= DATEADD(SECOND, -1, CAST(current_date()"
		            "AS TIMESTAMP));"
	},
	"complaint_cases": {
		"create_expectations_2019Q4": "SELECT * FROM complaint_cases where last_modified_time_utc >= to_date("
		                              "'2019-10-01') and last_modified_time_utc <= to_date('2019-12-31');",
		"create_expectations_dec2019": "SELECT * FROM complaint_cases where last_modified_time_utc >= to_date("
		                               "'2019-12-01') and last_modified_time_utc <= to_date('2019-12-31');",
		"create_expectations_2019Q3-Q4": "SELECT * FROM complaint_cases where last_modified_time_utc >= to_date("
		                                 "'2019-07-01') and last_modified_time_utc <= to_date('2019-12-31');",
		"validate": "SELECT * FROM complaint_cases where last_modified_time_utc >= cast(current_date() "
		            "-1 AS TIMESTAMP) AND last_modified_time_utc <= DATEADD(SECOND, -1, CAST(current_date()"
		            "AS TIMESTAMP));"
	},
	"zipcodes": {"create_expectations": "select * from zipcodes;",
	             "validate": "select * from zipcodes sample (10);"},
}
