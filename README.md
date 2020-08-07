# Great Expectations workflow with Airflow

This repository provides a general example in setting up *Great Expectations* as a data monitoring tool
against a specified datasource (i.e Relational tables, data files, etc) and using Astronomer Airflow 
to schedule the validation workflow.

## Table of Contents
1. [Introduction](#Introduction)
2. [Requirements](#requirements)
3. [Setup](#setup)
4. [Project Structure](#project-structure)
5. [Astronomer Airflow Structure Overview](#astronomer-airflow-folders--files)
6. [Great Expectations Structure Overview](#great-expectations-folders--files)

## Introduction

The purpose of this workflow is step through the project structure & setup needed to stand up great_expectations
as a data monitoring tool for any use case. Great Expectations is an open source library that sets up pipeline 
tests against batches of data using codified expectations of data behavior. Once locally tested and running, the workflow
is deployed in Airflow via Astronomer to be scheduled on a regular cadence. For the purposes of this documentation, installation of
Great Expectations and Airflow will not be covered here.

* Full description & installation of Great Expectations library, visit the project page [here](https://docs.greatexpectations.io/en/latest/intro.html#what-is-great-expectations).
* Full description & installation of Astronomer Airflow can be found [here](https://www.astronomer.io/docs/getting-started/).
    * More information on Apache Airflow framework & installation can be found on the [project page](https://airflow.apache.org/docs/stable/start.html)

## Requirements

The following should be installed or setup prior to:
* Docker (installation found [here](https://docs.docker.com/get-docker/))
* Astronomer Airflow or regular Airflow
* Great Expectations
* Python 3 virtual environment
* Relevant Python Packages
    * see _requirements.txt_ file

**Installation Notes:**
1. Astronomer Airflow should be installed from HOME directory (NOT FROM THE PROJECT directory).
2. Great Expectations should be installed in the virtual environment of the project directory path.

## Setup

After all the requirements are setup, start by creating a project directory to house
all the pieces & cd into it.

```commandline
$ mkdir <directory-name> && cd <directory-name>
```
Run the following astronomer airflow command to initialize the project
```commandline
$ astro dev init
```
This generates some skeleton files. 
```
├── dags # Where your DAGs go
│   ├── example-dag.py # An example dag that comes with the initialized project
├── Dockerfile # For Astronomer's Docker image and runtime overrides
├── include # For any other files you'd like to include
├── plugins # For any custom or community Airflow plugins
├──airflow_settings.yaml #For your Airflow Connections, Variables and Pools (local only)
├──packages.txt # For OS-level packages
└── requirements.txt # For any Python packages
```
From there, change into `include` folder and initialize great_expectations
```commandline
$ cd include && great_expectations init
```
This will create the following directory structure

```
great_expectations
    |-- great_expectations.yml
    |-- expectations
    |-- checkpoints
    |-- notebooks
    |-- plugins
    |-- .gitignore
    |-- uncommitted
        |-- config_variables.yml
        |-- documentation
        |-- validations
```
To get started with great expectations on your own, I would recommend getting familiarized with the [quick start guide](https://docs.greatexpectations.io/en/latest/tutorials/getting_started.html).
At this point, the project structure and its components and files as seen below will be explained as to their relevance in the workflow.

## Project Structure

```
├── .astro
├── .dockerignore
├── .gitignore
├── README.md
├── dags
│   └── validate_tables.py
├── include
│   └── great_expectations
│       ├── expectations
│       │   └── datasource
│       │       └── default
│       │           ├── claim_requests
│       │           │   └── warnings_dec2019.json
│       │           ├── complaint_cases
│       │           │   └── warnings_2019Q4.json
│       │           ├── provider_network
│       │           │   └── warnings_dec2019.json
│       │           ├── surveys
│       │           │   └── warnings_2019Q3-Q4.json
│       │           └── zipcodes
│       │               └── warnings.json
│       ├── ge_prod
│       │   ├── create_expectations-claim_requests.py
│       │   ├── create_expectations-complaint_cases.py
│       │   ├── create_expectations-provider_network.py
│       │   ├── create_expectations-surveys.py
│       │   ├── create_expectations-zipcodes.py
│       │   ├── ge_data_access.py
│       │   ├── queries.py
│       │   └── validate_expectations.py
│       ├── great_expectations.yml
│       ├── notebooks
│       │   ├── pandas
│       │   │   ├── create_expectations-template.ipynb
│       │   │   └── validation_playground-template.ipynb
│       │   ├── spark
│       │   │   ├── create_expectations.ipynb
│       │   │   └── validation_playground.ipynb
│       │   └── sql
│       │       ├── create_expectations.ipynb
│       │       └── validation_playground.ipynb
│       ├── plugins
│       │   └── custom_data_docs
│       │       └── styles
│       ├── templates
│       │   ├── create_expectations_template.py
│       │   └── validate_expectations_template.py
│       └── uncommitted
│           ├── config_variables.yml
│           ├── data_docs
│           │   └── local_site
│           │       ├── expectations
│           │       │   └── datasource
│           │       └── validations
│           └── validations
├── packages.txt
├── plugins
│   └── example-plugin.py
└── requirements.txt
```
### Astronomer Airflow folders & files
* `.astro` directory contains a yml file that contains the name of the project given. This is initialized when running
`astro dev init`.
* `.dockerignore` file allows you to exclude files from the context like a .gitignore file allow you to exclude files from your git repository.
* `dags` folder houses all necessary airflow dag files to be used as part of the workflow for great expectations.
    * `dags/validate_tables.py` DAG identifying where to point airflow operator logic to within the project to schedule data validation
    job.
* `include` folder houses any other files needed as part of the airflow workflow. If not great expectations logic, other processes
would be contained in this folder to have astronomer airflow point to to run DAGs.
* `packages.txt` captures all OS-level package dependencies needed for the specific workflow.
* `requirements.txt` captures any python packages needed specific to the workflow. Package versions need to specified to avoid any
conflicts between dependencies.
* `Dockerfile` houses the necessary image and any runtime overrides. It can also hold OS level or python packages needed if users prefer
to identify them here instead of in the `requirements.txt` or `packages.txt`. Depending on deployment testing, users may want consider
a Debian image over Alpine.

### Great Expectations folders & files
All the relevant files and folders can be found inside the `include/great_expectations` directory.
* `great_expectations.yml` will contain the main configuration your deployment. The configuration manages the following:
    * Datasource configurations (databases vs. data files)
    * Validation Operators
    * data stores in regards to expectations rules and validation results. Also whether they're stored locally or remotely.
    * Data Docs sites - html site configuration that houses the validation results and expectations for data sources specified.
    * Lastly to reference sensitive information, variable substitution is used `config_variables.yml` using the following syntax: `${password}`.
* `ge_prod/` folder contains all the necessary great expectations python logic specific to this workflow. Users can use another
folder structure or layout they prefer as long as path dependencies can be resolved.
    * `ge_prod/create_expectations-<datasource/table name>.py` scripts have been written to generate rules and codify expectations of data pertinent
    to that specific table or datasource. Once a user runs this script, the expectations or rules are stored in the `expectations/` folder.
    * `ge_prod./ge_data_access.py` module holds reusable functions that range from connecting to a database and outputting queried data to pandas dataframe to
    grabbing credentials needed connections
    Additionally, there other data storage minimizing functions using pandas.
    * `ge_prod/queries.py` contains all the queries pointed at specified data sources to base the creation of expectations and queries relevant to validating
    new incoming daily data.
    * `ge_prod/validate_expectations.py` contains logic to validate expectations against batches of data. CLI functionality has also 
    been added to call the validate function as part of bash script or cron job.
* The `expectations/` directory will store all your Expectations as JSON files. These are the JSON artifact files that contains rules
on what to expect of your data sources.
* The `notebooks/` houses helper notebooks that give users a sandbox environment to interact & familiarize with Great Expectations abstractions. 
* The `plugins/` holds code for any custom plugins that can customize graphics in data docs html site.
* The `templates/` folder contains templated functions that can be used as a starting point to create expectations on a data source.
Additionally, there is a validation process scripts that can be used as a starting point.
* The `uncommitted/` directory contains files and folders that shouldn't live in version control
    * `uncommitted/config_variables.yml` holds sensitive information such as database credentials and other secrets.
    * `uncommitted/data_docs` contains Data Docs relevant files that are needed to properly display data docs site. These 
    are generated from Expectations, Validation Results, and other metadata.
    * `uncommitted/validations` holds the JSON validation results generated from validating data against the Expectations.