FROM astronomerinc/ap-airflow:1.10.7-buster-onbuild
ARG ENV_TYPE=${ENV_TYPE:-""}
ENV ENV_TYPE=${ENV_TYPE}