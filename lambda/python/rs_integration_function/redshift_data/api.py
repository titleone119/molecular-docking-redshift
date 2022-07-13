# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import os

import boto3

from environment_labels import CLUSTER_IDENTIFIER, DATABASE, DB_USER
from logger import logger, l_id, l_next_token, l_statement_name, l_response

redshift_data_api = boto3.client('redshift-data')


def describe_statement(statement_id: str) -> dict:
    return redshift_data_api.describe_statement(Id=statement_id)


def get_statement_result(statement_id: str, next_token=None) -> dict:
    extra_args = {}
    if next_token is not None:
        extra_args["NextToken"]: next_token
    logger.debug({
        l_id: statement_id,
        l_next_token: next_token
    })
    return redshift_data_api.get_statement_result(Id=statement_id, **extra_args)


def cancel_statement(statement_id: str) -> dict:
    return redshift_data_api.cancel_statement(Id=statement_id)


def is_statement_in_active_state(query_string):
    active_states = ["SUBMITTED", "PICKED", "STARTED"]
    for state in active_states:
        response = redshift_data_api.list_statements(Status=state)
        for statement in response["Statements"]:
            if statement["QueryString"] == query_string:
                return True
    return False


def get_statement_id_for_statement_name(statement_name: str) -> str:
    response = redshift_data_api.list_statements(Status='ALL', StatementName=statement_name)
    logger.debug({l_statement_name: statement_name, l_response: response})
    statements = response["Statements"]
    assert len(statements) == 1, f"Should retrieve 1 result for {statement_name} got {statements}"
    return statements[0]["Id"]


def execute_statement(sql_statement: str, statement_name: str, with_event: bool, params = None) -> dict:
    '''
        Parameters=[
            {
                'name': 'string',
                'value': 'string'
            },
        ],
    '''
    
    if params is None:
        return redshift_data_api.execute_statement(
            ClusterIdentifier=os.environ[CLUSTER_IDENTIFIER],
            Database=os.environ[DATABASE],
            DbUser=os.environ[DB_USER],
            Sql=sql_statement,
            StatementName=statement_name,
            WithEvent=with_event  # When invoked from SFN with s task token we invoke using withEvent enabled.
        )
    else:
        
        for pair_dict in params:
            name = pair_dict['name']
            value = pair_dict['value']
            sql_statement = sql_statement.replace(':' + name, "'" + value + "'")
            
        logger.info("updated sql: " + sql_statement)
        
        return redshift_data_api.execute_statement(
            ClusterIdentifier=os.environ[CLUSTER_IDENTIFIER],
            Database=os.environ[DATABASE],
            DbUser=os.environ[DB_USER],
            Sql=sql_statement,
            StatementName=statement_name,
            WithEvent=with_event  # When invoked from SFN with s task token we invoke using withEvent enabled.
        )