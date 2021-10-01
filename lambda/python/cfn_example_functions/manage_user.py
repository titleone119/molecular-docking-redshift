# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This is just meant as a sample function that can show how to orchestrate tasks on Redshift via Cloudformation.
# This is by no means production ready code.

import json
import os
import boto3
from logger import logger
import cfnresponse


CFN_RESOURCE_PROPERTIES = "ResourceProperties"
CFN_OLD_RESOURCE_PROPERTIES = "OldResourceProperties"
CFN_REQUEST_TYPE = "RequestType"
CFN_REQUEST_DELETE = "Delete"
CFN_REQUEST_CREATE = "Create"
CFN_REQUEST_UPDATE = "Update"
CFN_REQUEST_TYPES = [CFN_REQUEST_CREATE, CFN_REQUEST_UPDATE, CFN_REQUEST_DELETE]
PROP_USERNAME = "username"
PROP_PASSWORD = "password"
PROP_CREATE_DB = "create_db"
PROP_CREAT_USER = "create_user"
PROP_UNRESTRICTED_SYSLOG_ACCESS = "unrestricted_syslog_access"
PROP_GROUPS = "groups"
PROP_VALID_UNTIL = "valid_until"
PROP_CONNECTION_LIMIT = "connection_limit"
PROP_SESSION_TIMEOUT = "session_timeout"
EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

lambda_client = boto3.client('lambda')


def handler(event: dict, context):
    """
    The entry point of an execution only task is to guarantee that returned object is JSON serializable.
    """
    try:
        logger.debug(json.dumps(event))
        props = event[CFN_RESOURCE_PROPERTIES]
        old_props = event.get(CFN_OLD_RESOURCE_PROPERTIES)

        request_type = event[CFN_REQUEST_TYPE]

        if request_type == CFN_REQUEST_UPDATE and props[PROP_USERNAME] != old_props[PROP_USERNAME]:
            # Username changed, Redshift doesn't allow altering user rename and other properties in a single statement
            new_username = props.pop(PROP_USERNAME)
            old_username = old_props.pop(PROP_USERNAME)
            if props == old_props:
                sql_statement = f"ALTER USER {old_username} RENAME TO {new_username};"
            else:
                # TODO: Could improve such that Cloudformation handles this case by 2 separate ALTER statements
                raise ValueError("Cannot change username and other user properties in a single operation!")
        else:
            user = DBUser.make_from_dict(props)

            if request_type == CFN_REQUEST_DELETE:
                sql_statement = user.get_drop_sql()
            elif request_type == CFN_REQUEST_CREATE:
                sql_statement = user.get_create_sql()
            elif request_type == CFN_REQUEST_UPDATE:
                sql_statement = user.get_update_sql()
            else:
                raise ValueError(f"{CFN_REQUEST_TYPE} must be one of {CFN_REQUEST_TYPES}")

        event[EVENT_SQL_STATEMENT] = sql_statement

        response = lambda_client.invoke(
            FunctionName=CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA,
            InvocationType='RequestResponse',
            Payload=json.dumps(event).encode('utf-8')
        )
        logger.info(f"Lambda returned {response}")

    except Exception as ve:
        fail_reason = f"User creation issue {ve}"
        cfnresponse.send(event, cfnresponse.FAILED, 'n/a', fail_reason)
        assert False, fail_reason


class DBUser(object):
    DEFAULT_PASSWORD = None
    DEFAULT_CREATE_DB = False
    DEFAULT_CREATE_USER = False
    DEFAULT_UNRESTRICTED_SYSLOG_ACCESS = False
    DEFAULT_VALID_UNTIL = None
    DEFAULT_CONNECTION_LIMIT = -1
    DEFAULT_SESSION_TIMEOUT = -1

    def __init__(self, name: str, password: str = None, create_db=False, create_user=False,
                 unrestricted_syslog_access=False, groups=None, valid_until=None, connection_limit=-1,
                 session_timeout=-1):
        self.check_username(name)
        self.name = name
        self.password = password
        self.create_db = create_db
        self.create_user = create_user
        self.unrestricted_syslog_access = unrestricted_syslog_access

        self.groups = groups or []
        for group_name in self.groups:
            self.check_groupname(group_name)

        self.valid_until = valid_until
        self.connection_limit = connection_limit
        self.session_timeout = session_timeout

    @classmethod
    def make_from_dict(cls, input_dict):
        if PROP_USERNAME not in input_dict:
            raise ValueError(f"Property {PROP_USERNAME} is mandatory for creating a user.")
        return cls(
            name=input_dict[PROP_USERNAME],
            password=input_dict.get(PROP_PASSWORD, cls.DEFAULT_PASSWORD),
            create_db=input_dict.get(PROP_CREATE_DB, cls.DEFAULT_CREATE_DB),
            create_user=input_dict.get(PROP_CREAT_USER, cls.DEFAULT_CREATE_USER),
            unrestricted_syslog_access=input_dict.get(PROP_UNRESTRICTED_SYSLOG_ACCESS,
                                                      cls.DEFAULT_UNRESTRICTED_SYSLOG_ACCESS),
            groups=input_dict.get(PROP_GROUPS, []),
            valid_until=input_dict.get(PROP_VALID_UNTIL, cls.DEFAULT_VALID_UNTIL),
            connection_limit=input_dict.get(PROP_CONNECTION_LIMIT, cls.DEFAULT_CONNECTION_LIMIT),
            session_timeout=input_dict.get(PROP_SESSION_TIMEOUT, cls.DEFAULT_SESSION_TIMEOUT),
        )

    def get_drop_sql(self, fail_if_not_existent=False):
        if fail_if_not_existent:
            # Default behavior of drop user statement fails with error so no if_exists_sql
            if_exists_sql = ''
        else:
            if_exists_sql = 'IF EXISTS '
        return f"DROP USER {if_exists_sql}{self.name};"

    @classmethod
    def check_username(cls, identifier):
        return cls.check_valid_identifier(identifier, 'username')

    @classmethod
    def check_groupname(cls, identifier):
        return cls.check_valid_identifier(identifier, 'groupname')

    @classmethod
    def check_valid_identifier(cls, identifier, label):
        if not (identifier.isidentifier() and len(identifier) < 128):
            raise ValueError(f"'{label}' {identifier} is not supported.")

    def _get_password_sql(self):
        if self.password is None:
            return "PASSWORD DISABLE "
        else:
            # Escape single quotes to avoid SQL injections
            if not self.password.startswith('md5'):
                doc_link = "https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html#r_CREATE_USER-parameters"
                raise ValueError("Passwords should not be passed in as plaintext use the md5 approach instead. "
                                 f"See the Redshift CREATE USER documentation ({doc_link}).")
            self.password.replace("'", "''")
            return f"PASSWORD '{self.password}' "

    @classmethod
    def _get_no(cls, boolean):
        return "" if boolean else "NO"

    def _get_create_db(self):
        return f"{self._get_no(self.create_db)}CREATEDB "

    def _get_create_user(self):
        return f"{self._get_no(self.create_user)}CREATEUSER "

    def _get_syslog_access(self):
        return f"SYSLOG ACCESS {'UNRESTRICTED' if self.unrestricted_syslog_access else 'RESTRICTED'} "

    def _get_groups(self):
        if len(self.groups) > 0:
            return f"IN GROUP {', '.join(self.groups)} "
        return ""

    def _get_valid_until(self):
        if self.valid_until is not self.DEFAULT_VALID_UNTIL:
            return f"VALID UNTIL '{self.valid_until}' "
        else:
            return ""

    def _get_connection_limit(self):
        if self.connection_limit != self.DEFAULT_CONNECTION_LIMIT:
            return f"CONNECTION LIMIT {self.connection_limit} "
        return ""

    def _get_session_timeout(self):
        if self.session_timeout != self.DEFAULT_SESSION_TIMEOUT:
            return f"SESSION TIMEOUT {self.session_timeout} "
        return ""

    def get_name(self) -> str:
        return self.name

    def get_create_sql(self) -> str:
        return f"CREATE USER {self.name} {self._get_password_sql()}{self._get_create_db()}{self._get_create_user()}" \
               f"{self._get_syslog_access()}{self._get_groups()}{self._get_valid_until()}" \
               f"{self._get_connection_limit()}{self._get_session_timeout()};"

    def get_update_sql(self) -> str:
        return f"ALTER USER {self.name} {self._get_create_db()}{self._get_create_user()}{self._get_syslog_access()}" \
               f"{self._get_password_sql()}{self._get_valid_until()}" \
               f"{self._get_connection_limit()}{self._get_session_timeout()}"
