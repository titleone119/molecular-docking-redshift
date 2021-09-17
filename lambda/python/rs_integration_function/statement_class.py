# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import os
from datetime import datetime, timedelta
from uuid import uuid4


class StatementName(object):
    """
    We can use the SFN execution ARN as statement name if the invocation comes from a stepfunction.
    We cannot make assumptions about the partition but we can check the ARN and SERVICE parts of the ARN.
    For non-step function invocations an ARN of the invoker can be provided if nothing is provided then default to an
    ARN like arn:::{region}::custom_invocation:{uuid4}
    """
    ARN_IDX = 0
    PARTITION_IDX = 1
    SERVICE_IDX = 2
    REGION_IDX = 3
    ACCOUNT_IDX = 4
    ACTION_IDX = 5
    MACHINE_NAME_IDX = 6
    EXECUTION_ID_IDX = 7
    INVOCATION_ID_IDX = 8  # Last element will be the invocaiton id.
    LAST_ELEMENT_IDX = -1

    ARN = "arn"
    STATES = "states"
    EXECUTION_ACTION = "execution"

    class NoSfnStatementName(Exception):
        pass

    @classmethod
    def generate_id(cls) -> str:
        return str(datetime.utcnow().timestamp())

    @classmethod
    def _invocation_id_to_datetime(cls, invocation_id):
        return datetime.fromtimestamp(float(invocation_id))

    def invocation_id_to_datetime(self):
        return self._invocation_id_to_datetime(self.invocation_id)

    @classmethod
    def is_id(cls, candidate: str) -> bool:
        try:
            invocation_dt = cls._invocation_id_to_datetime(candidate)
            one_year = timedelta(weeks=52)
            now = datetime.utcnow()
            return now - one_year < invocation_dt < datetime.utcnow() + one_year
        except ValueError:
            return False

    @classmethod
    def _is_sfn_invocation(cls, statement_instance: str) -> bool:
        arn_parts = statement_instance.split(':')
        try:
            return all([
                arn_parts[cls.ARN_IDX] == cls.ARN,
                arn_parts[cls.SERVICE_IDX] == cls.STATES,
                arn_parts[cls.ACTION_IDX] == cls.EXECUTION_ACTION,
                cls.is_id(arn_parts[cls.INVOCATION_ID_IDX])
            ])
        except IndexError:
            return False

    def is_sfn_invocation(self):
        return self._is_sfn_invocation(str(self))

    def __init__(self, execution_arn: str, invocation_id: str):
        self.execution_arn = execution_arn
        assert self.is_id(invocation_id), f"Invalid invocation_id {invocation_id}"
        self.invocation_id = invocation_id

    @classmethod
    def from_str(cls, statement_name: str):
        statement_name_parts = statement_name.split(':')
        return cls(
            execution_arn=':'.join(statement_name_parts[:cls.LAST_ELEMENT_IDX]),
            invocation_id=statement_name_parts[cls.LAST_ELEMENT_IDX]
        )

    def __str__(self):
        return f"{self.execution_arn}:{self.invocation_id}"

    @classmethod
    def from_execution_arn(cls, execution_arn: str):
        if execution_arn is None:
            """Adhoc invocation"""
            region = os.environ['AWS_REGION']
            execution_arn = f"arn:::{region}::custom_invocation:{str(uuid4())}"
        return cls(
            execution_arn=execution_arn,
            invocation_id=cls.generate_id()
        )
