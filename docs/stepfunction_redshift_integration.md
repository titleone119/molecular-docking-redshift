# Stepfunction Redshift integration

This documentation will detail how you can create step functions that orchestrate Redshift steps using this module.

## Step function definition

We will show an example definition of a state `SqlStep` in a step function that uses the exposed lambda function. 
The definition is mostly  boiler plate code so if you use CDK you can create useful helpers to generate defaults that
make sense for your use case (e.g. see section [Retry logic](/docs/stepfunction_redshift_integration.md#retry-logic)). 
We provide a JSON example and below it we will explain the different components. Note that `UPPER` case labels are the 
ones that should be replaced.
```json
{
  "SqlStep": {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
    "Parameters": {
      "FunctionName": "arn:aws:lambda:REGION:ACCOUNT_ID:function:FUNCTION_NAME",
      "Payload": {
        "taskToken.$": "$$.Task.Token",
        "executionArn.$": "$$.Execution.Id",
        "sqlStatement": "SQL_STATEMENT"
      }
    },
    "HeartbeatSeconds": 3600,
    "Next": "SUCCESS",
    "Catch": [
      {
         "ErrorEquals": [
            "States.Timeout"
         ],
         "Next": "TIMEOUT"
      },
      {
         "ErrorEquals": [
            "FAILED"
         ],
         "Next": "SQL_FAILURE"
      }, 
      {
        "ErrorEquals": [
          "States.ALL"
        ],
        "Next": "FAILURE"
      }
    ],
    "Retry": [
      {
        "ErrorEquals": [
          "Lambda.ServiceException",
          "Lambda.AWSLambdaException",
          "Lambda.SdkClientException"
        ],
        "IntervalSeconds": 2,
        "MaxAttempts": 6,
        "BackoffRate": 2
      },
      {
         "ErrorEquals": [
            "Lambda.TooManyRequestsException"
         ],
         "IntervalSeconds": 1,
         "MaxAttempts": 10,
         "BackoffRate": 1.5
      }
    ]
  },
  "SQL_FAILURE": {"HERE WOULD BE DEFINITION OF A STEP THAT HANDLES FAILURE DUE TO THE SQL FAILING ON REDSHIFT": "."},
  "FAILURE": {"HERE WOULD BE DEFINITION OF A STEP THAT HANDLES GENERIC FAILURES": "."},
  "SUCCESS": {"HERE WOULD BE DEFINITION OF A STEP THAT RUNS AFTER SUCCESFUL APPLYING THE SQL AGAINST REDSHIFT": "."},
  "TIMEOUT": {"HERE WOULD BE DEFINTION OF A STEP HANDLING TIMEOUT (E.G. CANCELING QUERY)": "."}
}
```
The step must by of `Type` `Task` and should await a callback which can be done by setting `Resource` to
`arn:aws:states:::lambda:invoke.waitForTaskToken`. The parameters should have:
 - `FunctionName`: Depends on deployment and can be retrieved via the `lambdaFunction` property of `SfnRedshiftTasker`
 - `Payload`: A JSON object that adheres to the api of the 
   [rs_integration_function](/lambda/python/rs_integration_function/README.md) (see that doc or other operations).
   This example does the default operation which is executing a SQL statement.

Values that you want to fine tune per state if you have multiple operations against Redshift:
- `SQL_STATEMENT`: The SQL statement that you want to run.
- `3600` (HeartbeatSeconds): How long the state will wait for feedback from the query (Note: maximum runtime is 24 hours,
  as per Amazon Redshift Data API).
- `SUCCESS` (Next): Name of the next state if the query execution succeeds.
- `SQL_FAILURE` (Catch.Next): Name of the next state if query execution fails.
- `FAILURE` (Catch.Next): Name of the next state if something else failed.
- `TIMEOUT` (Catch.Next): See [Timeout](/docs/stepfunction_redshift_integration.md#timeout)


### Retry logic
The provided Lambda function has a very short running time. By default a concurrency of 1 is allowed (configurable) 
therefore it is recommended to aggressively retry throttled requests (`Lambda.TooManyRequestsException`). For other 
exceptions retry mechanisms can be less aggressive. This is illustrated in the above example.

When utilizing CDK you can create a construct that sets defaults that match your need. The integration test code has an
example of this in [src/machines/utils.ts](/src/machines/util.ts#L8-L20)

### Timeout
You can set a time budget using the `HeartbeatSeconds` parameter. If that time has passed a `States.Timeout` exception
is thrown which can be caught in order to implement custom handling. In the above example a timeout would result in
triggering the `TIMEOUT` state.

**Handling of step timeout**

Users can manually add a `Catch` for `States.Timeout`, which gets thrown after `HeartbeatSeconds` has passed. By
catching this exception they can transition to a state for handling this scenario.

## How to use it in code
If you want to generate this via CDK you can just use the `lambdaFunction` property of the `SfnRedshiftTasker` instance
you create.

Note: below section was written against commit 13c8ac15ee24b4a62a268528b3990173bac79318 and code locations might have
changed since relative links don't allows specifying a commit hash please check that commit if links are broken.

The integration tests for this package will create a Redshift cluster [code](/src/integ.default.ts#L37-L58), a
`SfnRedshiftTasker` instance and will then be created that runs statements against this cluster 
[code](/src/integ.default.ts#L59-L68). Finally a step function is created that  calls separate stepfunctions that match 
different scenarios utilizing different operations:
 - [cancelling_statements.ts](src/machines/cancelling_statement.ts)
 - [parallel_no_concurrency](src/machines/parallel_no_concurrency.ts)
 - [polling](src/machines/polling.ts)
 - [single_failure](src/machines/single_failure.ts)
 - [single_success](src/machines/single_success.ts)
 - [success_and_fail.ts](src/machines/success_and_fail.ts)