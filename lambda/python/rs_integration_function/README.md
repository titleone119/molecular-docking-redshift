# Redshift integration function
This is a Python Lambda function that allows to interact with a Redshift cluster using the Redshift Data API. It allows
interaction with the Redshift without writing Lambda function code. The initial use case is to allow easy orchestration
of Redshift via Step functions.

## API
This function its API is defined by the structure of the events that it takes as invocations. This section documents the
supported operations and the event structures that invoke them.

### `executeStatement`

#### Event example for invocation
```yaml
action: executeSingletonStatement
sqlStatement: "call sp_my_proc(4);"
executionArn: "arn:aws:lambda:eu-west-1:012345678910:function:functionInteractingWithRS"
```

#### Detail

Executing a statement can be done via 2 actions: 
 - `executeStatement` allows concurrent executions of a statement 
 - `executeSingletonStatement` will make sure no concurrent statement with the same SQL Statement text is running. If
    there is such a concurrent statement a `ConcurrentExecution` exception is raised. This requires that the AWS Lambda 
    function is configured with a concurrency of 1 (the default)!
   
Specify the statement to be issued via the `sqlStatement` parameter.

When invoking from a place where you control call arguments it is a best practice to provide an `executionArn` and set
it to the ARN of the resources that requests the Redshift interaction. 

Stepfunctions preferably use the [`waitForTaskToken`](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html#connect-wait-token)
integration pattern. Invocation from step functions requires passing `taskToken` and `executionArn` to the payload. For
that the Context Object can be used so the `Payload` parameter will look like:
```json
{
  "action": "executeStatement",
  "sqlStatement": "...",
  "taskToken.$": "$$.Task.Token",
  "executionArn.$": "$$.Execution.Id"  
}
```
See the [stepfunction_redshift_integration](/docs/stepfunction_redshift_integration.md) documentation for a more 
elaborate example.

### `describeStatement`

#### Event example
```yaml
action: describeStatement
statementId: "000c3360-dbc6-469f-894e-e4d869b0aea9"
```

#### Detail

Describe the statement that has ID `statementId`.  The result follows the [response syntax of DescribeStatement](
https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeStatement.html#API_DescribeStatement_ResponseSyntax)
 from the Redshift Data API.

Stepfunctions can use `"statementId": "LATEST"` to describe the last `executeStatement` that passed an `executionArn`.
In that case `executionArn` must be passed as well. Example step function payload:
```json
{
  "action": "describeStatement",
  "statementId": "LATEST",
  "executionArn.$": "$$.Execution.Id"
}
```
The above is useful to follow up on a `SQL_FAILURE` exception.


### `cancelStatement`

#### Event example
```yaml
action: cancelStatement
statementId: "000c3360-dbc6-469f-894e-e4d869b0aea9"
```

#### Detail

Cancel the statement that has ID `statementId`.  The result follows the [response syntax of CancelStatement](
https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_CancelStatement.html#API_CancelStatement_ResponseElements)
from the Redshift Data API.

Stepfunctions can use `"statementId": "LATEST"` to describe the last `executeStatement` that passed an `executionArn`.
In that case `executionArn` must be passed as well. Example step function payload:
```json
{
  "action": "cancelStatement",
  "statementId": "LATEST",
  "executionArn.$": "$$.Execution.Id"
}
```
The above is useful to follow up on a `States.Timeout` exception. If you define a heartbeat using the step function you
can catch this timeout and cancel the statement if you want to make sure it doesn't keep on running on Redshift.

### `getStatementResult`

#### Event example
```yaml
action: getStatementResult
statementId: "000c3360-dbc6-469f-894e-e4d869b0aea9"
```

#### Detail

Get the results of the execution that had ID `statementId`.  The result follows the 
[response syntax of GetStatementResult](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_GetStatementResult.html#API_GetStatementResult_ResponseElements)
from the Redshift Data API.

Stepfunctions can use `"statementId": "LATEST"` to get the results of the last `executeStatement` that passed an 
`executionArn`. In that case `executionArn` must be passed as well. Example step function payload:
```json
{
  "action": "getStatementResult",
  "statementId": "LATEST",
  "executionArn.$": "$$.Execution.Id"
}
```

This operation also supports the `nextToken` attribute which indicates the starting point of the next set of responses
in a subsequent request.

## Development
For development open this directory in a separate IDE workspace as AWS Lambda will use this directory as base path for
its dependencies. Also make sure to add the layer directory to the project or PYTHONPATH.