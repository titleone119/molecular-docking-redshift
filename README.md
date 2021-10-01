## Welcome to the cdk-stepfunctions-redshift project!

`cdk-stepfunctions-redshift` provides `SfnRedshiftTasker` which is a JSII construct library to build AWS Serverless
infrastructure to implement a callback pattern for Amazon Redshift statements.

The `SfnRedshiftTasker` construct will take details of a Redshift target (clustername, database name & username) and 
the resulting object will have a `lambdaFunction` property which will provide the interface to run statements against
that Redshift target with callback functionality.

The current solution supports implementing 2 callback patterns:
- Step functions: have a state that issues a SQL statement and have it only transition once the statement succeeds or 
  fails (see [stepfunction_redshift_integration.md](/docs/stepfunction_redshift_integration.md) for how to use this)
- Cloud formation: After running the SQL statement do a callback to signal cloudformation success or failure (see
  [cloudformation_redhsift_integration.md](/docs/cloudformation_redshift_integration.md) for how to use this)


## Behind the scenes

When you use a `SfnRedshiftTasker` in your stack you will get:
- A Lambda function for invoking tasks on the Amazon Redshift cluster
- A DDB Table to track ongoing-executions
- An Event rule to monitor Amazon Redshift Data API completion events and route them to SQS
- An SQS queue to receive above mentioned Amazon Redshift Data API completion events
- A Lambda function to process API Completions events (by default same function as the one above)
- A KMS key which encrypts data at rest.

This allows to easily create step-function tasks which execute a SQL command and will only complete
once Amazon Redshift finishes executing the corresponding statement.

### How it works
Serverless infrastructure will be spawn up for a specific (cluster, user, database). A Lambda function will be provided
which allows invoking statements as this user.  States can then be used to do a seemingly synchronous invocation of a
Amazon Redshift statement allowing your statemachines to have a simpler definition (see
[Example definition](/docs/stepfunction_redshift_integration.md#step-function-definition) for an example state).

#### Example flow (for step function)
![alt text](images/aws-step-function-redshift-integration.png?raw=1 "Visualization completion.")

1. A step-function step triggers the Lambda function provided by the construct. The step function step follows a
   structure for its invocation payload which includes designated fields (following the [API of the invoker function](
   /lambda/python/rs_integration_function/README.md))

2. The Lambda function will generate a unique ID based on the execution ARN and register the SQL invocation in a
   DynamoDB state table.

3. The lambda function then starts the statement using the Amazon Redshift data API using the Unique ID as statement
   name and requesting events for state changes.

4. As a result of step 3 Amazon Redshift executes the statement. Once that statement completes it emits an event. Our
   building blocks have put in place a Cloudwatch Rule to monitor these events.

5. The event gets placed into an SQS queue

6. This SQS queue is monitored by a Lambda function (could be the same as the previous one).

7. The Lambda function will check whether the finished query is related to a step function invocation in order to
   retrieve the task token of the step.

8. If it is then it will do a succeed/fail callback to the step-function step (using the task token) depending on
   success/failure of the SQL statement.

9. It will mark the invocation as processed in the state table.

## How to use
This is a construct so you can use it from a CDK Stack. An example stack can be found at [integ.default.ts](src/integ.default.ts)
.  That stack sets up an Amazon Redshift cluster, the `SfnRedshiftTasker` infra and some state machines that use the
functionality. It can be launched by compiling the code (which creates a lib directory) and deploying the CDK app:
`yarn compile && npx cdk --app ./lib/integ.default.js deploy`

### Considerations
When using this approach do keep in mind the considerations of the [Amazon Redshift Data API](
https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html#data-api-calling-considerations).

These shouldn't be blockers:
- If query result is too big consider using `UNLOAD` rather than `SELECT`.
- If the statement size is too big consider splitting up the statement in multiple statements. For example by
  defining and utilizing views or encapsulating the logic in a stored procedure.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
