// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as assert from 'assert';
import * as path from 'path';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as iam from '@aws-cdk/aws-iam';
import * as kms from '@aws-cdk/aws-kms';
import * as lambda from '@aws-cdk/aws-lambda';
import * as lambdaPython from '@aws-cdk/aws-lambda-python';
import * as logs from '@aws-cdk/aws-logs';
import * as sam from '@aws-cdk/aws-sam';
import * as sqs from '@aws-cdk/aws-sqs';
import * as cdk from '@aws-cdk/core';
import { EventbridgeToSqs } from '@aws-solutions-constructs/aws-eventbridge-sqs';
import { LambdaToDynamoDB } from '@aws-solutions-constructs/aws-lambda-dynamodb';
import { SqsToLambda } from '@aws-solutions-constructs/aws-sqs-lambda';

/**
 * The details of the Redshift target in which you will execute SQL statements.
 */
export interface RedshiftTargetProps {
  /**
   * The Redshift database user that will execute the Redshift tasks.
   */
  readonly dbUser: string;
  /**
   * The Redshift database name in which the SQL statement will be executed.
   */
  readonly dbName: string;
  /**
   * The cluster identifier (name) in which the SQL statement will be executed. This is the part of the FQDN up to the
   * first '.'.
   */
  readonly clusterIdentifier: string;
}

/**
 * @summary The properties for the Construct
 */
export interface SfnRedshiftTaskerProps {
  /**
   * The details of the Redshift target in which you will execute SQL statements.
   */
  readonly redshiftTargetProps: RedshiftTargetProps;
  /**
   * Existing instance of SQS queue object, if this is set then the queueProps is ignored.
   *
   * @default - None
   */
  readonly existingQueueObj?: sqs.Queue;
  /**
   * User provided props to override the default props for the SQS queue.
   *
   * @default - Default props are used
   */
  readonly queueProps?: sqs.QueueProps;
  /**
   * Whether to grant additional permissions to the Lambda function enabling it to purge the SQS queue.
   *
   * @default - "false", disabled by default.
   */
  readonly enableQueuePurging?: boolean;
  /**
   * Optional user provided properties for the dead letter queue
   *
   * @default - Default props are used
   */
  readonly deadLetterQueueProps?: sqs.QueueProps;
  /**
   * Whether to deploy a secondary queue to be used as a dead letter queue.
   *
   * @default - true.
   */
  readonly deployDeadLetterQueue?: boolean;
  /**
   * The number of times a message can be unsuccessfully dequeued before being moved to the dead-letter queue.
   *
   * @default - required field if deployDeadLetterQueue=true.
   */
  readonly maxReceiveCount?: number;
  /**
   * Use a KMS Key, either managed by this CDK app, or imported. If importing an encryption key, it must be specified in
   * the encryptionKey property for this construct.
   *
   * @default - true (encryption enabled, managed by this CDK app).
   */
  readonly enableEncryptionWithCustomerManagedKey?: boolean;
  /**
   * An optional, imported encryption key to encrypt the SQS queue, and SNS Topic.
   *
   * @default - not specified.
   */
  readonly encryptionKey?: kms.Key;
  /**
   * Optional user-provided props to override the default props for the encryption key.
   *
   * @default - Default props are used.
   */
  readonly encryptionKeyProps?: kms.KeyProps;
  /**
   * Existing instance of Lambda Function object that starts execution, if this is set then the lambdaFunctionProps is
   * ignored.
   *
   * @default - None
   */
  readonly starterExistingLambdaObj?: lambda.Function;
  /**
   * User provided props to override the default props for the Lambda function that starts execution.
   *
   * @default - Default props are used
   */
  readonly starterLambdaFunctionProps?: lambda.FunctionProps;
  /**
   * Existing instance of Lambda Function object that completes execution, if this is set then the
   * completerLambdaFunctionProps is ignored.
   *
   * @default - None
   */
  readonly completerExistingLambdaObj?: lambda.Function;
  /**
   * User provided props to override the default props for the Lambda function that completes execution. If
   * completerExistingLambdaObj and this is omitted the Lambda function for starting executions is re-used.
   *
   * @default - Re-use starter Lambda function.
   */
  readonly completerLambdaFunctionProps?: lambda.FunctionProps;
  /**
   * Optional user provided props to override the default props
   *
   * @default - Default props are used
   */
  readonly dynamoTableProps?: dynamodb.TableProps;
  /**
   * Existing instance of DynamoDB table object, If this is set then the dynamoTableProps is ignored
   *
   * @default - None
   */
  readonly existingTableObj?: dynamodb.Table;
  /**
   * Optional table permissions to grant to the Lambda function.
   * One of the following may be specified: "All", "Read", "ReadWrite", "Write".
   *
   * @default - Read/write access is given to the Lambda function if no value is specified.
   */
  readonly tablePermissions?: string;
  /**
   * Optional user provided props to override the shared layer.
   *
   * @default - None
   */
  readonly pythonLayerVersionProps?: lambdaPython.PythonLayerVersionProps;
  /**
   * Optional log level to be used for Lambda functions.
   *
   * @default - INFO
   */
  readonly logLevel?: string;
  /**
   * Setup the infrastructure to support the step function callback mechanism. If you never want to trigger Redshift
   * statements from a step function then set this to false to avoid creating an SQS queue and the required polling.
   * If you already have an SfnRedshiftTasker setup you should disable this as well (e.g. adding function for another cluster/database/username).
   *
   * @default - true
   */
  readonly createCallbackInfra?: boolean;

  /**
   * The ARN of a lambda layer containing the AWS Lambda powertools.
   *
   * @default - Not provided then an application will be created from the serverless application registry to get the layer. If you plan to create
   * multiple SfnRedshiftTaskers then you can reuse the powertoolsArn from the first instance.
   */
  readonly powertoolsArn?: string;
}

/**
 * Create infrastructure to easily create tasks in a Stepfunction that run a SQL statement
 * on a Redshift cluster and await completion.
 */
export class SfnRedshiftTasker extends cdk.Construct {
  /**
   * The Lambda function which can be used from a Step function task to invoke a SQL statement.
   */
  public readonly lambdaFunction: lambda.Function;
  /**
   * A state table that tracks the Redshift statements being executed.
   */
  public readonly trackingTable: dynamodb.Table;

  /**
   * The ARN of a layer hosting AWS Lambda powertools
   */
  public readonly powertoolsArn: string;

  /**
   * Creates the infrastructure to allow stepfunction tasks that execute SQL commands and await their completion.
   * @param scope Scope within where this infrastructure is created.
   * @param id Identifier to name this building block.
   * @param props The configuration properties of the infrastructure.
   */
  constructor(scope: cdk.Construct, id: string, props: SfnRedshiftTaskerProps) {
    super(scope, id);

    let lambdaP = path.join(__dirname, '../lambda');
    let pythonP = path.join(lambdaP, 'python');
    let rsIntegrationFunctionP = path.join(pythonP, 'rs_integration_function');
    let ddbP = path.join(rsIntegrationFunctionP, 'ddb');
    let ddbInitP = path.join(ddbP, '__init__.py');
    let rsIntegrationFunctionEnvVarP = path.join(rsIntegrationFunctionP, 'environment_labels.py');
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const PropertiesReader = require('properties-reader');
    const stripSurroundingQuotes = (x: string) => x.replace(/^['"](.+)['"]$/, '$1');
    // Define helper function that return the string value from a Python string
    let ddbProps = new PropertiesReader(ddbInitP);
    const getDdbProp = (x: string) => stripSurroundingQuotes(ddbProps.get(x));

    /**
     * Make sure we set environment variable that our Lambda function expects.
     */
    let DDB_TTL = getDdbProp('DDB_TTL');
    let DDB_ID = getDdbProp('DDB_ID');
    let DDB_INVOCATION_ID = getDdbProp('DDB_INVOCATION_ID');

    let rsProcedureStarterEnvProps = new PropertiesReader(rsIntegrationFunctionEnvVarP);
    const getRsProcedureStarterEnvProp = (x: string) => stripSurroundingQuotes(rsProcedureStarterEnvProps.get(x));

    let CLUSTER_IDENTIFIER = getRsProcedureStarterEnvProp('CLUSTER_IDENTIFIER');
    let DATABASE = getRsProcedureStarterEnvProp('DATABASE');
    let DB_USER = getRsProcedureStarterEnvProp('DB_USER');

    if (props.powertoolsArn === undefined) {
      let powertools = new sam.CfnApplication(this, 'Powertools', {
        location: {
          applicationId: 'arn:aws:serverlessrepo:eu-west-1:057560766410:applications/aws-lambda-powertools-python-layer',
          semanticVersion: '1.11.0',
        },
      });
      this.powertoolsArn = powertools.getAtt('Outputs.LayerVersionArn').toString();
    } else {
      this.powertoolsArn = props.powertoolsArn;
    }

    let defaultDynamoTableProps = {
      partitionKey: { name: DDB_ID, type: dynamodb.AttributeType.STRING },
      sortKey: { name: DDB_INVOCATION_ID, type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: DDB_TTL,
    };

    let defaultLambdaFunctionProps = {
      code: new lambda.AssetCode(rsIntegrationFunctionP),
      handler: 'index.handler',
      runtime: lambda.Runtime.PYTHON_3_8,
      environment: {
        // DynamoDB table environment variable gets automatically added by LambdaToDynamoDB
        [CLUSTER_IDENTIFIER]: props.redshiftTargetProps.clusterIdentifier,
        [DATABASE]: props.redshiftTargetProps.dbName,
        [DB_USER]: props.redshiftTargetProps.dbUser,
        [DDB_TTL]: '1', //Default time to live is 1 day.
        LOG_LEVEL: props.logLevel || 'INFO',
      },
      layers: [lambda.LayerVersion.fromLayerVersionArn(this, 'powertoolsVersion', this.powertoolsArn)],
      logRetention: logs.RetentionDays.ONE_YEAR,
      timeout: cdk.Duration.seconds(29),
      reservedConcurrentExecutions: 1, // Limit to 1 concurrent execution to allow safe checking concurrent invocations
    };
    const existingTableErr = 'Must pass existing helper table via "existingTableObj" if createCallBackInfra is set to false';
    assert(props.createCallbackInfra || props.createCallbackInfra === undefined || props.existingTableObj !== undefined, existingTableErr);

    // When an existing lambda function is provided re-use it otherwise create one using the provided properties
    let lambdaDetails;
    if (props.starterExistingLambdaObj === undefined) {
      lambdaDetails = { lambdaFunctionProps: { ...defaultLambdaFunctionProps, ...props.starterLambdaFunctionProps } };
    } else {
      lambdaDetails = { existingLambdaObj: props.starterExistingLambdaObj };
    }

    // When an existing DDB table is provided re-use it otherwise create one using the provided properties
    let ddbDetails;
    if (props.existingTableObj === undefined) {
      ddbDetails = { dynamoTableProps: { ...defaultDynamoTableProps, ...props.dynamoTableProps } };
    } else {
      ddbDetails = { existingTableObj: props.existingTableObj };
    }
    let lambda_ddb = new LambdaToDynamoDB(this, 'RSInvoker', {
      ...lambdaDetails,
      ...ddbDetails,
      tablePermissions: props.tablePermissions || 'ReadWrite',
    });
    this.lambdaFunction = lambda_ddb.lambdaFunction;

    this.trackingTable = lambda_ddb.dynamoTable;

    let allowRedshiftDataApiExecuteStatement = new iam.PolicyStatement({
      actions: ['redshift-data:ExecuteStatement', 'redshift-data:DescribeStatement',
        'redshift-data:GetStatementResult', 'redshift-data:CancelStatement', 'redshift-data:ListStatements'],
      effect: iam.Effect.ALLOW,
      resources: ['*'],
    });

    let allowRedshiftGetCredentials = new iam.PolicyStatement({
      actions: ['redshift:GetClusterCredentials'],
      effect: iam.Effect.ALLOW,
      resources: [
        cdk.Fn.sub(
          'arn:${AWS::Partition}:redshift:${AWS::Region}:${AWS::AccountId}:dbname:${ID}/${DB}',
          {
            ID: props.redshiftTargetProps.clusterIdentifier,
            DB: props.redshiftTargetProps.dbName,
          },
        ),
        cdk.Fn.sub(
          'arn:${AWS::Partition}:redshift:${AWS::Region}:${AWS::AccountId}:dbuser:${ID}/${DB_USER}',
          {
            ID: props.redshiftTargetProps.clusterIdentifier,
            DB_USER: props.redshiftTargetProps.dbUser,
          },
        ),
      ],
    });

    this.lambdaFunction.addToRolePolicy(allowRedshiftDataApiExecuteStatement);
    this.lambdaFunction.addToRolePolicy(allowRedshiftGetCredentials);

    if (props.createCallbackInfra === undefined || props.createCallbackInfra) {
      let allowReportTaskOutcome = new iam.PolicyStatement({
        actions: ['states:SendTaskSuccess', 'states:SendTaskFailure'],
        effect: iam.Effect.ALLOW,
        resources: ['*'],
      });

      let completerLambdaDetails;
      if (props.completerExistingLambdaObj === undefined && props.completerLambdaFunctionProps === undefined ) {
        //We fall back on re-using the function that starts execution.
        completerLambdaDetails = { existingLambdaObj: this.lambdaFunction };
      } else {
        if (props.completerExistingLambdaObj === undefined) {
          completerLambdaDetails = { lambdaFunctionProps: { ...defaultLambdaFunctionProps, ...props.completerLambdaFunctionProps } };
        } else {
          completerLambdaDetails = { existingLambdaObj: props.completerExistingLambdaObj };
        }
      }
      let completerIntegration = new LambdaToDynamoDB(this, 'Completer', {
        ...completerLambdaDetails,
        existingTableObj: this.trackingTable,
      });

      let eventQueue = new EventbridgeToSqs(
        this,
        'QueryFinished',
        {
          eventRuleProps: {
            description: 'Monitor queries that have been issued by Redshift data API and that completed',
            enabled: true,
            eventPattern: {
              source: ['aws.redshift-data'],
              detailType: ['Redshift Data Statement Status Change'],
            },
          },
          existingQueueObj: props.existingQueueObj,
          queueProps: props.queueProps,
          enableQueuePurging: props.enableQueuePurging,
          deadLetterQueueProps: props.deadLetterQueueProps,
          deployDeadLetterQueue: props.deployDeadLetterQueue,
          maxReceiveCount: props.maxReceiveCount,
          enableEncryptionWithCustomerManagedKey: props.enableEncryptionWithCustomerManagedKey,
          encryptionKey: props.encryptionKey,
          encryptionKeyProps: props.encryptionKeyProps,
        },
      );

      new SqsToLambda(this, 'SqsToCompleter', {
        existingLambdaObj: completerIntegration.lambdaFunction,
        existingQueueObj: eventQueue.sqsQueue,
      });
      completerIntegration.lambdaFunction.addToRolePolicy(allowReportTaskOutcome);
    } else {
      // No callback infrastructure needed
      let no_queue_err = 'Queue is part of SFN callback infra so cannot be provided if sfnCallbackSupport == false';
      assert(props.existingQueueObj === undefined, no_queue_err);
      assert(props.queueProps === undefined, no_queue_err);
      assert(props.enableQueuePurging === undefined, no_queue_err);
      assert(props.deadLetterQueueProps === undefined, no_queue_err);
      assert(props.deployDeadLetterQueue === undefined, no_queue_err);
      assert(props.maxReceiveCount === undefined, no_queue_err);
      assert(props.enableEncryptionWithCustomerManagedKey === undefined, no_queue_err);
      assert(props.encryptionKey === undefined, no_queue_err);
      assert(props. encryptionKeyProps === undefined, no_queue_err);
    }

  }
}
