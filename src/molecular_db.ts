// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as path from 'path';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as lambda from '@aws-cdk/aws-lambda';
import *as sqs from '@aws-cdk/aws-sqs';

import { LambdaToSqs } from "@aws-solutions-constructs/aws-lambda-sqs";

import { Code, Runtime } from '@aws-cdk/aws-lambda';
import * as redshift from '@aws-cdk/aws-redshift';
// import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import { CustomResource, Duration } from '@aws-cdk/core';
import { SfnRedshiftTasker } from './index';
import { RedshiftTargetProps} from './index'

import { IResource, LambdaIntegration, MockIntegration, PassthroughBehavior, RestApi } from '@aws-cdk/aws-apigateway';
import { SqsToLambda } from '@aws-solutions-constructs/aws-sqs-lambda';
import { MolDockingMainWorkflow } from './machines/mol_dock_main';
// import { LambdaToDynamoDB } from '@aws-solutions-constructs/aws-lambda-dynamodb';


export class MolecularDb {
  readonly stack: cdk.Stack[];

  constructor() {
    const app = new cdk.App();
    const env = {
      region: process.env.CDK_DEFAULT_REGION,
      account: process.env.CDK_DEFAULT_ACCOUNT,
    };
  
    //the length of the name can't be more than 10. otherwise some name will fail to satisfy constraints "Member must have length less than or equal to 64 "
    const stack = new cdk.Stack(app, 'mol-stack', { env });

    let redshiftUsername = 'rsadmin';
    let redshiftDbName = 'dev';
    
    let cluster = new redshift.Cluster(
      stack, 'rsCluster', {
        vpc: new ec2.Vpc(stack, 'vpc',
          {
            subnetConfiguration: [
              {
                cidrMask: 28,
                name: 'isolated',
                subnetType: ec2.SubnetType.ISOLATED,
              },
            ],
          },
        ),
        masterUser: {
          masterUsername: redshiftUsername,
        },
        defaultDatabaseName: redshiftDbName,
        clusterType: redshift.ClusterType.SINGLE_NODE,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        vpcSubnets: { subnetType: ec2.SubnetType.ISOLATED },
      },
    );
    
    
    let props = {
          "clusterIdentifier": cluster.clusterName,
          "dbUser": redshiftUsername,
          "dbName": redshiftDbName,
          "endpoint_hostname": cluster.clusterEndpoint.hostname,
          "endpoint_port": cluster.clusterEndpoint.port + ""
    };
        
    let rs_task_helper = new SfnRedshiftTasker(
      stack, 'RSTask', {
        redshiftTargetProps: props,
        logLevel: 'DEBUG',
      },
    );

    let exampleFunctionsCode = Code.fromAsset(path.join(__dirname, '../lambda/python/cfn_example_functions'));
    
    //CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"] will be used in the lambda
    let rs_create_drop = new lambda.Function(stack, 'RSCreateDrop', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'create_drop.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(rs_create_drop);

    /**
     * Create a molecular_data table
    **/
    new CustomResource(stack, 'molecular_data_table', {
      serviceToken: rs_create_drop.functionArn,
      properties: {
        create_sql: 'CREATE TABLE "public"."molecular_data"(id BIGINT IDENTITY(1,1), ' +
                     'title      character varying(256) encode lzo,' +
                     'smiles     character varying(256) encode lzo,' + 
                     'format     character varying(256) encode lzo,' + 
                     'source     character varying(256) encode lzo,' + 
                     'category   character varying(256) encode lzo,' + 
                     'atoms      integer encode az64,' + 
  									 'abonds  numeric(18,0) encode az64,' + 
  									 'bonds numeric(18,0) encode az64,' + 
  									 'cansmi numeric(18,0) encode az64,' + 
  									 'cansmiNS numeric(18,0) encode az64,' + 
  									 'dbonds numeric(18,0) encode az64,' + 
  									 'formula character varying(256) encode lzo,' + 
  									 'hba1 numeric(18,0) encode az64,' + 
  									 'hba2 numeric(18,0) encode az64,' + 
  									 'hbd numeric(18,0) encode az64,' + 
  									 'inchi character varying(256) encode lzo,' + 
  									 'inchikey character varying(256) encode lzo,' + 
  									 'l5 double precision,' + 
  									 'logp double precision,' + 
  									 'mp double precision,' + 
  									 'mr double precision,' + 
  									 'mw double precision, ' + 
  									 'tpsa numeric(18,0) encode az64,' + 
                     'charge     double precision,' + 
                     'dim        character varying(8) encode lzo,' + 
                     'energy     numeric(18,0) encode az64,' + 
                     'exactmass  double precision,' + 
                     'file_data  binary varying(1000000) encode lzo,' + 
                     'CONSTRAINT molecular_data_pkey PRIMARY KEY(id));',
        drop_sql: 'drop table if exists "public"."molecular_data";',
      },
    });
  
    /**
     * Create exp_data_table for docking execution result
    **/
    new CustomResource(stack, 'exp_data_table', {
      serviceToken: rs_create_drop.functionArn,
      properties: {
        create_sql: 'CREATE TABLE "public"."exp_data"(id BIGINT IDENTITY(1,1), ' +
                     'molId     BIGINT,' + 
                     'executionId      character varying(256) encode lzo,' +
                     'create_time     timestamp,' +
                     'score     numeric(18,0) encode az64,' + 
                     'result_data  binary varying(500) encode lzo,' + 
                     'CONSTRAINT exp_data_pkey PRIMARY KEY(id));',
        drop_sql: 'drop table if exists "public"."exp_data";',
      },
    });
    
    // //create lambda listening to mol json object and insert into table
    // let insert_mol_json = new lambda.Function(stack, 'insert_mol_json', {
    //   runtime: Runtime.PYTHON_3_8,
    //   handler: 'insert-mol-json.handler',
    //   code: exampleFunctionsCode,
    //   environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName,
    //                 ...props
    //                 },
    // });
    // rs_task_helper.lambdaFunction.grantInvoke(insert_mol_json);

    /**
     * Lambda Mol Object and API Gateway
     * */
    let mol_object_func = new lambda.Function(stack, 'mol_object_function', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'mol_object.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName,
                    ...props},
    });
    rs_task_helper.lambdaFunction.grantInvoke(mol_object_func);
    
    addFunctionRSPolicy(mol_object_func,props);

    
    // Integrate the Lambda functions with the API Gateway resource
    const mol_objects = new LambdaIntegration(mol_object_func);
    //const getMolList = new LambdaIntegration(mol_object_func);

    // Create an API Gateway resource for each of the CRUD operations
    const api = new RestApi(stack, 'molecularApi', {
      restApiName: 'Molecular Api Service'
    });

    const items = api.root.addResource('mols');
    items.addMethod('POST', mol_objects);
    addCorsOptions(items);
    
    
    /**
     * SQS for docking Result data 
     */
    const docking_result_queue = new sqs.Queue(stack, 'docking_result_queue',{
            maxMessageSizeBytes:256000,
            retentionPeriod:Duration.days(1),
            visibilityTimeout:Duration.minutes(15),
    });
    
    let resultDockingFunctionCode = Code.fromAsset(path.join(__dirname, '../lambda/python/molecule_object'));
    /**
     * Docking Result Lambda
     */
    let docking_result_func = new lambda.Function(stack, 'docking_result_func', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'docking_result.handler',
      code: resultDockingFunctionCode,
      environment: {...props},
    });
    
    
    /**
     * Source SQS to docking Result Lambda  
     */
    new SqsToLambda(stack, 'SQS_docking_result_queue', {
        existingLambdaObj: docking_result_func,
        existingQueueObj: docking_result_queue,
        sqsEventSourceProps:{
          batchSize: 20, // default
          maxBatchingWindow: Duration.minutes(5)
        }
      });
      
    
    /**
     * SQS for docking task data 
     */
    const data_queue = new sqs.Queue(stack, 'MolecularDataQueue',{
            maxMessageSizeBytes:1000,
            retentionPeriod:Duration.days(5),
            visibilityTimeout:Duration.minutes(15),
    });
    
    let dockingFunctionCode = Code.fromAsset(path.join(__dirname, '../lambda/python/sample_docking'));
    /**
     * Mol Docking Lambda
     */
    let docking_mol_func = new lambda.Function(stack, 'docking_mol_func', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'docking_lambda.handler',
      code: dockingFunctionCode,
      environment: { "docking_result_queue": docking_result_queue.queueUrl, ...props},
    });
    
    new LambdaToSqs(this, 'LambdaToSqsForDockingResult', {
        existingLambdaObj: docking_mol_func,
        existingQueueObj: docking_result_queue,
    });
    
    addFunctionRSPolicy(docking_mol_func,props);
    
    
    /**
     * Source SQS to docking Lambda  
     */
    new SqsToLambda(stack, 'SqsToCompleter', {
        existingLambdaObj: docking_mol_func,
        existingQueueObj: data_queue,
        sqsEventSourceProps:{
          batchSize: 1, // default
          maxBatchingWindow: Duration.minutes(1)
        }
      });
    
    

    /**
     * workflow to emit mol data to SQS
    */
   new MolDockingMainWorkflow(stack,rs_task_helper.lambdaFunction,data_queue)
    
    this.stack = [stack];
  }
}

export function addFunctionRSPolicy(lambdaFuc:lambda.Function, props:RedshiftTargetProps){
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
            ID: props.clusterIdentifier,
            DB: props.dbName,
          },
        ),
        cdk.Fn.sub(
          'arn:${AWS::Partition}:redshift:${AWS::Region}:${AWS::AccountId}:dbuser:${ID}/${DB_USER}',
          {
            ID: props.clusterIdentifier,
            DB_USER: props.dbUser,
          },
        ),
      ],
    });

    lambdaFuc.addToRolePolicy(allowRedshiftDataApiExecuteStatement);
    lambdaFuc.addToRolePolicy(allowRedshiftGetCredentials);
}

export function addCorsOptions(apiResource: IResource) {
  apiResource.addMethod('OPTIONS', new MockIntegration({
    integrationResponses: [{
      statusCode: '200',
      responseParameters: {
        'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent'",
        'method.response.header.Access-Control-Allow-Origin': "'*'",
        'method.response.header.Access-Control-Allow-Credentials': "'false'",
        'method.response.header.Access-Control-Allow-Methods': "'OPTIONS,GET,PUT,POST,DELETE'",
      },
    }],
    passthroughBehavior: PassthroughBehavior.NEVER,
    requestTemplates: {
      "application/json": "{\"statusCode\": 200}"
    },
  }), {
    methodResponses: [{
      statusCode: '200',
      responseParameters: {
        'method.response.header.Access-Control-Allow-Headers': true,
        'method.response.header.Access-Control-Allow-Methods': true,
        'method.response.header.Access-Control-Allow-Credentials': true,
        'method.response.header.Access-Control-Allow-Origin': true,
      },
    }]
  })
}



// run the integ testing
new MolecularDb();