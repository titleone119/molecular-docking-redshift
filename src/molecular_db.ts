// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as path from 'path';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as lambda from '@aws-cdk/aws-lambda';
import { Code, Runtime } from '@aws-cdk/aws-lambda';
import * as redshift from '@aws-cdk/aws-redshift';
import * as cdk from '@aws-cdk/core';
import { CustomResource } from '@aws-cdk/core';
import { SfnRedshiftTasker } from './index';

import { IResource, LambdaIntegration, MockIntegration, PassthroughBehavior, RestApi } from '@aws-cdk/aws-apigateway';


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
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        vpcSubnets: { subnetType: ec2.SubnetType.ISOLATED },
      },
    );
    let rs_task_helper = new SfnRedshiftTasker(
      stack, 'RSTask', {
        redshiftTargetProps: {
          dbUser: redshiftUsername,
          dbName: redshiftDbName,
          clusterIdentifier: cluster.clusterName,
        },
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

    // Create a table
    //let rsTable = 
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

    
    //create lambda listening to mol json object and insert into table
    let insert_mol_json = new lambda.Function(stack, 'insert_mol_json', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'insert-mol-json.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(insert_mol_json);

    
    let mol_object_func = new lambda.Function(stack, 'mol_object_function', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'mol_object.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(mol_object_func);
  
    
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
    
    this.stack = [stack];
  }
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