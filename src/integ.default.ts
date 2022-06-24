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
import { CancellingStatementMachine } from './machines/cancelling_statement';
import { ChainedMachine } from './machines/chained_machines';
import { ParallelNoConcurrencyMachine } from './machines/parallel_no_concurrency';
import { PollingMachine } from './machines/polling';
import { SingleFailureMachine } from './machines/single_failure';
import { SingleSuccessMachine } from './machines/single_success';
import { SuccessAndFailMachine } from './machines/success_and_fail';


export class IntegTesting {
  readonly stack: cdk.Stack[];

  constructor() {
    const app = new cdk.App();
    const env = {
      region: process.env.CDK_DEFAULT_REGION,
      account: process.env.CDK_DEFAULT_ACCOUNT,
    };

    const stack = new cdk.Stack(app, 'integ-test-stack', { env });

    let redshiftUsername = 'admin';
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
    //Deploying separate function to allow access to another user without duplicating infra or powertools layer.
    let rs_task_helper2 = new SfnRedshiftTasker(
      stack, 'RSTaskUser2', {
        redshiftTargetProps: {
          dbUser: 'user2',
          dbName: redshiftDbName,
          clusterIdentifier: cluster.clusterName,
        },
        existingTableObj: rs_task_helper.trackingTable,// use existing tracking table created in helper1
        createCallbackInfra: false,
        powertoolsArn: rs_task_helper.powertoolsArn,  //use existing powertoll created in helper1
      },
    );

    let exampleFunctionsCode = Code.fromAsset(path.join(__dirname, '../lambda/python/cfn_example_functions'));
    let rs_user_manager = new lambda.Function(stack, 'RSUserManager', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'manage_user.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(rs_user_manager);

    new CustomResource(stack, 'rs_cfncreated_user', {
      serviceToken: rs_user_manager.functionArn,
      properties: {
        username: 'cfncreated_user',
        password: 'md5e1c252bf4c426727db9c7bfc726760d8', //Note that this would require all password changes to pass through CFN.
        create_db: true,
        create_user: false,
        unrestricted_syslog_access: true,
        groups: [], // must be the existing group name of rs cluster! check by command
        valid_until: '2025-01-01 12:00:00',
        connection_limit: 3,
        session_timeout: 1728000,
      },
    });

    let rs_create_drop = new lambda.Function(stack, 'RSCreateDrop', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'create_drop.handler',
      code: exampleFunctionsCode,
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(rs_create_drop);

    // Create a table
    let rsTable = new CustomResource(stack, 'rs_table', {
      serviceToken: rs_create_drop.functionArn,
      properties: {
        create_sql: 'create table my_table(id int, value varchar(50));',
        drop_sql: 'drop table if exists my_table;',
      },
    });

    // Create a view that depends on the table
    let rsView = new CustomResource(stack, 'rs_view', {
      serviceToken: rs_create_drop.functionArn,
      properties: {
        create_sql: 'create view my_view as (select id from my_table);',
        drop_sql: 'drop view if exists my_view;',
      },
    });
    rsView.node.addDependency(rsTable); // Explicitly put dependencies!

    let chainedMachine = new ChainedMachine(stack);
    chainedMachine.push_front('singleFailure', new SingleFailureMachine(stack, rs_task_helper2.lambdaFunction).definition);
    chainedMachine.push_front('singleSuccess', new SingleSuccessMachine(stack, rs_task_helper.lambdaFunction).definition);
    chainedMachine.push_front('parallelNoConcurrency', new ParallelNoConcurrencyMachine(stack, rs_task_helper.lambdaFunction).definition);
    chainedMachine.push_front('successAndFail', new SuccessAndFailMachine(stack, rs_task_helper.lambdaFunction).definition);
    chainedMachine.push_front('polling', new PollingMachine(stack, rs_task_helper.lambdaFunction).definition);
    chainedMachine.push_front('cancelling', new CancellingStatementMachine(stack, rs_task_helper.lambdaFunction).definition);
    chainedMachine.build();
    this.stack = [stack];
  }
}

// run the integ testing
new IntegTesting();