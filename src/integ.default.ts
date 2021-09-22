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
        existingTableObj: rs_task_helper.trackingTable,
        createCallbackInfra: false,
        powertoolsArn: rs_task_helper.powertoolsArn,
      },
    );

    let rs_user_manager = new lambda.Function(stack, 'RSUserManager', {
      runtime: Runtime.PYTHON_3_8,
      handler: 'index.handler',
      code: Code.fromAsset(path.join(__dirname, '../lambda/python/cfn_manage_user_function')),
      environment: { CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA: rs_task_helper.lambdaFunction.functionName },
    });
    rs_task_helper.lambdaFunction.grantInvoke(rs_user_manager);

    new CustomResource(stack, 'rs_cfncreated user', {
      serviceToken: rs_user_manager.functionArn,
      properties: {
        username: 'cfncreated_user',
        password: 'md5e1c252bf4c426727db9c7bfc726760d8', //Note that this would require all password changes to pass through CFN.
        create_db: true,
        create_user: true,
        unrestricted_syslog_access: true,
        groups: ['group_a', 'group_b'],
        valid_until: '2025-01-01 12:00:00',
        connection_limit: 3,
        session_timeout: 1728000,
      },
    });

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