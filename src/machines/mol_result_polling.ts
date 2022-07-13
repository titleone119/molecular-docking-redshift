// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as lambda from '@aws-cdk/aws-lambda';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as cdk from '@aws-cdk/core';
import { RetryableLambdaInvoke } from './util';


export class MolResultPollingMachine {
  public readonly definition: sfn.StateMachine;

  constructor(scope: cdk.Construct, lambdaFunction: lambda.Function) {
    let statementSucceeded1 = new sfn.Succeed(scope, 'PollingStatementSucceeded_');

    
    let executeQueryResultProcedure = new RetryableLambdaInvoke(
      scope, 'executeQueryResultProcedure', {
        lambdaFunction: lambdaFunction,
        payloadResponseOnly: true,
        payload: sfn.TaskInput.fromObject({
          sqlStatement:  sfn.JsonPath.stringAt('$.docking_result_sql'),
        }),
        heartbeat: cdk.Duration.seconds(10),
        resultPath: '$.executionDetails',
      },
    );
    
    let describeQueryResultProcedure = new RetryableLambdaInvoke(
      scope, 'describeQueryResultProcedure', {
        lambdaFunction: lambdaFunction,
        integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
        payload: sfn.TaskInput.fromObject({
            'statementId.$': '$.executionDetails.Id',
            'action': 'describeStatement',
        }),
        heartbeat: cdk.Duration.seconds(10),
        resultSelector: {
          "Id.$": "$.Payload.Id",
          "Status.$": "$.Payload.Status",
          "docking_result_sql.$": "$.Payload.QueryString"
        },
        resultPath: "$.executionDetails"
      },
    );
    
    let executeGetResultProcedure = new RetryableLambdaInvoke(
      scope, 'executeGetResultProcedure', {
        lambdaFunction: lambdaFunction,
        integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
        payload: sfn.TaskInput.fromObject({
            'statementId.$': '$.executionDetails.Id',
            'action': 'getStatementResult',
        }),
        heartbeat: cdk.Duration.seconds(10),
        resultPath: '$.executionDetails',
        resultSelector: {
          "resultCount.$": "$.Payload.Records[0][0].longValue"
        },
    });
    
    let waitBetweenDescribeResults = new sfn.Wait(scope, 'waitBetweenDescribeResults', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(1)),
    });
    
    let choiceCheckDescribe = new sfn.Choice(scope, 'choiceCheckDescribeGetDockingResult', {}).when(
      sfn.Condition.stringEquals('$.executionDetails.Status', 'FINISHED'),
        executeGetResultProcedure,
      ).otherwise(waitBetweenDescribeResults);

    
    
    let waitBetweenPollResults = new sfn.Wait(scope, 'waitBetweenPollResults', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(120)),
    });
    
    let choiceQueryIdResult = new sfn.Choice(scope, 'choiceQueryIdResult', {}).when(
      sfn.Condition.numberGreaterThanEqualsJsonPath('$.executionDetails.resultCount', '$.totalCount'),
      statementSucceeded1,
    ).otherwise(waitBetweenPollResults);

    executeQueryResultProcedure.next(describeQueryResultProcedure);
    describeQueryResultProcedure.next(choiceCheckDescribe);
    waitBetweenDescribeResults.next(describeQueryResultProcedure)
    executeGetResultProcedure.next(choiceQueryIdResult);
    waitBetweenPollResults.next(executeQueryResultProcedure);

    this.definition = new sfn.StateMachine(
      scope, 'PollingDockingResultTask', {
        definition: executeQueryResultProcedure,
      },
    );
  }
}