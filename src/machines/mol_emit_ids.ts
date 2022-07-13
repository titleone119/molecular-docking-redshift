// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as lambda from '@aws-cdk/aws-lambda';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as cdk from '@aws-cdk/core';
import { RetryableLambdaInvoke } from './util';
import { Construct, Duration } from '@aws-cdk/core';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks'
import { IQueue } from '@aws-cdk/aws-sqs';
import { Runtime, Code } from '@aws-cdk/aws-lambda';
import path = require('path');


export class MolEmitIdsWorkflow {
  
  public readonly definition: sfn.StateMachine;

  constructor(scope: cdk.Construct, rsLambda: lambda.Function, data_queue: IQueue) {
    
    let workflowFailed1 = new sfn.Fail(scope, 'StatementFailed_');
    
    let testDataQueue= data_queue;
    
    if(testDataQueue == data_queue){
      data_queue = testDataQueue;
    }
    
    
    let exampleFunctionsCode = Code.fromAsset(path.join(__dirname, '../../lambda/python/cfn_example_functions'));
    
    /**
     * task for query count
     */
    function creatPageSqlProcedure(_scope: Construct) {
      //let successRun = new sfn.Succeed(_scope, `success`);
      //let countQueryStm = "select id from public.molecular_data " + taskProps.queryStmt;
      
      let page_sql_func = new lambda.Function(_scope, 'page_sql_func', {
        runtime: Runtime.PYTHON_3_8,
        handler: 'make_paging_stmt.handler',
        code: exampleFunctionsCode,
        environment: { },
      });
      
      return new tasks.LambdaInvoke(_scope, 'MakePageSqlTask', {
          lambdaFunction: page_sql_func,
          payload: sfn.TaskInput.fromObject({
              "params.$": "$"
          }),
          outputPath:'$.Payload'
      }).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      );;
    }
    
    /**
     * task for query id
     */
    function createQueryingIDsProcedure(_scope: Construct) {
      //let successRun = new sfn.Succeed(_scope, `success`);
      //let countQueryStm = "select id from public.molecular_data " + taskProps.queryStmt;
      
      return new RetryableLambdaInvoke(
        _scope, 'QueryIdsTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
          payload: sfn.TaskInput.fromObject({
            'taskToken': sfn.JsonPath.taskToken,
            'executionArn.$': '$$.Execution.Id',
            'sqlStatement.$': '$.idSqlStatement' , 
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      );
    }
    
    /**
     * task for get result of ID
     */
    function createGetResultOfIDsProcedure(_scope: Construct) {
      
      return new RetryableLambdaInvoke(
        _scope, 'GetResultOfIDsTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
          payload: sfn.TaskInput.fromObject({
            'statementId.$': '$.executionDetails.detail.statementId',
            'action': 'getStatementResult',
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails.Payload',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      );
    }
    
    /**
     * start --> query IDs ---> get IDs -----> Map based on Query Out ID List  
     *                                                       
     */
    
    let pagingSqlProcedure = creatPageSqlProcedure(scope)
    //query the count
    let startQueryIdsProcedure = createQueryingIDsProcedure(scope);
    pagingSqlProcedure.next(startQueryIdsProcedure)
    
    let getIdsProcedure = createGetResultOfIDsProcedure(scope)
    startQueryIdsProcedure.next(getIdsProcedure)
    
   this.definition = new sfn.StateMachine(
      scope, 'PollingMolIDsTask', {
        definition: pagingSqlProcedure,
      },
    );
  }
}