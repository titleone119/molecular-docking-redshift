// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as lambda from '@aws-cdk/aws-lambda';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as cdk from '@aws-cdk/core';
import { IQueue } from '@aws-cdk/aws-sqs';
import { StepFunctionsStartExecution } from '@aws-cdk/aws-stepfunctions-tasks';
import { IntegrationPattern } from '@aws-cdk/aws-stepfunctions';
import { MolEmitIdsWorkflow } from './mol_emit_ids';
import { MolParallelPush } from './mol_parallel_push';
import { MolResultPollingMachine } from './mol_result_polling';


export class MolDockingMainWorkflow {
  public readonly definition: sfn.StateMachine;

  constructor(scope: cdk.Construct, rsLambda: lambda.Function, data_queue: IQueue) {
    
    //let workflowFailed1 = new sfn.Fail(scope, 'StatementFailed_');
    let workflowSucceeded1 = new sfn.Succeed(scope, 'SuccessDocking')
    
    let testDataQueue= data_queue;
    
    if(testDataQueue == data_queue){
      data_queue = testDataQueue;
    }
    
    //find id list
    let emitIds = new MolEmitIdsWorkflow(scope, rsLambda, data_queue);
    let emitIdsTask = new StepFunctionsStartExecution(scope, 'pollingIdWorkflow', {
                                  integrationPattern: IntegrationPattern.RUN_JOB,
                                  stateMachine:emitIds.definition,
                                  resultSelector: {
                                    "index.$": "$.Output.index",
                                    "sqlStatement.$": "$.Output.sqlStatement",
                                    "idSqlStatement.$": "$.Output.idSqlStatement",
                                    "totalCount.$": "$.Output.totalCount",
                                    "docking_result_sql.$": "$.Output.docking_result_sql",
                                    "executionId.$": "$.Output.executionId",
                                    "rows.$": "$.Output.executionDetails.detail.rows",
                                    "Records.$": "$.Output.executionDetails.Payload.Payload.Records"
                                  }
    });
    
    //push id list
    let parallelPush = new MolParallelPush(scope,data_queue)
    let parallelPushTask = new StepFunctionsStartExecution(scope, 'parallelPushWorkflow', {
                                  integrationPattern: IntegrationPattern.REQUEST_RESPONSE,//don't support RUN_JOB,but async response for Express State Machine
                                  stateMachine: parallelPush.definition,
                                  outputPath: "$.['index','sqlStatement','idSqlStatement','totalCount','docking_result_sql', 'executionId','rows']",
                                  resultPath: sfn.JsonPath.DISCARD
    });
    parallelPushTask.next(emitIdsTask)
    
    //polling docking result
    let pollingDockingResult = new MolResultPollingMachine(scope,rsLambda)
    let pollingDockingResultTask = new StepFunctionsStartExecution(scope, 'pollingResultTask', {
                                  integrationPattern: IntegrationPattern.RUN_JOB,
                                  stateMachine: pollingDockingResult.definition
    });
    
    //check whether the id list is done
    let checkHasMoreIDTask = new sfn.Choice(scope, 'choiceExecutionResult', {}).when(
                            sfn.Condition.numberGreaterThan('$.rows', 0),
                              parallelPushTask,
                          ).otherwise(
                              pollingDockingResultTask
                          );
   emitIdsTask.next(checkHasMoreIDTask);
   
                          
   pollingDockingResultTask.next(workflowSucceeded1)
   
   
   
    this.definition = new sfn.StateMachine(scope, 'Mol_Docking_Main_Workflow', {
          definition: emitIdsTask,
      });
      
  }

}