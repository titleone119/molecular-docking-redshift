// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as lambda from '@aws-cdk/aws-lambda';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as cdk from '@aws-cdk/core';
import { RetryableLambdaInvoke } from './util';
import { Construct, Duration } from '@aws-cdk/core';

/**
 * The details of the Redshift target in which you will execute SQL statements.
 */
export interface DockingTaskProps {
  /**
   * query the docking molecules
   * where MW < 2 and MM > 4 or .... 
   */
  queryStmt: string;
  
  /**
   * max concurrecy of the paralell map 
   */
   maxConcurrency: number;
   
   /**
    * the number for mols for each docking task
    */
  batchSize: number;
   
   /**
    * 
    */
   dockingConf: string;
}


export class MolDockingSampleWorkflow {
  public readonly definition: sfn.StateMachine;

  constructor(scope: cdk.Construct, rsLambda: lambda.Function,taskProps : DockingTaskProps) {
    
    let workflowFailed1 = new sfn.Fail(scope, 'StatementFailed_');
    //let workflowSucceeded1 = new sfn.Succeed(scope, 'StatementSucceeded_');
    
    /**
     * task for query count
     */
    function createQueryingIDsProcedure(_scope: Construct) {
      let successRun = new sfn.Succeed(_scope, `success`);
      let countQueryStm = "select id from public.molecular_data " + taskProps.queryStmt;
      
      return new RetryableLambdaInvoke(
        _scope, 'QueryCountTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
          payload: sfn.TaskInput.fromObject({
            'taskToken': sfn.JsonPath.taskToken,
            'executionArn.$': '$$.Execution.Id',
            'sqlStatement': countQueryStm, 
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      ).next(successRun);
    }
    
    /**
     * task for query count
     */
    function createGetResultOfIDsProcedure(_scope: Construct) {
      
      return new RetryableLambdaInvoke(
        _scope, 'QueryCountTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
          payload: sfn.TaskInput.fromObject({
            'statementId.$': '$.executionDetails.Id',
            'action': 'getStatementResult',
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      )
    }
    
    
    /**
     * get content by id
     */
    function createDockingProcedure(_scope: Construct) {
      let successRun = new sfn.Succeed(_scope, `success`);
      let stmt = "select * from public.molecular_data where 1=1 and id =";
      
      return new RetryableLambdaInvoke(
        _scope, 'QueryCountTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
          payload: sfn.TaskInput.fromObject({
            'taskToken': sfn.JsonPath.taskToken,
            'executionArn.$': '$$.Execution.Id',
            'sqlStatement': stmt + '$.results.id', 
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      ).next(successRun);
    }
    
    /**
     * start --> query IDs ---> get IDs -----> Map based on Query Out ID List  
     *                                                           ----> Item each Query and Docking all the mols
     * @abstract                                                      -------> Sum all the docking result from DB table
     *                                                                    -----|| End
     */
    
    //query the count
    let startQueryCountTask = createQueryingIDsProcedure(scope);
    
    let dockingTask = createDockingProcedure(scope)
    
    let parallelMap = new sfn.Map(scope, 'Map State', {
                          maxConcurrency: taskProps.maxConcurrency,
                          itemsPath: sfn.JsonPath.stringAt('$.result.records'),
    });
    
    parallelMap.iterator(dockingTask);
    
    this.definition = new sfn.StateMachine(
      scope, 'PollingRsTask', {
        definition: startQueryCountTask,
      },
    );
  }
}