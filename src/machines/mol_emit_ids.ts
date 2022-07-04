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
    function createCountQueryTask(_scope: Construct) {
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
    function createGetResultOfCountQueryTask(_scope: Construct) {
      let successRun = new sfn.Succeed(_scope, `successGetCount`);
      
      return new RetryableLambdaInvoke(
        _scope, 'getResultOfCountTask', {
          lambdaFunction: rsLambda,
          integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
          payload: sfn.TaskInput.fromObject({
            //'statementId': '$.executionDetails.smatementId',
            'statementId': 'LATEST',
            'executionArn.$': '$$.Execution.Id',
            'action': 'getStatementResult',
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(workflowFailed1, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      ).next(successRun);
    }
    
    
    /**
  
    function createGetResultOfQueryCountAndMakeMolQueryTask(_scope: construct){
      return new RetryableLambdaInvoke(
        _scope, `startSlowQuery${i}`, {
          lambdaFunction: makeDockStatement,
          integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
          payload: sfn.TaskInput.fromObject({
            'taskToken': sfn.JsonPath.taskToken,
            'executionArn.$': '$$.Execution.Id',
            'sqlStatement': molQueryStm, //
            'action': 'executeSingletonStatement',
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(testFail, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(testFail, { errors: ['States.ALL'] }, // We don't expect any other failure
      ).next(successRun);
      
    }
    
    function createDockingMolsTask(_scope: Construct, i: string) {
      let testFail = new sfn.Fail(_scope, `testFail${i}`);
      let successRun = new sfn.Succeed(_scope, `successParallel${i}`);
      let offset = taskProps.batchSize * i; 
      let molQueryStm = "select file_data from public.molecular_data " + taskProps.whereOfQueryStm + " order by id offset " + offset;
      
      return new RetryableLambdaInvoke(
        _scope, `startSlowQuery${i}`, {
          lambdaFunction: dockingLambda,
          integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
          payload: sfn.TaskInput.fromObject({
            'taskToken': sfn.JsonPath.taskToken,
            'executionArn.$': '$$.Execution.Id',
            'sqlStatement': molQueryStm, //
            'action': 'executeSingletonStatement',
          }),
          heartbeat: Duration.seconds(300),
          resultPath: '$.executionDetails',
        },
      ).addCatch(testFail, { errors: ['States.Timeout'] }, // We don't expect timeout on RS cluster
      ).addCatch(testFail, { errors: ['States.ALL'] }, // We don't expect any other failure
      ).next(successRun);
    }
    **/
    
    
    /**
     * start --> queryCount --> getCount and Make Query List -----> Map based on QueryList  
     *                                                           ----> Item each Query and Docking all the mols
     * @abstract                                                      -------> Sum all the docking result from DB table
     *                                                                    -----|| End
     */
    
    //query the count
    let startQueryCountTask = createCountQueryTask(scope);
    
    let getQueryCountAndMakeQueryTask = createGetResultOfCountQueryTask(scope); //must a lambda logic
    
    
    //get result count and output the query list
    
    //add query list
    /**
    dockingTask = createDockingMolsTask(scope)
    
    let parallelMap = new sfn.Map(self, 'Map State', {
                          maxConcurrency: 1,
                          itemsPath: sfn.JsonPath.stringAt('$.inputForMap'),
    });
    
    parallelMap.iterator(dockingTask);
  **/
    
    
    //polling the count result and output multiple query task in parrelel in a query-id array
    
    //Map Parellel job for docking with lambda based on the query-id array
    
    //Sum Job
 
    let waitBetweenPolls = new sfn.Wait(scope, 'WaitBetweenPolls', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(10)),
    });
    
    startQueryCountTask.next(waitBetweenPolls);
    let checkExecutionStateRSTask = new RetryableLambdaInvoke(
      scope, 'checkExecutionStateRSTask', {
        lambdaFunction: rsLambda,
        payloadResponseOnly: true,
        payload: sfn.TaskInput.fromObject({
          'statementId.$': '$.executionDetails.Id',
          'action': 'describeStatement',
        }),
        heartbeat: cdk.Duration.seconds(300),
        resultPath: '$.executionDetails',
      },
    );
    waitBetweenPolls.next(checkExecutionStateRSTask);

    let choiceExecutionResult = new sfn.Choice(scope, 'choiceExecutionResult', {}).when(
      sfn.Condition.stringEquals('$.executionDetails.Status', 'FINISHED'),
      getQueryCountAndMakeQueryTask,
    ).when(
      sfn.Condition.stringEquals('$.executionDetails.Status', 'ABORTED'),
      workflowFailed1,
    ).when(
      sfn.Condition.stringEquals('$.executionDetails.Status', 'FAILED'),
      workflowFailed1,
    ).otherwise(waitBetweenPolls);

    checkExecutionStateRSTask.next(choiceExecutionResult);
     
    this.definition = new sfn.StateMachine(
      scope, 'PollingRsTask', {
        definition: startQueryCountTask,
      },
    );
  }
}