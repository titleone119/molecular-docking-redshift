// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0


import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as cdk from '@aws-cdk/core';
import { Construct } from '@aws-cdk/core';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks'
import { IQueue } from '@aws-cdk/aws-sqs';


export class MolParallelPush {
  public readonly definition: sfn.StateMachine;

  constructor(scope: cdk.Construct, data_queue: IQueue) {
    
    let workflowFailed1 = new sfn.Fail(scope, 'ParallelStatementFailed_');
    
    let testDataQueue= data_queue;
    
    if(testDataQueue == data_queue){
      data_queue = testDataQueue;
    }
   
    /**
     * task for post data to SQS
     */
    function postData2SQSProcedure(_scope: Construct) {
      
      return new tasks.SqsSendMessage(scope, 'sendMolecularData', {
        queue:data_queue,
        messageBody: sfn.TaskInput.fromJsonPathAt('$'),
        resultPath: sfn.JsonPath.DISCARD
      }
      ).addCatch(workflowFailed1, { errors: ['States.ALL'] }, // We don't expect any other failure
      );
    }
    
  
    /**
     * start --> query IDs ---> get IDs -----> Map based on Query Out ID List  
     *                                                           ----> Item each Query and Docking all the mols
     * @abstract                                                      -------> post2SQS
     *                                                                    -----|| End
     */
    
    let parallelMap = new sfn.Map(scope, 'Map_IDList', {
                          maxConcurrency: 40,
                          itemsPath: sfn.JsonPath.stringAt('$.Records'),
                          resultPath: sfn.JsonPath.DISCARD,
    });
     
    // //let queryDataProcedure = createQueryingDataProcedure(scope)
    // //let getDataProcedure = createGetDataProcedure(scope)
    let postSQSProcedure = postData2SQSProcedure(scope)
    
    // //queryDataProcedure.next(getDataProcedure)
    // //getDataProcedure.next(postSQSProcedure)
    
    parallelMap.iterator(postSQSProcedure);
    
    // getIdsProcedure.next(postSQSProcedure)
    
    
   this.definition = new sfn.StateMachine(
      scope, 'ParallelPush', {
        definition: parallelMap,
        stateMachineType: sfn.StateMachineType.EXPRESS,
      },
    );
  }
}