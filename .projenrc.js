const { AwsCdkConstructLibrary } = require('projen');

const AWS_CDK_VERSION = '1.120.0';
const PROJECT_NAME = 'cdk-stepfunctions-redshift';
const PROJECT_DESCRIPTION = 'A JSII construct lib to build AWS Serverless infrastructure to orchestrate Redshift using AWS stepfunctions.';
const AUTOMATION_TOKEN = 'PROJEN_GITHUB_TOKEN';

const project = new AwsCdkConstructLibrary({
  author: 'Peter Van Bouwel',
  authorAddress: 'pbbouwel@amazon.com',
  cdkVersion: AWS_CDK_VERSION,
  jsiiFqn: `aws-samples.${PROJECT_NAME}`,
  description: PROJECT_DESCRIPTION,
  name: PROJECT_NAME,
  repositoryUrl: 'https://github.com/aws-samples/cdk-stepfunctions-redshift.git',
  cdkVersionPinning: true,
  cdkDependencies: [
    '@aws-cdk/core',
    '@aws-cdk/aws-dynamodb',
    '@aws-cdk/aws-ec2',
    '@aws-cdk/aws-events-targets',
    '@aws-cdk/aws-iam',
    '@aws-cdk/aws-kms',
    '@aws-cdk/aws-lambda',
    '@aws-cdk/aws-lambda-event-sources',
    '@aws-cdk/aws-lambda-python',
    '@aws-cdk/aws-redshift',
    '@aws-cdk/aws-sqs',
    '@aws-cdk/aws-stepfunctions',
    '@aws-cdk/aws-stepfunctions-tasks',
    '@aws-cdk/aws-logs',
    '@aws-cdk/aws-events',
    '@aws-solutions-constructs/aws-eventbridge-sqs',
    '@aws-solutions-constructs/aws-sqs-lambda',
    '@aws-solutions-constructs/aws-lambda-dynamodb',
    '@aws-cdk/aws-sam',
  ],
  deps: ['properties-reader'],
  bundledDeps: [
    'properties-reader',
    'xmldom@github:xmldom/xmldom#0.7.0',
  ],
  eslint: true,
  keywords: ['cdk', 'redshift', 'stepfunction', 'orchestration'],
  license: 'MIT-0',
  copyrightPeriod: '',
  copyrightOwner: 'Amazon.com, Inc. or its affiliates. All Rights Reserved.',
  defaultReleaseBranch: 'release',
  pullRequestTemplateContents: [
    '---',
    '\nIssue #, if available:',
    '\nDescription of changes:',
    '\nBy submitting this pull request, I confirm that you can use, modify, copy, and redistribute this contribution, under the terms of your choice.',
  ],
  dependabotOptions: {
    scheduleInterval: 'weekly',
    autoMerge: true,
  },
  publishToPypi: {
    module: 'cdk_stepfunctions_redshift',
    distName: PROJECT_NAME,
  },
});

const common_exclude = ['cdk.out', 'cdk.context.json', 'docker-compose.yml', 'yarn-error.log', '**/.idea', 'package-lock.json', '.projenrc.js.bak', 'version.json.bak'];
project.npmignore.exclude(...common_exclude, '/codebase');
project.gitignore.exclude(...common_exclude);
const isCdkVersionDefinedAsExpected = 'grep "^const AWS_CDK_VERSION =" .projenrc.js | wc -l | grep 1 >/dev/null';
const cdkVerMatch = project.addTask('cdk-ver-match', { condition: isCdkVersionDefinedAsExpected } );
cdkVerMatch.exec(' ' +
    '  grep \'"version": "\'"$(grep "^const AWS_CDK_VERSION" .projenrc.js | grep -o "[0-9]*\\.[0-9]*\\.")[0-9][0-9]*"\'"\' version.json 2>/dev/null' +
    '  || standard-version --release-as "$(grep "^const AWS_CDK_VERSION" .projenrc.js | grep -o "[0-9]*\\.[0-9]*\\.[0-9]*")"');

project.synth();
