`cfn_manage_user_function` is a sample function that creates a Cloudformation custom resource that allows to manage
Redshift users.

# AWS Cloudformation Redshift integration
For generic advice on a AWS Cloudformation Redshift integration see 
[cloudformation_redshift_integration.md](/docs/cloudformation_redshift_integration.md).

# How it works (with code examples)

A Cloudformation custom resource has to be able to handle Create, Update & Delete `RequestType`s so a helper function
should be created in order to create a declarative cloudformation representation which will manage the user
correspondingly on Redshift. To do this generate the SQL depending on the `RequestType`. Since
`cdk-stepfunctions-redshift` provides a `CfnCallback` callback
[(code-link)](/lambda/python/rs_integration_function/callback_sources/cfn_callback.py#L9-L19) that takes care of
performing the callback these custom resources only need to implement the logic to map Cloud formation representation to
SQL statement. That means considering the `RequestType` (`Create`, `Delete`, `Update`) and the `ResourceProperties` to
generate the SQL statement.

1. A Lambda function is to be created out of this code [(code-link)](/src/integ.default.ts#L83-L88).
2. This lambda function has to be granted invoke permissions for `SfnRedshiftTasker` lambda function 
   [(code-link)](/src/integ.default.ts#L89).
3. You can create a custom cloudformation resource backed by this Lambda function. 
   [(code-link)](/src/integ.default.ts#L91-L104)
   
The properties passed in end up in `ResourceProperties` of the event that triggers the lambda function. For this example
code the supported properties are in the [source code](/lambda/python/cfn_manage_user_function/index.py#L23-L31).

# cfnresponse.py

This is a slight modification of the module provided in the [AWS documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-lambda-function-code-cfnresponsemodule.html).
We will use this to inform Cloudformation of failures in case users provide invalid input to the Lambda function.

This is only needed for error handling prior to calling the Lambda function that will make sure the SQL statement is ran
. Errors that happen after successful invocation (e.g. actual database errors) will be reported by the callback
mechanism and doesn't require additional action.

# Known limitations 
- Setting of a session parameter at user level is unsupported. This is only supported in alter statements. If
one requires to do so the create will have to generate 2 SQL statements (1 create and 1 alter). This can be achieved by 
adding some code. Because Redshift Data API has released support for multiple statements within a transaction it 
shouldn't even be too hard to achieve it but out of scope for this example.