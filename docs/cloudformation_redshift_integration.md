# AWS Cloudformation Redshift integration

This documentation will detail how you can have AWS Cloudformation resources that orchestrate Redshift steps using this 
module.

Cloudformation allows defining infrastructure as code in a declarative way. This allows you to define resources with
properties and let Cloudformation do the orchestration in order to instantiate those resources. The `lambdaFunction`
from `SfnRedshiftTasker` allows to callback cloudformation if the event has the fields that are emitted by Cloud 
formation when operating on a custom resource. This means it takes care of most of the heavy lifting (tracking query
progress and once finished respond with success/failure), and a custom resource should only define how to get the SQL 
that needs to be executed in order to perform a transition when changing the Cloud formation template.

## Example to manage users

There is an example lambda function to manage users on Redshift. This will illustrate how this functionality can be
used. See [cfn_manage_user_function](/lambda/python/cfn_example_functions/README.md) for more details.

# Good to know

## Updates keep the Physical IDs

All updates are meant as in-place updates. The physical ID remains the same. This is currently hard coded
[here](/lambda/python/rs_integration_function/callback_sources/cfn_callback.py#L60-L63).

## Handling updates
Since cloudformation is declarative you only specify the desired state in the new template. In that scenario
cloudformation uses `RequestType`=`Update` and will set `OldResourceProperties` with the previous state properties. In
code it can be determined how the transition should happen. This is for example important when changing an identifier
like a username (e.g.: [alter redhsift username](/lambda/python/cfn_example_functions/manage_user.py#L46-L54))

## Failure handling
The function that backs the resource can encounter errors these should be reported back to CloudFormation via the 
callback!
In order to reduce chances of missing unforeseen errors it is possible to have the body of the `handler` method in one
big `try` block and have a generic `except Exception` to handle unexpected exceptions
(e.g.: [Reporting User errors for manage user function](/lambda/python/cfn_example_functions/manage_user.py#L76-L79)). 