# AzureFunctionForSplunkVS
Azure Function sends Azure Monitor telemetry to Splunk - coded in C# / Visual Studio.

# Azure Function For Splunk
Azure Function code that sends telemetry from Azure resources to a Splunk Enterprise or Splunk Cloud instance.

It consumes Metrics, Diagnostic Logs and the Activity Log according to the techniques defined by Azure Monitor, which provides highly granular and real-time monitoring data for Azure resources, and passes those selected by the user's configuration along to Splunk. 

Here are a few resources if you want to learn more about Azure Monitor:<br/>
* [Overview of Azure Monitor](https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-overview)
* [Overview of Azure Diagnostic Logs](https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-overview-of-diagnostic-logs)
* [Overview of the Azure Activity Log](https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-overview-activity-logs)
* [Overview of Metrics in Microsoft Azure](https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-overview-metrics)  

## Important Security Note
The HEC endpoint for a Splunk instance is SSL encrypted. At this time, this function CAN ignore the validity of the certificate. To do so, do not provide App Setting 'splunkCertThumbprint' or leave it blank. To ENABLE cert validation, make the value of that setting the thumbprint of the cert.

## Solution Overview

At a high level, the Azure Functions approach to delivering telemetry to Splunk does this:
* Azure resources deliver telemetry to event hubs
* Azure Function is triggered by these messages
* Azure Function delivers the messages to Splunk

Azure Functions are arranged hierarchically as a Function App containing individual functions within. An individual function is triggered by a single event hub. Regarding logs from Azure Monitor, each log category is sent to its own hub. Each Azure Resource Provider that emits logs may emit more than one log category. Similarly, metrics are sent to a hub as configured by the user. Hence, there may be many hubs for the Function App to watch over. These hubs may be in one or more Event Hub Namespaces.

The solution leverages the capacity of an Azure Function to be triggered by arrival of messages to an Event Hub. The messages are aggregated by the Azure Functions back end so they arrive at the function already in a batch where the size of the batch depends on current message volume and settings. The batch is examined, the properties of each event are augmented, and then the events are sent via the selected output binding to the Splunk instance.  

### Cloud-based Splunk using HTTP Event Collector Output Binding

![AzureFunctionPlusHEC](images/AzureFunctionPlusHEC.PNG)
The image shows only Splunk VM, but the solution targets Splunk Cloud as well. The Splunk VM may be Splunk Enterprise or a Forwarder.  

### Premises-based Splunk using Azure Relay Hybrid Connection Output Binding

![AzureFunctionPlusRelay](images/AzureFunctionPlusRelay.PNG)
This architecture utilizes a different add-on that is enabled to get events via Service Bus Relay and then add them to the Splunk index.

## Installation and Configuration

Installation and Configuration tasks for the overall solution fall into a few buckets:

* Diagnostics profiles 
* Event hubs
* Splunk instance
* Azure Function
* Host.json

### Diagnostics Profiles
Each resource to be monitored must have a diagnostics profile created for it. This can be done in the portal, but more likely you'll want to write a script to configure existing resources and update your solution templates to create these profiles upon creation of the resource. Here's a place to start:

[Automatically enable Diagnostic Settings at resource creation using a Resource Manager template](https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-enable-diagnostic-logs-using-template)

### Event hubs

As mentioned, logs and metrics are sent through event hubs. Event hubs are created automatically by the Azure resource providers that need to write the information, so at the outset all you need to do is create the Event Hub Namespace. Here's how to do this in the portal:

[Create an Event Hubs namespace and an event hub using the Azure portal](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create)

You will need to provide credentials to the Azure Function so it can read the hubs. On one end of the security spectrum you could provide the RootManageSharedAccessKey to all functions for all hubs within the namespace. At the other end of the spectrum (principal of least required authority) you can create a policy for each hub with Listen access and provide that credential on a function-by-function basis.

An example of copying the connection string (NOT just the key) associated with the RootManageSharedAccessKey policy is given on [this page](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create) at the bottom of the page.  

To create a least permissions policy:
* Select the hub from the list of hubs on the event hub namespace blade
* Select "Shared access policies"
* Click "+ Add"
* Give it a name, select "Listen", click "Create" button.
* Once it's created, re-enter the properties for that new policy and copy the connection string (NOT just the key).

### Splunk Instance

#### Using HEC output binding
Configuration of the Splunk instance amounts to opening the HEC endpoint and creating/copying the authentication token. The endpoint address and token value must be entered as settings into the Function App.

Instructions for opening the endpoint and creating/copying the token are on this Splunk webpage:  

[HTTP Event Collector walkthrough](http://dev.splunk.com/view/event-collector/SP-CAAAE7F#usinghttpeventcollector)

#### Using Azure Relay output binding
Configuration of the Splunk instance involves installing an add-on whose job it is to connect to the Azure Relay Hybrid Connection, receive messages sent over that channel and then add those events to the Splunk instance.

This add-on is located [here](https://github.com/sebastus/AzureRelayHCAddonForSplunk). Instructions for installing it are located in the README.md of that repo.

Instructions for creating the Azure Relay Hybrid Connection are in the first two steps of this page:

[Get started with Relay Hybrid Connections](https://docs.microsoft.com/en-us/azure/service-bus-relay/relay-hybrid-connections-dotnet-get-started)


### Azure Function

There are several ways to create an Azure Function and load your code into it. Here's one such example:

[Create your first function using the Azure CLI](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-azure-function-azure-cli)

This technique requires that your code be referencable in a github repo, and this is exactly what we need. If you examine a couple of the "insights-logs-" folders in this repo, you'll see a file called "function.json" in each. Function.json contains configuration details for the event hub that will trigger the function. The only setting of interest is "connection". This should contain the name of the setting that contains your event hub connection string for that hub / log category. You can have one connection string for all hubs or one for each or any mix thereof.

Because the repo needs to contain settings specific to your installation, I recommend you fork this repo and make your changes there. Then provide the address of your fork in the example above to populate your function app.

Note that the actual settings are not in the code. These are provided by you in the portal.

If you want to automate the creation of your Azure Function, there is a solution template that accomplishes this located here:

[Azure Function Deployment](https://github.com/sebastus/AzureFunctionDeployment)

Once the Function App exists, add the appropriate values into settings:

#### Using HEC output binding

* outputBinding - HEC
* splunkAddress - e.g. https://YOURVM.SOMEREGION.cloudapp.azure.com:8088/services/collector/event
* splunkToken - e.g. 5F1B2C8F-YOUR-GUID-HERE-CE29A659E7D1

#### Using Azure Relay output binding

* outputBinding - Relay
* relayNamespace - namespace prefix of your service bus
* relayPath - the name of your hybrid connection
* policyName - SAS key access policy name
* policyKey - SAS key value

## Host.json

In host.json is an array named "functions". That array lists the functions that should be executed. There is a function in the master repo for each of the known log categories and one for metrics. 

The functions array is seeded with only one function name as an example - that for "insights-logs-workflowruntime". 

Once you know all of the logs that you will be following, enter the names of those functions into the array. If you delete the array, all functions will run. If the function runs but the hub doesn't exist, the function will drop a lot of errors into the function app logs.

