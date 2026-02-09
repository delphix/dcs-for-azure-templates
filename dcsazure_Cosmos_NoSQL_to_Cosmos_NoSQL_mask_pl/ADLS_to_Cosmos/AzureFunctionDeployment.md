# Azure Function Deployment Using Azure CLI (ZIP Deployment)

## Overview
This document describes the standard process for deploying an Azure Function App using the Azure CLI with ZIP deployment. ZIP deployment packages the function app code into a single archive, uploads it to Azure, and runs the application in *run-from-package* mode.

---

## Prerequisites
- Azure CLI installed on the local machine
- Access to an Azure subscription with permission to deploy Azure Function Apps
- ADLS_to_Cosmos ZIP file containing the Azure Function App code and configuration

---

## Login to Azure Using Azure CLI

1. Open **Command Prompt** or **PowerShell** on your local machine.
1. Run the following command to authenticate:

   ```bash
   az login
1. A browser window will open prompting you to sign in with your Azure credentials.

1. After successful authentication, select the appropriate subscription if prompted.

## Deploy the Azure Function App

1. Use the following command to deploy the ZIP package to the Azure Function App:

    ```
    az functionapp deployment source config-zip \
      --resource-group <RESOURCE_GROUP_NAME> \
      --name <FUNCTION_APP_NAME> \
      --src "<FULL_PATH_TO_ZIP_FILE>"
      --build-remote true

1. Parameters

* `resource-group` : The Azure resource group that contains the ADLS_to_Cosmos Azure Function App
* `name` : The name of the ADLS_to_Cosmos Azure Function App
* `src` : The full path to the ADLS_to_Cosmos ZIP package containing the Function App code
    
## Deployment Result

1. If the deployment is successful, Azure CLI returns a response similar to the following:

* Deployment status: Succeeded
* Deployment message: Created via a push deployment
* Application mode: Run-from-package (read-only)

1. This confirms that the ZIP package has been uploaded successfully and mounted by the Function App.

## Notes

1. Azure Cloud Shell does not support ZIP deployment for Azure Function Apps due to authentication and file system limitations.