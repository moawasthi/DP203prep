# Connect-AzAccount
# The SubscriptionId in which to create these objects
Register-AzResourceProvider -ProviderNamespace Microsoft.Databricks

$SubscriptionId = '6a4c1741-e88c-44c0-8a38-22ac6269c95c' # Provide your subscription Id
# Set the resource group name and location for your server
$resourceGroupName = "RG-DATAFACTORY-DEV$(Get-Random)"
$location = "westus2"


$STRGACCNAME= "sabicontosodev$(Get-Random)"
$TypeSTRG= "Standard_LRS"
$containerName  = "masterdata"
$prefixName     = "loop"



#data factory name
$dataFactoryName = "Contoso-ADF-dev$(Get-Random)";

# Set subscription 
Set-AzContext -SubscriptionId $subscriptionId

# Create a resource group
$resourceGroup = New-AzResourceGroup -Name $resourceGroupName -Location $location

# Create a server with a system wide unique server name
#$server = New-AzSqlServer -ResourceGroupName $resourceGroupName -ServerName $serverName -Location $location -SqlAdministratorCredentials $(New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminSqlLogin, $(ConvertTo-SecureString -String $password -AsPlainText -Force))

# Create a server firewall rule that allows access from the specified IP range
#$serverFirewallRule = New-AzSqlServerFirewallRule -ResourceGroupName $resourceGroupName -ServerName $serverName -FirewallRuleName "AllowedIPs" -StartIpAddress $startIp -EndIpAddress $endIp

# Create a blank database with an S0 performance level
#$database = New-AzSqlDatabase  -ResourceGroupName $resourceGroupName -ServerName $serverName -DatabaseName $databaseName -RequestedServiceObjectiveName "S0" -SampleName "AdventureWorksLT"

# Create a storage account
$storageaccount = New-AzStorageAccount -ResourceGroupName $resourceGroupName -Name $STRGACCNAME -Type $TypeSTRG -Location $location

# Create a context object using Azure AD credentials
$ctx = New-AzStorageContext -StorageAccountName $STRGACCNAME -UseConnectedAccount

# Approach 1: Create a container
New-AzStorageContainer -Name $containerName -Context $ctx

$DataFactory = Set-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Location $location -Name $dataFactoryName

New-AzDatabricksWorkspace -Name mydatabricksws -ResourceGroupName $resourceGroupName -Location $location -ManagedResourceGroupName databricks-group -Sku standard

# Clean up deployment 
# Remove-AzResourceGroup -ResourceGroupName $resourceGroupName
