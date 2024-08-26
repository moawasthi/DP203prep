# Connect-AzAccount
# The SubscriptionId in which to create these objects
$SubscriptionId = ''
# Set the resource group name and location for your server
$resourceGroupName = "rg-contoso-$(Get-Random)"
$location = "westus2"
# Set an admin login and password for your server
$adminSqlLogin = "SqlAdmin"
$password = "ChangeYourAdminPassword1"
# Set server name - the logical server name has to be unique in the system
$serverName = "sql-db-server-$(Get-Random)"
# The sample database name
$databaseName = "tsqlv6"
# The ip address range that you want to allow to access your server
$startIp = "0.0.0.0"
$endIp = "0.0.0.0"

$STRGACCNAME= "strggetpractical01"
$TypeSTRG= "Standard_LRS"

# Set subscription 
Set-AzContext -SubscriptionId $subscriptionId

# Create a resource group
$resourceGroup = New-AzResourceGroup -Name $resourceGroupName -Location $location

# Create a server with a system wide unique server name
$server = New-AzSqlServer -ResourceGroupName $resourceGroupName -ServerName $serverName -Location $location -SqlAdministratorCredentials $(New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminSqlLogin, $(ConvertTo-SecureString -String $password -AsPlainText -Force))

# Create a server firewall rule that allows access from the specified IP range
$serverFirewallRule = New-AzSqlServerFirewallRule -ResourceGroupName $resourceGroupName -ServerName $serverName -FirewallRuleName "AllowedIPs" -StartIpAddress $startIp -EndIpAddress $endIp

# Create a blank database with an S0 performance level
$database = New-AzSqlDatabase  -ResourceGroupName $resourceGroupName -ServerName $serverName -DatabaseName $databaseName -RequestedServiceObjectiveName "S0" -SampleName "AdventureWorksLT"

# Create a storage account
$storageaccount = New-AzStorageAccount -ResourceGroupName $resourceGroupName -Name $STRGACCNAME -Type $TypeSTRG -Location $location

# Clean up deployment 
# Remove-AzResourceGroup -ResourceGroupName $resourceGroupName
