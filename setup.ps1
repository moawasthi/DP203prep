# Login to Azure Account
Connect-AzAccount

# Define resource group, server, and database names
$resourceGroupName = "rg-contoso-2508-dev"
$location = "EastUS"  # Choose a region close to your location for cost efficiency
$serverName = "sqldbcontoso2508dev"  # Must be unique across all Azure SQL Servers
$adminUsername = "testuser"
$adminPassword = "userTest@123"  # Use a secure password
$databaseName = "TSQLV6"

# Create a Resource Group
New-AzResourceGroup -Name $resourceGroupName -Location $location

# Create an Azure SQL Server
New-AzSqlServer -ResourceGroupName $resourceGroupName `
                -ServerName $serverName `
                -Location $location `
                -SqlAdministratorCredentials (New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminUsername, (ConvertTo-SecureString -String $adminPassword -AsPlainText -Force))

# Create a Basic SQL Database with minimal cost settings
New-AzSqlDatabase -ResourceGroupName $resourceGroupName `
                  -ServerName $serverName `
                  -DatabaseName $databaseName `
                  -Edition "Basic" `
                  -ComputeGeneration "Gen5" `
                  -RequestedServiceObjectiveName "Basic" `
                  -MaxSizeBytes 2147483648  # 2 GB

# Output the database details
Get-AzSqlDatabase -ResourceGroupName $resourceGroupName -ServerName $serverName -DatabaseName $databaseName