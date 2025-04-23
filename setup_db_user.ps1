$target = Read-Host -Prompt "Target db"
$schema = Read-Host -Prompt "Proxy schema"
$creds = Get-Credential

$username = $creds.Username
if ($schema) {
  $username += "[$schema]"
  $env:DBT_DB_SCHEMA = $schema
}
else {
  $env:DBT_DB_SCHEMA = $username
}
$env:DBT_ENV_SECRET_USER = $username
$env:DBT_ENV_SECRET_PASS = $creds.GetNetworkCredential().password
$env:DBT_DB_TARGET = $target

$env:DBT_PROFILES_DIR = Get-Location


# midlertidig fiks for thin client fra utviklerimage
Remove-Item -Path Env:https_proxy
$env:ORA_PYTHON_DRIVER_TYPE = "thin"
echo "ORA_PYTHON_DRIVER_TYPE: $env:ORA_PYTHON_DRIVER_TYPE"