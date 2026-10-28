# Native commands with no output can produce $null even through a string cast.
# Join an explicit array before trimming, and retain only running services.
function Read-RunningImages {
    param([string[]]$Names = @('desk','trainer','maintenance'))
    $images=@{}
    foreach ($serviceName in $Names) {
        $ids=@(& docker compose --profile ml ps -q $serviceName)
        if ($LASTEXITCODE -ne 0) { throw "Cannot inspect running service: $serviceName" }
        $containerId=($ids -join '').Trim()
        if (!$containerId) { continue }
        $output=@(& docker inspect --format '{{.Image}}' $containerId)
        if ($LASTEXITCODE -ne 0) { throw "Cannot capture image: $serviceName" }
        $imageId=($output -join '').Trim()
        if ($imageId -notmatch '^sha256:[a-f0-9]{64}$') { throw "Invalid running image: $serviceName" }
        $images[$serviceName]=$imageId
    }
    return $images
}
