#!/usr/bin/env bash
set -euo pipefail
: "${PORTAINER_URL:?}" "${PORTAINER_API_KEY:?}" "${PORTAINER_ENDPOINT_ID:?}" "${APP_IMAGE:?}"
# The private engine deployer holds this same host-backed lock while updating
# the stack mount, so neither workflow can overwrite the other's newer state.
exec 9>/deployment/releases/.deploy.lock
flock -w 600 9
STACK_NAME=battlefront-eternal-server-production
case "$APP_IMAGE" in ghcr.io/joshualambert/battlefront-eternal-server:sha-*) ;; *) echo 'Unexpected image repository' >&2; exit 1;; esac
api() { curl --fail-with-body --silent --show-error --max-time 300 -H "X-API-Key: $PORTAINER_API_KEY" "$@"; }
base="${PORTAINER_URL%/}/api"
stack="$(api "$base/stacks" | jq -e --arg name "$STACK_NAME" --argjson endpoint "$PORTAINER_ENDPOINT_ID" '[.[]|select(.Name==$name and .EndpointId==$endpoint)] | if length==1 then .[0] else error("Expected existing match server stack") end')"
id="$(jq -r '.Id' <<< "$stack")"
file="$(api "$base/stacks/$id/file" | jq -r '.StackFileContent')"
# Update the image and the authorized two-core budget, preserving private mounts.
file="$(jq -enr --arg file "$file" --arg image "$APP_IMAGE" '
  "ghcr.io/joshualambert/battlefront-eternal-server:sha-[a-f0-9]+" as $pattern |
  if ([$file | scan($pattern)] | length) != 1 then error("Expected one pinned server image")
  else $file | sub($pattern; $image) end |
  "(?m)^(?<indent>[ ]+)cpus:[^\\n]+$" as $cpu |
  if ([scan($cpu)] | length) != 1 then error("Expected one server CPU limit")
  else sub($cpu; "\(.indent)cpus: 2") end')"
payload="$(jq -n --arg file "$file" --argjson env "$(jq '.Env // []' <<< "$stack")" '{stackFileContent:$file,env:$env,pullImage:true,prune:false}')"
api -X PUT "$base/stacks/$id?endpointId=$PORTAINER_ENDPOINT_ID" -H 'Content-Type: application/json' --data "$payload" | jq '{Id,Name}'
for attempt in $(seq 1 60); do
 state="$(api "$base/endpoints/$PORTAINER_ENDPOINT_ID/docker/containers/$STACK_NAME/json")"
 if jq -e --arg image "$APP_IMAGE" '.State.Health.Status=="healthy" and .Config.Image==$image and .HostConfig.NanoCpus==2000000000 and .HostConfig.Memory==1073741824' <<< "$state" >/dev/null; then echo 'Match server deployment healthy (2 cores, 1 GiB)'; exit 0; fi
 sleep 5
done
echo 'Match server failed to become healthy' >&2
exit 1
