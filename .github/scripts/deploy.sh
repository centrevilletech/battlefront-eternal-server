#!/usr/bin/env bash
set -euo pipefail
: "${PORTAINER_URL:?}" "${PORTAINER_API_KEY:?}" "${PORTAINER_ENDPOINT_ID:?}" "${APP_IMAGE:?}"
STACK_NAME=battlefront-eternal-server-production
case "$APP_IMAGE" in ghcr.io/joshualambert/battlefront-eternal-server:sha-*) ;; *) echo 'Unexpected image repository' >&2; exit 1;; esac
api() { curl --fail-with-body --silent --show-error --max-time 300 -H "X-API-Key: $PORTAINER_API_KEY" "$@"; }
base="${PORTAINER_URL%/}/api"
stack="$(api "$base/stacks" | jq -e --arg name "$STACK_NAME" --argjson endpoint "$PORTAINER_ENDPOINT_ID" '[.[]|select(.Name==$name and .EndpointId==$endpoint)] | if length==1 then .[0] else error("Expected existing match server stack") end')"
id="$(jq -r '.Id' <<< "$stack")"
file="$(api "$base/stacks/$id/file" | jq -r '.StackFileContent')"
# Change only this service image. Preserve all private mounts and runtime settings.
file="$(jq -enr --arg file "$file" --arg image "$APP_IMAGE" '
  "ghcr.io/joshualambert/battlefront-eternal-server:sha-[a-f0-9]+" as $pattern |
  if ([$file | scan($pattern)] | length) != 1 then error("Expected one pinned server image")
  else $file | sub($pattern; $image) end')"
payload="$(jq -n --arg file "$file" --argjson env "$(jq '.Env // []' <<< "$stack")" '{stackFileContent:$file,env:$env,pullImage:true,prune:false}')"
api -X PUT "$base/stacks/$id?endpointId=$PORTAINER_ENDPOINT_ID" -H 'Content-Type: application/json' --data "$payload" | jq '{Id,Name}'
for attempt in $(seq 1 60); do
 state="$(api "$base/endpoints/$PORTAINER_ENDPOINT_ID/docker/containers/$STACK_NAME/json")"
 if jq -e --arg image "$APP_IMAGE" '.State.Health.Status=="healthy" and .Config.Image==$image' <<< "$state" >/dev/null; then echo 'Match server deployment healthy'; exit 0; fi
 sleep 5
done
echo 'Match server failed to become healthy' >&2
exit 1
