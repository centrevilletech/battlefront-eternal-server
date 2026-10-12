.State.Health.Status == "healthy" and
.HostConfig.NanoCpus == 2000000000 and .HostConfig.Memory == 1073741824 and
.Config.Labels["com.docker.compose.project"] == "battlefront-eternal-server-production" and
.Config.Labels["com.docker.compose.project.config_files"] == ("/data/compose/" + $stackId + "/docker-compose.yml") and
any(.Mounts[]; .Destination == "/data" and .Source == "/opt/battlefront-eternal/server/state" and .RW == true) and
any(.Mounts[]; .Destination == "/game-data" and .Source == "/opt/battlefront-eternal/server/game-data" and .RW == false) and
any(.Mounts[]; .Destination == "/engine" and .RW == false and
    (.Source == "/opt/battlefront-eternal/server/engine" or
     (.Source | test("^/opt/battlefront-eternal/server/releases/[a-f0-9]{40}$"))))
