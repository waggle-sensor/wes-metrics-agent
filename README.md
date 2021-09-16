# WES Metrics Agent

Plugin to collect and report the system metrics.

## Run Instructions

In order for the GPU information to be collected the container needs access to
the hosts `/sys/kernel/debug` directory.  Therefore, a volume mount is necessary.

Docker:

```
docker run --privileged --rm -it -v /sys/kernel/debug:/sys/kernel/debug --entrypoint /bin/bash joe:latest
```

Kubernetes Pod:

```
apiVersion: v1
kind: Pod
metadata:
  name: my-pod
  labels:
    app: my-app
spec:
  containers:
    - name: app
      image: localhost:5000/joe:latest
      volumeMounts:
      - mountPath: /sys/kernel/debug
        name: sys-kernel-debug
      securityContext:
        privileged: true
  volumes:
  - name: sys-kernel-debug
    hostPath:
      path: /sys/kernel/debug
      type: Directory
```

## Notes: tegrastats

The `tegrastats` tool is used to collect system metrics on NVidia Tegra hardware.
The following resources were used as reference to parse that data:
- https://docs.nvidia.com/jetson/l4t/index.html#page/Tegra%20Linux%20Driver%20Package%20Development%20Guide/AppendixTegraStats.html
- https://github.com/rbonghi/jetson_stats
