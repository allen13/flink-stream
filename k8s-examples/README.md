# Kubernetes examples

Resources that are *not* part of the Helm release, kept here so the operator's other CRDs can be tried by hand
without making them part of every deploy.

| File | Shows |
|---|---|
| [`flinkstatesnapshot.yaml`](flinkstatesnapshot.yaml) | taking a savepoint on demand, as a Kubernetes object |
| [`flinksessionjob.yaml`](flinksessionjob.yaml) | submitting a job into a session cluster declaratively |

The operator also registers `FlinkBlueGreenDeployment`, which runs two versions of a job side by side and cuts
over once the new one is healthy. It is not exercised here because it needs twice the TaskManager memory —
worth reading about if you are planning zero-downtime upgrades.
