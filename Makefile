# flink-stream - a Flink learning project on local Kubernetes
#
#   make up        one command from nothing to a running environment
#   make help      everything else

SHELL := /bin/bash
.DEFAULT_GOAL := help

NAMESPACE ?= flink-stream
RELEASE   ?= fs
export NAMESPACE RELEASE

# The JDK used for local (non-Docker) builds. The image build carries its own.
JAVA_HOME_17 := $(shell /usr/libexec/java_home -v 17 2>/dev/null)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------------------------- lifecycle

.PHONY: up
up: bootstrap image deploy ## Bootstrap the cluster, build the image and deploy everything

.PHONY: bootstrap
bootstrap: ## Install cert-manager and the Confluent Flink Kubernetes Operator
	./scripts/bootstrap.sh

.PHONY: image
image: ## Build the job image on confluentinc/cp-flink
	./scripts/build-image.sh

.PHONY: deploy
deploy: ## helm upgrade --install the flink-stream chart
	./scripts/deploy.sh

.PHONY: deploy-lite
deploy-lite: ## Deploy the ~5 GiB profile (1 broker, DataStream job only)
	./scripts/deploy.sh --lite

.PHONY: redeploy
redeploy: image ## Rebuild the image and restart the Flink jobs with it
	./scripts/deploy.sh
	kubectl -n $(NAMESPACE) delete pod -l component=jobmanager --ignore-not-found

.PHONY: down
down: ## Uninstall the release (keeps volumes)
	./scripts/teardown.sh

.PHONY: clean
clean: ## Uninstall everything including volumes, the operator and cert-manager
	./scripts/teardown.sh --all

# ---------------------------------------------------------------------------------------------- build

.PHONY: build
build: ## Compile and package the job jar locally (needs a JDK 17)
	cd flink-jobs && JAVA_HOME=$(JAVA_HOME_17) ./mvnw -B clean package

.PHONY: test
test: ## Run the unit and MiniCluster tests
	cd flink-jobs && JAVA_HOME=$(JAVA_HOME_17) ./mvnw -B test

.PHONY: pdf
pdf: ## Render docs/pdf/*.md to PDF (needs pandoc + xelatex)
	./scripts/build-pdf.sh

.PHONY: lint
lint: ## Render and lint the Helm charts
	helm lint charts/flink-stream
	helm template $(RELEASE) charts/flink-stream -n $(NAMESPACE) > /dev/null && echo "chart renders"
	helm template $(RELEASE) charts/flink-stream -n $(NAMESPACE) \
		-f charts/flink-stream/values-lite.yaml > /dev/null && echo "lite profile renders"
	helm lint charts/flink-stream-eks -f charts/flink-stream-eks/values-msk-iam.yaml
	helm template $(RELEASE) charts/flink-stream-eks -n $(NAMESPACE) \
		-f charts/flink-stream-eks/values-msk-iam.yaml > /dev/null && echo "eks chart renders"
	@# The EKS chart carries byte-identical copies of the lab SQL, because Helm cannot read files outside a
	@# chart directory. This is what stops the two from drifting apart unnoticed.
	@for f in charts/flink-stream/files/sql/[1-5]0-*.sql; do \
		diff -q "$$f" "charts/flink-stream-eks/files/sql/$$(basename $$f)" >/dev/null \
			|| { echo "DRIFT: $$f differs from the EKS chart's copy - run 'make sync-eks-sql'"; exit 1; }; \
	done && echo "eks SQL copies in sync"

.PHONY: sync-eks-sql
sync-eks-sql: ## Re-copy the lab SQL into charts/flink-stream-eks (00-catalog.sql is generated there, not copied)
	cp charts/flink-stream/files/sql/[1-5]0-*.sql charts/flink-stream-eks/files/sql/
	cp charts/flink-stream/files/yugabyte-schema.sql charts/flink-stream-eks/files/db-schema.sql
	@echo "synced"

# ---------------------------------------------------------------------------------------------- inspect

.PHONY: status
status: ## Pods, Flink jobs, topics and registered schemas
	./scripts/status.sh

.PHONY: ui
ui: ## Port-forward every web UI
	./scripts/port-forward.sh

.PHONY: topics
topics: ## List Kafka topics with their shard counts
	./scripts/kafka.sh topics

.PHONY: sql-client
sql-client: ## Open the Flink SQL client (needs flink.session.enabled=true)
	./scripts/sql-client.sh

.PHONY: ysql
ysql: ## Open a ysqlsh shell on YugabyteDB
	./scripts/ysql.sh

.PHONY: logs-datastream
logs-datastream: ## Tail the DataStream job's TaskManager logs
	kubectl -n $(NAMESPACE) logs -f -l app=$(RELEASE)-flink-stream-datastream,component=taskmanager --max-log-requests 6

.PHONY: logs-sql
logs-sql: ## Tail the SQL job's TaskManager logs
	kubectl -n $(NAMESPACE) logs -f -l app=$(RELEASE)-flink-stream-sql,component=taskmanager --max-log-requests 6

.PHONY: logs-generator
logs-generator: ## Tail the data generator
	kubectl -n $(NAMESPACE) logs -f -l app.kubernetes.io/component=generator

.PHONY: logs-operator
logs-operator: ## Tail the Flink operator
	kubectl -n flink-operator logs -f deploy/flink-kubernetes-operator
