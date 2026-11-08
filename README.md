Stream-to-Model Control Plane
A Kubernetes-native platform for connecting external data feeds to machine learning models, with full visibility into every stage of the pipeline.

What This Is
This project is a control plane for data pipelines. It lets you declare what you want using simple Kubernetes resources, and the system takes care of the rest: pulling data from external APIs, cleaning it, joining streams together, feeding features to machine learning models, and producing predictions back into a message queue.

It is built on Kubernetes, Redpanda (a Kafka-compatible event broker), and Apache Flink (for stream processing). A live web dashboard shows you the entire pipeline in real time, from the external API to the final prediction.

Why We Built It
Machine learning models rarely fail because the model itself is bad. They fail because the data feeding them breaks, goes stale, or stops matching reality. A model that scored perfectly in testing becomes useless in production when:

The upstream API changes its format without warning.

A stream processing job silently stops saving its progress.

The live data slowly drifts away from what the model was trained on.

Nobody notices any of this until the business loses money.

Most teams solve this with a pile of scripts, manual Kubernetes manifests, and ad-hoc monitoring. We wanted something cleaner: a single system where every pipeline stage is declared as code, reconciled automatically, and visible at a glance.

The Core Idea
You describe what you want, and the platform makes it happen.

You write a small YAML file that says "I want a feed pulling data from this API every 15 minutes, publishing to this topic." That is called a Feed resource. The platform reads it, deploys a connector pod, configures the polling, registers the data schema, and starts streaming.

You write another YAML file that says "I want to join these two topics together and produce a new feature topic." That is called a ModelQuery resource. The platform reads it, spins up a Flink job with the SQL you provided, manages its checkpoints, and starts emitting features.

Your machine learning model then subscribes to that feature topic and produces predictions. The platform monitors all of it, and the dashboard shows the entire graph.

You never manually wire anything up. You never run kubectl commands in a specific order. You declare intent, and the cluster converges.

How It Works
The system has three main parts.

First, there are the custom resources. These are the YAML files you write. They describe the feeds and queries you want. Kubernetes stores them as the source of truth.

Second, there are the controllers. These are background processes that watch your resources and make the real world match what you declared. When you create a Feed, the controller deploys a Redpanda Connect pod and configures it. When you create a ModelQuery, the controller deploys a Flink job and manages its lifecycle. If you delete a resource, the controller cleans up the infrastructure (though it keeps your data topics by default, so you can recover).

Third, there is the event broker and the stream processor. Redpanda holds all the topics and messages. Flink reads from those topics, transforms the data, and writes to new topics. The models subscribe to the final topics and emit predictions.

The dashboard reads the status of every resource from Kubernetes and shows you the live picture.

Key Design Choices
Kubernetes is the source of truth. Everything you want is declared in YAML. The cluster is responsible for converging to that state. Nothing is created imperatively.

Resources describe results, not steps. You say "I want this topic to exist with this schema and this data flowing into it." You do not say "first create the pod, then configure the connector, then register the schema." The controller figures out the steps.

Reconciliation is safe to repeat. If a controller crashes and restarts, it will reconcile everything again. It is idempotent. Nothing breaks, nothing duplicates.

Schemas are explicit and versioned. Every topic has a schema registered in a schema registry. No message reaches a model without validation. If a producer sends bad data, it goes to a dead letter queue instead.

Readiness is based on observation. Every resource reports its own status. It tells you whether it is Ready, whether it is Progressing, whether it is Degraded, and crucially, whether its data is Fresh. A pod can be running and healthy while the data it produces is stale. The dashboard shows both signals separately.

Secrets never leave the cluster. Credentials, SQL, and connector configuration stay server-side. The browser only sees sanitized status messages.

Deletion is safe by default. When you delete a Feed or a ModelQuery, the controller removes the infrastructure but keeps the topics and schemas. You can recover or recreate without losing history.

The Stack
The platform uses four main technologies.

Kubernetes provides the API, the custom resource definitions, the RBAC, the watches, and the reconciliation model. It is the foundation.

Redpanda is the event broker. It is Kafka-compatible, so any Kafka client or tool works. It stores all the topics, handles replication across nodes, and provides durability for the entire pipeline.

Redpanda Connect handles ingestion. Each Feed gets its own dedicated Connect pod. It polls the external API, validates incoming messages against the schema, and writes them to the raw topic. If a message fails validation, it is routed to a dead letter queue instead of breaking the pipeline.

Apache Flink handles stream processing. It runs the ModelQuery SQL jobs. It performs stateful joins, windowing, and aggregation. It writes enriched features to new topics. Flink is managed by the Flink Kubernetes Operator, which means the platform treats Flink jobs as Kubernetes resources like any other.

Triton Inference Server or a custom Python consumer handles the models. Models subscribe to one or more feature topics and emit predictions back into Redpanda.

Why This Is Extensible
Adding a new data source does not require touching the platform code. You write a Feed resource, apply it, and the controller handles the rest. No redeployment, no code changes.

Adding a new feature stream does not require touching the platform code. You write a ModelQuery resource, and Flink starts processing.

Adding a new model does not require touching the platform code. You deploy a container that subscribes to the relevant feature topics, and the platform observes it in the dashboard.

Adding a new dashboard view requires only frontend changes. The topology API already exposes the resource graph and status conditions.

This is the whole point. The platform is a thin layer over Kubernetes that makes streaming pipelines declarative. The rest is composed from standard, replaceable pieces.

What You Can See
The dashboard has five main views.

Topology shows the entire pipeline as a graph. You can see every external API, every connector, every topic, every Flink query, and every model. The graph updates as resources change. Clicking a node shows its configuration, its status, and for topics, its schema.

Feeds shows a table of every Feed resource. You can see its phase (Ready, Reconciling, Degraded), its freshness (how long since the last message), and its dead letter queue count. If a feed is failing, you see it immediately.

Queries shows every ModelQuery and its Flink health. You see checkpoint duration, TaskManager count, and the SQL being executed. A failing checkpoint flips the query to Degraded.

Observability shows the live performance of your models. You see MAE, R², bias, and feature drift. You see whether the live data matches the training distribution. You see whether predictions are actually good.

Price Forecast is a business-facing view. It shows the day-ahead energy price prediction, a what-if simulator, and the output curve.

Every view is built on the same foundation: Kubernetes status conditions and topic metrics.

What You Can Do
You can create a feed by writing a YAML file and applying it. The platform deploys a connector, registers the schema, and starts streaming. The dashboard shows it within seconds.

You can pause and resume the stream from the dashboard. You can inspect any node in the graph. You can see exactly what data is flowing where, and what its schema looks like.

You can see whether your models are accurate. You can see whether the data they are receiving has drifted. You can trigger alerts when freshness degrades, when checkpoints fail, or when predictions drift.

The platform gives you the same operational confidence for ML pipelines that Kubernetes gives you for microservices.

Scaling Path
The current version is a production-shaped local demo. It runs one real feed, one continuous Flink query, one prediction consumer, and one live dashboard. That is enough to prove the concept and to run a real pipeline end to end.

Future milestones add multi-namespace isolation, a web form for creating feeds, alerting on degradation, multi-cluster operation, cloud identity integration, public ingress, schema evolution enforcement, and automated retraining triggers.

Each of these is additive. None of them require rewriting the core.

Getting Started
Apply the custom resource definitions to your cluster. Deploy the controllers. Apply a sample Feed and ModelQuery. Watch them reconcile with kubectl. Open the dashboard.

Everything runs locally in a single Kubernetes namespace. No cloud account required.

The whole point is that you describe what you want, and the system produces it. The dashboard shows you what happened. The pipeline does the rest.


```bash
pip install -r requirements.txt
python -m src.fetch 2026-04-20 2026-11-07   # real data (else synthetic is used)
python main.py                              # the pitch table: £, rank, % of perfect
pytest -q                                   # guard rails
make help                                   # everything else
```
