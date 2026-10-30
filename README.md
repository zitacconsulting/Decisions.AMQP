# Decisions AMQP Module

> ⚠️ **Important:** Use this module at your own risk. See the **Disclaimer** section below.

## Overview

**Decisions AMQP Module** connects the Decisions platform to any **AMQP 1.0** message broker: ActiveMQ Artemis, ActiveMQ Classic, RabbitMQ 4.x, Azure Service Bus, Apache Qpid, Solace and others. It plugs into the standard Decisions message queue framework, so AMQP queues are created and handled like any other Decisions queue (**Jobs & Events > Messaging**), and adds:

- **Broker-side message filters**, configured with simple rules, an expression, or raw AMQP filters, so unwanted messages never reach Decisions.
- **Rotating credentials:** a flow step that stores new credentials on a queue and restarts it on every server.
- Steps to **send**, **receive**, and **request/reply**.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Message Filters](#message-filters)
- [Rotating Credentials](#rotating-credentials)
- [Flow Steps](#flow-steps)
- [Handler Flow Data](#handler-flow-data)
- [Broker Notes](#broker-notes)
- [Cluster Deployments](#cluster-deployments)
- [Monitoring](#monitoring)
- [Building from Source](#building-from-source)
- [Disclaimer](#disclaimer)

## Features

### Connectivity
- AMQP 1.0 over TCP, with TLS (TLS 1.2/1.3) and an option to allow untrusted certificates
- Authentication: username/password (SASL PLAIN), anonymous, client certificate (SASL EXTERNAL / mutual TLS), and access tokens via CBS (Azure Service Bus / Event Hubs with Microsoft Entra ID or SAS tokens)
- RabbitMQ OAuth 2.0: the access token goes in the password
- Virtual host / open hostname (RabbitMQ `vhost:<name>`)
- Heartbeat (idle timeout) and connection timeout
- Connection settings at system or project level, overridable per queue; **credentials overridable per queue on their own**

### Receiving
- Queues and topics, with optional durable and shared durable subscriptions
- Broker-side filters: simple rules, JMS selectors, RabbitMQ SQL filter expressions, RabbitMQ stream offsets and Bloom filters, or any custom AMQP filter
- **Filter safety check:** the queue stops if the broker doesn't confirm that it applies the filter, instead of silently delivering every message
- Messages are removed from the broker only after the handler flow has completed (configurable), with a choice of what happens when the flow fails: mark as failed, release, reject (dead-letter), or accept
- Prefetch (link credit)
- Two consume modes: all servers/threads as competing consumers, or one connection at a time across the cluster (lease based, with automatic failover)
- Automatic reconnect when the connection or link is lost, with a growing delay between failed attempts (1, 2, 4 ... 60 seconds), so a broker that rejects the connection (for example expired credentials) isn't flooded. The delay resets when the queue is saved, resynced or gets new credentials

### Flow Steps
- **Send AMQP Message**: application properties, message annotations, standard properties, priority, TTL, durability, and an optional wait for a reply (request/reply)
- **Receive AMQP Message**: wait for one message, with an optional filter override
- **Update AMQP Queue Credentials**: rotate credentials and restart the queue on all servers
- The built-in **Message Queues** steps (Enable, Disable, Pause, Resume, Resync, Shut Down, Test) also work with AMQP queues

## Requirements

- **Decisions Platform**: 9.32 or later. Checked against 9.32, 10.0, 10.1, 10.2 and 10.3.
- **Decisions.MessageQueues** module (installed automatically as a dependency)
- **Broker**: any AMQP 1.0 broker. RabbitMQ needs 4.0 or later (native AMQP 1.0); filters on RabbitMQ need streams, and SQL filters need RabbitMQ 4.2 or later.

The module bundles `Amqp.Net.dll` (AMQPNetLite 2.4.3), the same file the built-in Decisions.ActiveMQ module ships, so the two modules can be installed side by side.

## Installation

1. Build the module (see [Building from Source](#building-from-source)) or take the pre-built `Decisions.AMQP.zip`.
2. In Decisions, go to **System > Administration > Features** and choose **Install Module**.
3. Upload `Decisions.AMQP.zip`.
4. In each project that will use AMQP, add **Decisions.AMQP** as a project dependency (**Manage > Configuration > Dependencies**).

## Quick Start

1. **Connection settings**: **System > Settings > Message Queue Settings > AMQP Settings** (or the project's **Jobs & Events > Messaging > Settings**). Set the broker host, TLS, port and credentials.
2. **Create a queue**: **Jobs & Events > Messaging > Queues > Add AMQP Queue**.
   - Choose the **Broker Type**. The help text then explains the address format and which filters that broker supports.
   - Enter the **Address (Receive From)**, e.g. `orders` (Artemis/Classic/Qpid), `/queues/orders` (RabbitMQ), `my-topic/Subscriptions/decisions` (Service Bus).
   - Optionally add a **Message Filter**. Use **Test** on the queue to check that the broker accepts it.
3. **Create a handler flow** and bind it as a **Queue Handler** to the queue. The flow receives the message body, the application properties as **Headers** and the AMQP properties as **Other Data** (see [Handler Flow Data](#handler-flow-data)).
4. Messages start flowing as soon as the handler exists and the queue is enabled.

## Configuration

Queue properties are grouped in sections:

| Section | What it holds |
|---|---|
| **1 Definition** | Broker type, receive address, address type (queue/topic), optional publish address |
| **2 Message Filter** | Filter type, rules, expression, custom filters, generated filter preview, the "stop if broker ignores filter" safety check |
| **3 RabbitMQ Stream** | Stream start position (first/last/next/offset/timestamp/age) and Bloom filter values. Only shown for RabbitMQ |
| **4 Receiving** | Consume mode, prefetch, when to remove messages, what to do when the flow fails, durable/shared subscriptions |
| **5 Connection** | Override the settings' host, TLS, port, virtual host, timeouts |
| **6 Credentials** | Override the settings' authentication, username, password/token, client certificate. Shows when a flow last updated them |
| **7 Advanced** | Container ID override, CBS token audience |

Every option has a help note in the editor. Settings can also be set with environment variables named `DECISIONS_AMQPQUEUE_<PROPERTY>`, for example `DECISIONS_AMQPQUEUE_SERVER`.

## Message Filters

Filters are evaluated **by the broker**: messages that don't match stay on the broker (for other consumers) and never reach Decisions.

| Filter Type | Use it when |
|---|---|
| **None** | You want every message on the address |
| **Simple Rules** | You want to match on application properties, correlation ID, message ID or priority (and subject/content type on RabbitMQ) without learning a filter syntax. Rules are combined with AND or OR |
| **Filter Expression** | You want to write the filter yourself: a JMS selector for Artemis/Classic/Qpid/Solace/generic brokers, or an AMQP SQL expression for RabbitMQ |
| **Custom AMQP Filters** | You need a broker-specific filter: legacy topic/direct bindings, RabbitMQ property filters, numeric descriptors, and so on |

Rule operators: equals, not equals, greater/less (or equal), starts with, ends with, contains, is one of, is not one of, exists, does not exist. Values can be text, numbers or true/false. Special characters are escaped automatically. **Generated Filter** shows exactly what is sent to the broker.

Examples of what the rules generate:

```
Artemis:   (region = 'EU') AND (amount > 1000) AND (customer LIKE 'AC\_ME%' ESCAPE '\')
RabbitMQ:  (a.region = 'EU') AND (a.amount >= 1000)
```

### Filter safety check

Brokers ignore filters they don't support, and a receiver without its filter gets *every* message. With **Stop If Broker Ignores Filter** on (the default), the queue checks the broker's attach reply and stops with a clear error when a filter wasn't accepted.

On RabbitMQ the reply alone isn't enough. RabbitMQ 4.2 echoes SQL and property filters back on classic and quorum queues but doesn't apply them (tested on 4.2.9). The module therefore also checks the filter capabilities RabbitMQ only advertises on streams.

## Rotating Credentials

Many customers use short-lived credentials: passwords rotated by a vault, OAuth 2.0 access tokens, SAS tokens. To keep queues connected:

1. Create a **scheduled flow** (Jobs & Events > Scheduled Jobs) that runs well before the credentials expire.
2. In the flow, fetch the new credentials, for example with a REST call to your identity provider's token endpoint or a Key Vault step.
3. Add **Update AMQP Queue Credentials** and map the new **Password or Token** (and **Username** if it changes).

The step:
- stores the credentials on the queue, turning on **Override Credentials** and keeping the authentication method from the settings if the queue didn't override them yet,
- saves the queue, which restarts it on this server and tells every other server in the cluster to reload it from the database and restart. Running handler flows finish first, and unprocessed prefetched messages go back to the broker,
- skips queues whose credentials didn't change, so running it more often than needed is harmless,
- reconnects immediately, even if the queue was waiting between failed attempts because the old credentials had already expired,
- outputs **Restarted Queues**, the queues it restarted.

It can update the selected queue, a queue named at runtime, or **all AMQP queues in the project**.

| Broker / auth | What to put in "Password or Token" |
|---|---|
| Username/password (any broker) | The password |
| RabbitMQ with OAuth 2.0 | The access token (JWT). Username is ignored by RabbitMQ |
| Azure Service Bus, Shared Access Policy | The key (username = policy name) |
| Azure Service Bus / Event Hubs, Entra ID (CBS) | The access token (JWT); Token Type "JWT" |
| Azure Service Bus, SAS token (CBS) | The full `SharedAccessSignature sr=...` token; Token Type "SAS" |

## Flow Steps

All steps are under **Integration > AMQP**.

### Send AMQP Message
Sends a message using the queue's connection, to its **Publish Address** (or receive address), or to **Address Override**.

| Input | Notes |
|---|---|
| Message | Message body (text) |
| Address Override | Optional target address, e.g. `/exchanges/orders/eu` on RabbitMQ |
| Application Properties | Name/value pairs. Receivers can filter on these |
| Content Type, Subject, Message ID, Correlation ID, Group ID, Reply To | Standard AMQP properties. Message ID is generated when empty |
| Priority, Time To Live (ms), Durable | Header fields |
| Message Annotations | e.g. `x-stream-filter-value` for RabbitMQ stream Bloom filters, `x-opt-partition-key` for Azure |

**Body Format**: *Automatic* sends a binary data section for RabbitMQ and Azure Service Bus (what their SDKs and AMQP 0.9.1 consumers expect), and an AMQP string value for JMS brokers (which become JMS TextMessages).

**Wait For Reply** (request/reply): the step waits for a reply with a matching correlation ID and has the outcomes **Reply Received** and **Timeout**. With an empty **Reply Address** the broker creates a temporary reply queue (tested with Artemis and RabbitMQ 4.2). With a fixed reply address the reply is picked out with a broker filter on the correlation ID, which needs filter support on that address.

### Receive AMQP Message
Waits up to **Timeout (seconds)** for one message on the queue's address (with the queue's filter, or **Filter Expression Override**), removes it from the broker, and returns it on **Message Received**. If nothing arrives, the outcome is **No Message**.

### Update AMQP Queue Credentials
See [Rotating Credentials](#rotating-credentials).

## Handler Flow Data

Handler flows get the standard Decisions message queue inputs:

| Input | Content |
|---|---|
| Message | `MessageQueueMessage`: `StringPayload` is the body as text (data sections are decoded as UTF-8; maps and lists as JSON), `Payload` the raw bytes |
| Headers | The AMQP **application properties** (name/value) |
| Other Data | `Address`, `MessageId`, `CorrelationId`, `Subject`, `ContentType`, `ContentEncoding`, `ReplyTo`, `To`, `UserId`, `GroupId`, `CreationTime`, `AbsoluteExpiryTime`, `Durable`, `Priority`, `TimeToLiveMs`, `DeliveryCount`, and each message annotation as `Annotation.<name>` |
| RoutingKey | RabbitMQ's `x-routing-key` annotation, otherwise the AMQP subject |

## Broker Notes

| Broker | Address | Filters | Notes |
|---|---|---|---|
| **ActiveMQ Artemis** | `orders`, FQQN `address::queue` | JMS selector (rules/expression) | Topics, durable and shared durable subscriptions, temporary reply queues. Tested on Artemis 2.x |
| **ActiveMQ Classic** | `orders`, `topic://news` | JMS selector | AMQP connector must be enabled. Rejects duplicate container IDs |
| **RabbitMQ 4.x** | `/queues/<queue>` to receive; `/queues/<q>` or `/exchanges/<x>/<key>` to send | SQL (4.2+) and property filters, stream offset, Bloom filter: **streams only** | Queue must exist. Virtual host via `vhost:<name>`. Tested on 4.2.9 |
| **Azure Service Bus** | `queue`, `topic/Subscriptions/sub` | None on the receiver. Use subscription rules in Azure | TLS port 5671. SAS policy (PLAIN) or CBS tokens |
| **Apache Qpid** | queue/exchange name | JMS selector; legacy bindings as custom filters | |
| **Solace** | queue name, `topic://name` | JMS selector | |

## Cluster Deployments

- **All servers and threads** (default): every server opens as many receivers as the handler's **Active Flow Count**. Brokers share queue messages between them. The usual choice for queues.
- **One connection at a time**: a database lease (`amqp_lease` table) makes sure only one receiver in the cluster is active. Standby threads take over within about a minute when it stops. Use it for ordered processing, or for a topic subscription that must process each message once (unless you use a shared durable subscription).
- Saving a queue, or updating its credentials, restarts it on every server.

Each connection gets a unique container ID unless **Container ID Override** is set. A fixed ID is needed for non-shared durable subscriptions, and many brokers reject a second connection with the same ID, so combine it with "One connection at a time".

## Monitoring

| Log category | Content |
|---|---|
| `AMQP` | Connections, lease handling, queue start/stop, test results |
| `AMQP Settings` | Settings |
| `AMQP Flow Worker` | Receiver threads: connect, receive, filter checks, connection loss, handler flow failures |
| `AMQP Step` | Send/receive/credential steps |

Use the queue's **Test** action to connect, attach with the configured filters and see which filters the broker accepted.

## Building from Source

### Prerequisites
- .NET 10 SDK
- `CreateDecisionsModule` global tool (installed/updated by the build)
- `Decisions.MessageQueues.dll`: copy it from a Decisions installation (`<Decisions>/modules/Decisions.MessageQueues/CoreServicesDlls/Decisions.MessageQueues.dll`, or from `Decisions.MessageQueues.zip`) into `lib/`. It is not in source control.

### Build
```bash
./build_module.sh        # Linux/macOS
.\build_module.ps1       # Windows
```
or `dotnet build build.proj`. The output is `Decisions.AMQP.zip` in the repository root, containing `Decisions.AMQP.dll`, `Amqp.Net.dll` and the icon.

The project targets .NET 10 (every Decisions version since 9.26 runs on .NET 10) and compiles against DecisionsSDK 9.31. `build.proj` passes `-buildmodule Decisions.AMQP.Module` and renames the zip, because CreateDecisionsModule drops everything after the last `.` from the module name.

## Disclaimer

This module is provided "as is" without warranties of any kind. Use it at your own risk. The authors, maintainers, and contributors disclaim all liability for any direct, indirect, incidental, special, or consequential damages, including data loss or service interruption, arising from the use of this software.

**Important Notes:**
- Always test in a non-production environment first
- Ensure proper monitoring and alerting for production deployments
- Review your broker's documentation for broker-specific configuration
- This module is not officially supported by Decisions
