namespace Decisions.AmqpMessageQueue
{
    /// <summary>Help texts shown as read-only notes next to settings, queue and step properties.</summary>
    internal static class AmqpNotes
    {
        public const string VirtualHost =
            "Optional. Sent as the hostname in the AMQP open frame. RabbitMQ selects the virtual host from it: " +
            "use 'vhost:my-vhost' (leave empty for the default vhost '/'). Azure Service Bus and most other brokers " +
            "don't need it. Leave empty to send the broker host name.";

        public const string SettingsCredentials =
            "Default credentials for all AMQP queues that don't override them. If the credentials rotate (for example " +
            "tokens or passwords issued by an identity provider), use the 'Update AMQP Queue Credentials' step in a " +
            "scheduled flow. The step stores the new credentials on the queue and restarts it on every server.\n\n" +
            "RabbitMQ with OAuth 2.0: choose 'Username and Password' and put the access token in Password (the username is ignored).";

        public const string ClientCertificate =
            "Optional, for mutual TLS. Required for 'Client Certificate (SASL EXTERNAL)' authentication. " +
            "Paste the certificate as a Base64-encoded PFX/PKCS#12 file including its private key " +
            "(Linux: 'base64 -w0 client.pfx', PowerShell: '[Convert]::ToBase64String([IO.File]::ReadAllBytes(\"client.pfx\"))').";

        public const string QueueCredentials =
            "By default the queue uses the credentials from the AMQP settings. Turn on 'Override Credentials' to give this " +
            "queue its own credentials. The 'Update AMQP Queue Credentials' step turns this on automatically and " +
            "restarts the queue on every server when it changes the credentials.";

        public const string ConsumeMode =
            "All servers and threads: every Decisions server opens as many receivers as the handler's 'Active Flow Count' " +
            "and the broker shares the messages between them. Use this for normal queues.\n\n" +
            "One connection at a time: only one receiver in the whole cluster is active. The others wait and take over " +
            "within about a minute if it stops. Use this when message order matters, or for a topic subscription where " +
            "each message must only be processed once.";

        public const string Acknowledgement =
            "After the handler flow completes: the message stays on the broker until the flow has finished. If the flow " +
            "fails, the 'On Flow Failure' action decides what happens. Nothing is lost if Decisions restarts.\n\n" +
            "As soon as the message is received: the broker deletes the message before the flow runs. Fastest, but the " +
            "message is lost if the flow fails or the server stops.";

        public const string FailureAction =
            "Mark as failed: the broker redelivers the message and counts the attempt. Brokers like Artemis and RabbitMQ " +
            "(quorum queues) dead-letter it after their configured maximum number of attempts.\n" +
            "Release: the broker redelivers without counting the attempt. A message that always fails is retried forever.\n" +
            "Reject: the broker dead-letters the message (if a dead-letter address is configured) or discards it.\n" +
            "Accept: the message is removed as if it had been processed.";

        public const string Prefetch =
            "How many messages each receiver may hold before they are processed (AMQP link credit). Higher values give " +
            "more throughput. Keep it low (1) when messages take a long time to process or when order matters.";

        public const string VerifyFilter =
            "Brokers silently ignore filters they don't support, and the receiver would then get every message. When this " +
            "is on, the queue stops with an error if the broker doesn't confirm the filter. Turn it off only if your " +
            "broker applies filters without confirming them.";

        public const string RuleSyntax =
            "Each rule compares a message field with a value. The rules are turned into a filter that the broker applies, " +
            "so non-matching messages never reach Decisions. The generated filter is shown in 'Generated Filter'.";

        public const string SelectorExpression =
            "A JMS message selector (SQL-92 syntax), evaluated by the broker. Application properties are used by name. " +
            "Examples:\n" +
            "  region = 'EU' AND amount > 1000\n" +
            "  type IN ('order', 'invoice') AND priority IS NOT NULL\n" +
            "  JMSCorrelationID = 'abc-123'\n" +
            "  customer LIKE 'ACME%'";

        public const string SqlExpression =
            "A RabbitMQ AMQP SQL filter expression (RabbitMQ 4.2 or later, streams only). Application properties are used " +
            "by name or as a.name; message properties as p.subject, p.correlation_id, p.message_id, p.content_type; " +
            "priority as h.priority. Examples:\n" +
            "  region = 'EU' AND amount > 1000\n" +
            "  p.subject LIKE 'order.%'\n" +
            "  a.[order-type] IN ('new', 'changed')";

        public const string CustomFilters =
            "Each row is added to the AMQP filter set of the receiver as-is. Use this for broker-specific filters that " +
            "the other options don't cover, for example:\n" +
            "  Key 'topic' / Descriptor 'apache.org:legacy-amqp-topic-binding:string' / String 'orders.#' (Qpid)\n" +
            "  Key 'selector' / Descriptor 'apache.org:selector-filter:string' / String 'color = ''red'''\n" +
            "  Key 'props' / Descriptor 'amqp:application-properties-filter' / Map 'region=EU' (RabbitMQ 4.1+ streams)\n" +
            "Descriptor can be a symbolic name or a numeric code such as 0x0000468C00000004.";

        public const string Stream =
            "Only for RabbitMQ streams. Start Position is applied every time the queue connects (also after a restart or " +
            "credential update), so 'First' re-reads the whole stream each time. Bloom filter values only deliver messages " +
            "published with a matching 'x-stream-filter-value' message annotation; combine them with a rule or expression " +
            "for exact results.";

        public const string ServiceBusFilter =
            "Azure Service Bus doesn't support filters on the receiver. Create a topic subscription with a SQL or " +
            "correlation filter rule in Azure, and use the subscription as the address: 'my-topic/Subscriptions/my-subscription'.";

        public const string DurableSubscription =
            "Only for topics. Leave empty for a temporary subscription: messages published while Decisions is not connected " +
            "are not received. Enter a name to create a durable subscription that keeps messages while Decisions is offline. " +
            "A durable subscription that is not shared can only have one receiver, so it requires 'One connection at a time' " +
            "and a fixed Container ID.";

        public const string SharedSubscription =
            "Lets several receivers (servers and threads) share the durable subscription, each message going to one of them " +
            "(Artemis, Solace and other JMS 2.0 brokers).";

        public const string ContainerId =
            "The AMQP container ID identifies the client to the broker. By default every connection gets a unique ID. " +
            "Set a fixed ID only if the broker requires it, for example for a durable subscription that is not shared.";

        public const string CbsAudience =
            "Only for 'Access Token via CBS'. The audience the token is authorized for. Leave empty to use " +
            "'amqp://<host>/<address>' for each address.";

        public static string GetBrokerNote(string brokerType)
        {
            switch (brokerType)
            {
                case AmqpOptions.BROKER_ARTEMIS:
                    return "ActiveMQ Artemis: the address is the queue or address name (e.g. 'orders'). Choose Queue or Topic as the " +
                           "address type when the address is not pre-configured. Filters use JMS selector syntax. For queue name " +
                           "FQQN use 'address::queue'.";
                case AmqpOptions.BROKER_ACTIVEMQ_CLASSIC:
                    return "ActiveMQ Classic: the address is the queue name (e.g. 'orders'). Use 'topic://name' for topics or choose " +
                           "Topic as the address type. Filters use JMS selector syntax. AMQP must be enabled on the broker (port 5672).";
                case AmqpOptions.BROKER_RABBITMQ:
                    return "RabbitMQ 4.x (native AMQP 1.0): receive from '/queues/<queue>' (the queue must exist). Send to " +
                           "'/queues/<queue>' or '/exchanges/<exchange>/<routing-key>'. Filters are supported on streams only: SQL " +
                           "filter expressions need RabbitMQ 4.2 or later. Set the virtual host as 'vhost:<name>' in the connection settings.";
                case AmqpOptions.BROKER_AZURE_SERVICE_BUS:
                    return "Azure Service Bus: the address is the queue name, or 'topic/Subscriptions/subscription' for a topic " +
                           "subscription. Use TLS (port 5671). Authenticate with a Shared Access Policy (username = policy name, " +
                           "password = key) or with an access token via CBS.";
                case AmqpOptions.BROKER_QPID:
                    return "Apache Qpid Broker-J / Qpid Dispatch: the address is the queue or exchange name. Filters use JMS selector " +
                           "syntax. Legacy topic/direct bindings can be added as custom filters.";
                case AmqpOptions.BROKER_SOLACE:
                    return "Solace: the address is the queue name, or 'topic://name' for topics. Filters use JMS selector syntax.";
                default:
                    return "Any AMQP 1.0 broker. The address is the node to receive from, usually the queue name. Filters are sent " +
                           "as JMS selectors (apache.org:selector-filter:string), which most AMQP 1.0 brokers support. Use custom " +
                           "filters for anything else.";
            }
        }
    }
}
