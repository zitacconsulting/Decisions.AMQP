namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// Display strings used by the dropdowns on settings, queues and steps. The stored value is the
    /// display string itself (like the MQTT module), so don't rename these without a migration.
    /// </summary>
    public static class AmqpOptions
    {
        // --- Broker types ---
        public const string BROKER_GENERIC = "Generic AMQP 1.0";
        public const string BROKER_ARTEMIS = "ActiveMQ Artemis";
        public const string BROKER_ACTIVEMQ_CLASSIC = "ActiveMQ Classic";
        public const string BROKER_RABBITMQ = "RabbitMQ 4.x";
        public const string BROKER_AZURE_SERVICE_BUS = "Azure Service Bus";
        public const string BROKER_QPID = "Apache Qpid";
        public const string BROKER_SOLACE = "Solace";

        public static readonly string[] BrokerTypes =
        {
            BROKER_GENERIC, BROKER_ARTEMIS, BROKER_ACTIVEMQ_CLASSIC, BROKER_RABBITMQ,
            BROKER_AZURE_SERVICE_BUS, BROKER_QPID, BROKER_SOLACE
        };

        // --- Address types ---
        public const string ADDRESS_DEFAULT = "Broker Default";
        public const string ADDRESS_QUEUE = "Queue (each message goes to one consumer)";
        public const string ADDRESS_TOPIC = "Topic (every subscriber gets a copy)";

        public static readonly string[] AddressTypes = { ADDRESS_DEFAULT, ADDRESS_QUEUE, ADDRESS_TOPIC };

        // --- Filter types ---
        public const string FILTER_NONE = "None - receive all messages";
        public const string FILTER_RULES = "Simple Rules";
        public const string FILTER_EXPRESSION = "Filter Expression";
        public const string FILTER_CUSTOM = "Custom AMQP Filters (Advanced)";

        public static readonly string[] FilterTypes = { FILTER_NONE, FILTER_RULES, FILTER_EXPRESSION, FILTER_CUSTOM };

        public const string MATCH_ALL = "All rules must match (AND)";
        public const string MATCH_ANY = "Any rule may match (OR)";

        public static readonly string[] MatchModes = { MATCH_ALL, MATCH_ANY };

        // --- RabbitMQ stream start positions ---
        public const string STREAM_DEFAULT = "Not a stream / broker default";
        public const string STREAM_FIRST = "First - all stored messages";
        public const string STREAM_LAST = "Last - from the last stored chunk";
        public const string STREAM_NEXT = "Next - only new messages";
        public const string STREAM_OFFSET = "From offset";
        public const string STREAM_TIMESTAMP = "From timestamp (UTC)";
        public const string STREAM_INTERVAL = "From relative age (e.g. 30m, 2h, 7D)";

        public static readonly string[] StreamStartPositions =
        {
            STREAM_DEFAULT, STREAM_FIRST, STREAM_LAST, STREAM_NEXT, STREAM_OFFSET, STREAM_TIMESTAMP, STREAM_INTERVAL
        };

        // --- Receiving ---
        public const string CONSUME_ALL_SERVERS = "All servers and threads (competing consumers)";
        public const string CONSUME_SINGLE = "One connection at a time (cluster lease)";

        public static readonly string[] ConsumeModes = { CONSUME_ALL_SERVERS, CONSUME_SINGLE };

        public const string ACK_AFTER_FLOW = "After the handler flow completes (recommended)";
        public const string ACK_ON_RECEIVE = "As soon as the message is received";

        public static readonly string[] AckModes = { ACK_AFTER_FLOW, ACK_ON_RECEIVE };

        public const string FAILURE_MODIFY = "Mark as failed - broker redelivers and counts attempts";
        public const string FAILURE_RELEASE = "Release - broker redelivers without counting";
        public const string FAILURE_REJECT = "Reject - dead-letter or discard";
        public const string FAILURE_ACCEPT = "Accept - remove the message anyway";

        public static readonly string[] FailureActions = { FAILURE_MODIFY, FAILURE_RELEASE, FAILURE_REJECT, FAILURE_ACCEPT };

        // --- Authentication ---
        public const string AUTH_PLAIN = "Username and Password (SASL PLAIN)";
        public const string AUTH_ANONYMOUS = "Anonymous (SASL ANONYMOUS)";
        public const string AUTH_EXTERNAL = "Client Certificate (SASL EXTERNAL)";
        public const string AUTH_CBS = "Access Token via CBS (Azure Service Bus / Event Hubs)";

        public static readonly string[] AuthenticationMethods = { AUTH_PLAIN, AUTH_ANONYMOUS, AUTH_EXTERNAL, AUTH_CBS };

        public const string CBS_TOKEN_JWT = "JWT (Microsoft Entra ID / OAuth 2.0)";
        public const string CBS_TOKEN_SAS = "Shared Access Signature (SAS) token";

        public static readonly string[] CbsTokenTypes = { CBS_TOKEN_JWT, CBS_TOKEN_SAS };

        // --- Message body ---
        public const string BODY_AUTO = "Automatic (based on broker type)";
        public const string BODY_TEXT = "Text (AMQP value)";
        public const string BODY_BINARY = "Binary (AMQP data section, UTF-8)";

        public static readonly string[] BodyFormats = { BODY_AUTO, BODY_TEXT, BODY_BINARY };

        public static bool IsRabbitMq(string brokerType) => brokerType == BROKER_RABBITMQ;
        public static bool IsServiceBus(string brokerType) => brokerType == BROKER_AZURE_SERVICE_BUS;

        /// <summary>True when the broker understands JMS selectors (apache.org:selector-filter:string).</summary>
        public static bool UsesSelectorSyntax(string brokerType)
            => !IsRabbitMq(brokerType) && !IsServiceBus(brokerType);
    }
}
