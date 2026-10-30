using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using DecisionsFramework;
using DecisionsFramework.Data.Messaging;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.Properties.Attributes;
using DecisionsFramework.ServiceLayer.Utilities;
using Decisions.MessageQueues;

namespace Decisions.AmqpMessageQueue
{
    [AutoRegisterNativeType]
    [DataContract]
    public class AmqpMessageQueue : BaseMqDefinition, IAmqpConnectionSettings, IAmqpCredentialSettings
    {
        public override string LogCategory => "AMQP";
        protected override IMessageQueue CreateNewQueueImpl() => new AmqpMessageQueueImpl(this);
        protected override BaseMqClusterNotification CreateNewClusterNotification() => new AmqpClusterNotification();

        private ORMOneToManyRelationship<AmqpFilterRule> filterRules;
        private ORMOneToManyRelationship<AmqpCustomFilter> customFilters;

        public AmqpMessageQueue()
        {
            filterRules = new ORMOneToManyRelationship<AmqpFilterRule>("amqp_message_queue_id", false) { DeleteOrphans = true };
            customFilters = new ORMOneToManyRelationship<AmqpCustomFilter>("amqp_message_queue_id", false) { DeleteOrphans = true };
        }

        // ===================== 1 Definition =====================

        [ORMField]
        [WritableValue]
        private string brokerType = AmqpOptions.BROKER_GENERIC;

        [DataMember]
        [PropertyClassification(1, "Broker Type", "1 Definition")]
        [SelectStringEditor(nameof(BrokerTypes))]
        public string BrokerType
        {
            get { return brokerType; }
            set
            {
                brokerType = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(BrokerNote));
                OnPropertyChanged(nameof(ShowAddressType));
                OnPropertyChanged(nameof(IsServiceBus));
                OnPropertyChanged(nameof(IsRabbitMq));
                OnPropertyChanged(nameof(ShowFilterOptions));
                OnFilterVisibilityChanged();
            }
        }

        [PropertyHidden]
        public string[] BrokerTypes => AmqpOptions.BrokerTypes;

        [ReadonlyEditor]
        [PropertyClassification(2, "Broker Info", "1 Definition")]
        public string BrokerNote
        {
            get => AmqpNotes.GetBrokerNote(brokerType);
            set { }
        }

        [ORMField]
        [WritableValue]
        private string address;

        [DataMember]
        [PropertyClassification(3, "Address (Receive From)", "1 Definition")]
        public string Address
        {
            get { return address; }
            set { address = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private string addressType = AmqpOptions.ADDRESS_DEFAULT;

        [DataMember]
        [PropertyClassification(4, "Address Type", "1 Definition")]
        [SelectStringEditor(nameof(AddressTypes))]
        [BooleanPropertyHidden(nameof(ShowAddressType), false)]
        public string AddressType
        {
            get { return addressType; }
            set
            {
                addressType = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ShowDurableSubscription));
            }
        }

        [PropertyHidden]
        public string[] AddressTypes => AmqpOptions.AddressTypes;

        [ORMField]
        [WritableValue]
        private string publishAddress;

        [DataMember]
        [PropertyClassification(5, "Publish Address (optional)", "1 Definition")]
        public string PublishAddress
        {
            get { return publishAddress; }
            set { publishAddress = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(6, "Publish Address Info", "1 Definition")]
        public string PublishAddressNote
        {
            get => "Where the 'Send AMQP Message' step sends to when it doesn't override the address. Leave empty to send to the receive address.";
            set { }
        }

        // ===================== 2 Message Filter =====================

        [ReadonlyEditor]
        [PropertyClassification(1, "Filtering Not Supported", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(IsServiceBus), false)]
        public string ServiceBusFilterNote
        {
            get => AmqpNotes.ServiceBusFilter;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string filterType = AmqpOptions.FILTER_NONE;

        [DataMember]
        [PropertyClassification(2, "Filter Type", "2 Message Filter")]
        [SelectStringEditor(nameof(FilterTypes))]
        [BooleanPropertyHidden(nameof(ShowFilterOptions), false)]
        public string FilterType
        {
            get { return filterType; }
            set { filterType = value; OnPropertyChanged(); OnFilterVisibilityChanged(); }
        }

        [PropertyHidden]
        public string[] FilterTypes => AmqpOptions.FilterTypes;

        [ReadonlyEditor]
        [PropertyClassification(3, "Rules Info", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowRules), false)]
        public string RulesNote
        {
            get => AmqpNotes.RuleSyntax;
            set { }
        }

        [DataMember]
        [PropertyClassification(4, "Filter Rules", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowRules), false)]
        public AmqpFilterRule[] FilterRules
        {
            get { return filterRules.Items; }
            set { filterRules.Items = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [ORMField]
        [WritableValue]
        private string ruleMatchMode = AmqpOptions.MATCH_ALL;

        [DataMember]
        [PropertyClassification(5, "Rule Matching", "2 Message Filter")]
        [SelectStringEditor(nameof(MatchModes))]
        [BooleanPropertyHidden(nameof(ShowRules), false)]
        public string RuleMatchMode
        {
            get { return ruleMatchMode; }
            set { ruleMatchMode = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [PropertyHidden]
        public string[] MatchModes => AmqpOptions.MatchModes;

        [ReadonlyEditor]
        [PropertyClassification(6, "Expression Syntax", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowExpression), false)]
        public string ExpressionNote
        {
            get => AmqpOptions.IsRabbitMq(brokerType) ? AmqpNotes.SqlExpression : AmqpNotes.SelectorExpression;
            set { }
        }

        [ORMField(typeof(TextFieldConverter))]
        [WritableValue]
        private string filterExpression;

        [DataMember]
        [PropertyClassification(7, "Filter Expression", "2 Message Filter")]
        [LongTextPropertyEditor(100)]
        [BooleanPropertyHidden(nameof(ShowExpression), false)]
        public string FilterExpression
        {
            get { return filterExpression; }
            set { filterExpression = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [ReadonlyEditor]
        [PropertyClassification(8, "Custom Filters Info", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowCustomFilters), false)]
        public string CustomFiltersNote
        {
            get => AmqpNotes.CustomFilters;
            set { }
        }

        [DataMember]
        [PropertyClassification(9, "Custom Filters", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowCustomFilters), false)]
        public AmqpCustomFilter[] CustomFilters
        {
            get { return customFilters.Items; }
            set { customFilters.Items = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [ReadonlyEditor]
        [PropertyClassification(10, "Generated Filter", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowFilterOptions), false)]
        public string GeneratedFilter
        {
            get => AmqpFilterBuilder.Describe(this);
            set { }
        }

        [ORMField]
        [WritableValue]
        private bool verifyFilterAccepted = true;

        [DataMember]
        [PropertyClassification(11, "Stop If Broker Ignores Filter", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowFilterOptions), false)]
        public bool VerifyFilterAccepted
        {
            get { return verifyFilterAccepted; }
            set { verifyFilterAccepted = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(12, "Stop If Broker Ignores Filter Info", "2 Message Filter")]
        [BooleanPropertyHidden(nameof(ShowFilterOptions), false)]
        public string VerifyFilterNote
        {
            get => AmqpNotes.VerifyFilter;
            set { }
        }

        // ===================== 3 RabbitMQ Stream =====================

        [ReadonlyEditor]
        [PropertyClassification(1, "Stream Info", "3 RabbitMQ Stream")]
        [BooleanPropertyHidden(nameof(IsRabbitMq), false)]
        public string StreamNote
        {
            get => AmqpNotes.Stream;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string streamStartPosition = AmqpOptions.STREAM_DEFAULT;

        [DataMember]
        [PropertyClassification(2, "Start Position", "3 RabbitMQ Stream")]
        [SelectStringEditor(nameof(StreamStartPositions))]
        [BooleanPropertyHidden(nameof(IsRabbitMq), false)]
        public string StreamStartPosition
        {
            get { return streamStartPosition; }
            set { streamStartPosition = value; OnPropertyChanged(); OnPropertyChanged(nameof(ShowStreamStartValue)); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [PropertyHidden]
        public string[] StreamStartPositions => AmqpOptions.StreamStartPositions;

        [ORMField]
        [WritableValue]
        private string streamStartValue;

        [DataMember]
        [PropertyClassification(3, "Start Position Value", "3 RabbitMQ Stream")]
        [BooleanPropertyHidden(nameof(ShowStreamStartValue), false)]
        public string StreamStartValue
        {
            get { return streamStartValue; }
            set { streamStartValue = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [ORMField(4000, typeof(FixedLengthStringFieldConverter))]
        [WritableValue]
        private string streamFilterValues;

        [DataMember]
        [PropertyClassification(4, "Bloom Filter Values (comma separated)", "3 RabbitMQ Stream")]
        [BooleanPropertyHidden(nameof(IsRabbitMq), false)]
        public string StreamFilterValues
        {
            get { return streamFilterValues; }
            set { streamFilterValues = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        [ORMField]
        [WritableValue]
        private bool streamMatchUnfiltered;

        [DataMember]
        [PropertyClassification(5, "Also Receive Messages Without Filter Value", "3 RabbitMQ Stream")]
        [BooleanPropertyHidden(nameof(IsRabbitMq), false)]
        public bool StreamMatchUnfiltered
        {
            get { return streamMatchUnfiltered; }
            set { streamMatchUnfiltered = value; OnPropertyChanged(); OnPropertyChanged(nameof(GeneratedFilter)); }
        }

        // ===================== 4 Receiving =====================

        [ReadonlyEditor]
        [PropertyClassification(1, "Consume Mode Info", "4 Receiving")]
        public string ConsumeModeNote
        {
            get => AmqpNotes.ConsumeMode;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string consumeMode = AmqpOptions.CONSUME_ALL_SERVERS;

        [DataMember]
        [PropertyClassification(2, "Consume Mode", "4 Receiving")]
        [SelectStringEditor(nameof(ConsumeModes))]
        public string ConsumeMode
        {
            get { return consumeMode; }
            set { consumeMode = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] ConsumeModes => AmqpOptions.ConsumeModes;

        [PropertyHidden]
        public bool UseClusterLease => consumeMode == AmqpOptions.CONSUME_SINGLE;

        [ORMField]
        [WritableValue]
        private int prefetchCount = 5;

        [DataMember]
        [PropertyClassification(3, "Prefetch Count", "4 Receiving")]
        public int PrefetchCount
        {
            get { return prefetchCount; }
            set { prefetchCount = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(4, "Prefetch Count Info", "4 Receiving")]
        public string PrefetchNote
        {
            get => AmqpNotes.Prefetch;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string acknowledgementMode = AmqpOptions.ACK_AFTER_FLOW;

        [DataMember]
        [PropertyClassification(5, "Remove Message From Broker", "4 Receiving")]
        [SelectStringEditor(nameof(AckModes))]
        public string AcknowledgementMode
        {
            get { return acknowledgementMode; }
            set { acknowledgementMode = value; OnPropertyChanged(); OnPropertyChanged(nameof(AckAfterFlow)); }
        }

        [PropertyHidden]
        public string[] AckModes => AmqpOptions.AckModes;

        [PropertyHidden]
        public bool AckAfterFlow => acknowledgementMode != AmqpOptions.ACK_ON_RECEIVE;

        [ReadonlyEditor]
        [PropertyClassification(6, "Remove Message Info", "4 Receiving")]
        public string AcknowledgementNote
        {
            get => AmqpNotes.Acknowledgement;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string failureAction = AmqpOptions.FAILURE_MODIFY;

        [DataMember]
        [PropertyClassification(7, "On Flow Failure", "4 Receiving")]
        [SelectStringEditor(nameof(FailureActions))]
        [BooleanPropertyHidden(nameof(AckAfterFlow), false)]
        public string FailureAction
        {
            get { return failureAction; }
            set { failureAction = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] FailureActions => AmqpOptions.FailureActions;

        [ReadonlyEditor]
        [PropertyClassification(8, "On Flow Failure Info", "4 Receiving")]
        [BooleanPropertyHidden(nameof(AckAfterFlow), false)]
        public string FailureActionNote
        {
            get => AmqpNotes.FailureAction;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string durableSubscriptionName;

        [DataMember]
        [PropertyClassification(9, "Durable Subscription Name", "4 Receiving")]
        [BooleanPropertyHidden(nameof(ShowDurableSubscription), false)]
        public string DurableSubscriptionName
        {
            get { return durableSubscriptionName; }
            set { durableSubscriptionName = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(10, "Durable Subscription Info", "4 Receiving")]
        [BooleanPropertyHidden(nameof(ShowDurableSubscription), false)]
        public string DurableSubscriptionNote
        {
            get => AmqpNotes.DurableSubscription;
            set { }
        }

        [ORMField]
        [WritableValue]
        private bool sharedSubscription;

        [DataMember]
        [PropertyClassification(11, "Shared Subscription", "4 Receiving")]
        [BooleanPropertyHidden(nameof(ShowDurableSubscription), false)]
        public bool SharedSubscription
        {
            get { return sharedSubscription; }
            set { sharedSubscription = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(12, "Shared Subscription Info", "4 Receiving")]
        [BooleanPropertyHidden(nameof(ShowDurableSubscription), false)]
        public string SharedSubscriptionNote
        {
            get => AmqpNotes.SharedSubscription;
            set { }
        }

        // ===================== 5 Connection =====================

        [ORMField]
        [WritableValue]
        private bool overrideSettings;

        [DataMember]
        [PropertyClassification(1, "Override Connection Settings", "5 Connection")]
        public bool OverrideSettings
        {
            get { return overrideSettings; }
            set { overrideSettings = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private string server;

        [DataMember]
        [PropertyClassification(2, "Broker Host", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public string Server
        {
            get { return server; }
            set { server = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private bool useTls;

        [DataMember]
        [PropertyClassification(3, "Use TLS/SSL", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public bool UseTls
        {
            get { return useTls; }
            set { useTls = value; OnPropertyChanged(); OnPropertyChanged(nameof(EffectivePortNote)); }
        }

        [ORMField]
        [WritableValue]
        private bool allowUntrustedCertificates;

        [DataMember]
        [PropertyClassification(4, "Allow Untrusted Certificates", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        [BooleanPropertyHidden(nameof(UseTls), false)]
        public bool AllowUntrustedCertificates
        {
            get { return allowUntrustedCertificates; }
            set { allowUntrustedCertificates = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private bool useDefaultPort = true;

        [DataMember]
        [PropertyClassification(5, "Use Default Port", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public bool UseDefaultPort
        {
            get { return useDefaultPort; }
            set { useDefaultPort = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(6, "Effective Port", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        [BooleanPropertyHidden(nameof(UseDefaultPort), false)]
        public string EffectivePortNote
        {
            get => $"Using default port: {AmqpPorts.Default(UseTls)}";
            set { }
        }

        [ORMField]
        [WritableValue]
        private int port = AmqpPorts.AMQP;

        [DataMember]
        [PropertyClassification(7, "Port", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        [BooleanPropertyHidden(nameof(UseDefaultPort), true)]
        public int Port
        {
            get { return port; }
            set { port = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private string virtualHost;

        [DataMember]
        [PropertyClassification(8, "Virtual Host / Open Hostname", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public string VirtualHost
        {
            get { return virtualHost; }
            set { virtualHost = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(9, "Virtual Host Info", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public string VirtualHostNote
        {
            get => AmqpNotes.VirtualHost;
            set { }
        }

        [ORMField]
        [WritableValue]
        private int idleTimeoutSeconds = 60;

        [DataMember]
        [PropertyClassification(10, "Heartbeat / Idle Timeout (seconds)", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public int IdleTimeoutSeconds
        {
            get { return idleTimeoutSeconds; }
            set { idleTimeoutSeconds = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private int connectionTimeoutSeconds = 15;

        [DataMember]
        [PropertyClassification(11, "Connection Timeout (seconds)", "5 Connection")]
        [BooleanPropertyHidden(nameof(OverrideSettings), false)]
        public int ConnectionTimeoutSeconds
        {
            get { return connectionTimeoutSeconds; }
            set { connectionTimeoutSeconds = value; OnPropertyChanged(); }
        }

        // ===================== 6 Credentials =====================

        [ReadonlyEditor]
        [PropertyClassification(1, "Credentials Info", "6 Credentials")]
        public string CredentialsNote
        {
            get => AmqpNotes.QueueCredentials;
            set { }
        }

        [ORMField]
        [WritableValue]
        private bool overrideCredentials;

        [DataMember]
        [PropertyClassification(2, "Override Credentials", "6 Credentials")]
        public bool OverrideCredentials
        {
            get { return overrideCredentials; }
            set { overrideCredentials = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private string authenticationMethod = AmqpOptions.AUTH_PLAIN;

        [DataMember]
        [PropertyClassification(3, "Authentication", "6 Credentials")]
        [SelectStringEditor(nameof(AuthenticationMethods))]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        public string AuthenticationMethod
        {
            get { return authenticationMethod; }
            set
            {
                authenticationMethod = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(UsesPassword));
                OnPropertyChanged(nameof(UsesToken));
            }
        }

        [PropertyHidden]
        public string[] AuthenticationMethods => AmqpOptions.AuthenticationMethods;

        [PropertyHidden]
        public bool UsesPassword => authenticationMethod == AmqpOptions.AUTH_PLAIN || string.IsNullOrEmpty(authenticationMethod);

        [PropertyHidden]
        public bool UsesToken => authenticationMethod == AmqpOptions.AUTH_CBS;

        [ORMField]
        [WritableValue]
        private string username;

        [DataMember]
        [PropertyClassification(4, "Username", "6 Credentials")]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        [BooleanPropertyHidden(nameof(UsesPassword), false)]
        public string Username
        {
            get { return username; }
            set { username = value; OnPropertyChanged(); }
        }

        [ORMField(typeof(TextFieldConverter))]
        [WritableValue]
        private string password;

        [DataMember]
        [PropertyClassification(5, "Password", "6 Credentials")]
        [PasswordText]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        [BooleanPropertyHidden(nameof(UsesPassword), false)]
        public string Password
        {
            get { return password; }
            set { password = value; OnPropertyChanged(); }
        }

        [PropertyClassification(6, "Access Token", "6 Credentials")]
        [PasswordText]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        [BooleanPropertyHidden(nameof(UsesToken), false)]
        public string AccessToken
        {
            get { return password; }
            set { password = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private string cbsTokenType = AmqpOptions.CBS_TOKEN_JWT;

        [DataMember]
        [PropertyClassification(7, "Token Type", "6 Credentials")]
        [SelectStringEditor(nameof(CbsTokenTypes))]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        [BooleanPropertyHidden(nameof(UsesToken), false)]
        public string CbsTokenType
        {
            get { return cbsTokenType; }
            set { cbsTokenType = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] CbsTokenTypes => AmqpOptions.CbsTokenTypes;

        [ORMField(typeof(TextFieldConverter))]
        [WritableValue]
        private string clientCertificate;

        [DataMember]
        [PropertyClassification(8, "Client Certificate (Base64 PFX)", "6 Credentials")]
        [PasswordText]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        public string ClientCertificate
        {
            get { return clientCertificate; }
            set { clientCertificate = value; OnPropertyChanged(); }
        }

        [ORMField(4000, typeof(FixedLengthStringFieldConverter))]
        [WritableValue]
        private string clientCertificatePassword;

        [DataMember]
        [PropertyClassification(9, "Client Certificate Password", "6 Credentials")]
        [PasswordText]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        public string ClientCertificatePassword
        {
            get { return clientCertificatePassword; }
            set { clientCertificatePassword = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(10, "Client Certificate Info", "6 Credentials")]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        public string ClientCertificateNote
        {
            get => AmqpNotes.ClientCertificate;
            set { }
        }

        [ORMField]
        [WritableValue]
        private DateTime? credentialsLastUpdated;

        [DataMember]
        [PropertyHidden]
        public DateTime? CredentialsLastUpdated
        {
            get { return credentialsLastUpdated; }
            set { credentialsLastUpdated = value; }
        }

        [ReadonlyEditor]
        [PropertyClassification(11, "Credentials Last Updated", "6 Credentials")]
        [BooleanPropertyHidden(nameof(OverrideCredentials), false)]
        public string CredentialsLastUpdatedNote
        {
            get => credentialsLastUpdated.HasValue
                ? $"{credentialsLastUpdated.Value:yyyy-MM-dd HH:mm:ss} UTC (by the 'Update AMQP Queue Credentials' step)"
                : "Not updated by a flow yet";
            set { }
        }

        // ===================== 7 Advanced =====================

        [ORMField]
        [WritableValue]
        private string containerIdOverride;

        [DataMember]
        [PropertyClassification(1, "Container ID Override", "7 Advanced")]
        public string ContainerIdOverride
        {
            get { return containerIdOverride; }
            set { containerIdOverride = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(2, "Container ID Info", "7 Advanced")]
        public string ContainerIdNote
        {
            get => AmqpNotes.ContainerId;
            set { }
        }

        [ORMField(2000, typeof(FixedLengthStringFieldConverter))]
        [WritableValue]
        private string cbsAudience;

        [DataMember]
        [PropertyClassification(3, "CBS Token Audience", "7 Advanced")]
        public string CbsAudience
        {
            get { return cbsAudience; }
            set { cbsAudience = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(4, "CBS Token Audience Info", "7 Advanced")]
        public string CbsAudienceNote
        {
            get => AmqpNotes.CbsAudience;
            set { }
        }

        // ===================== Visibility helpers =====================

        [PropertyHidden]
        public bool IsServiceBus => AmqpOptions.IsServiceBus(brokerType);

        [PropertyHidden]
        public bool IsRabbitMq => AmqpOptions.IsRabbitMq(brokerType);

        [PropertyHidden]
        public bool ShowAddressType => !IsRabbitMq && !IsServiceBus;

        [PropertyHidden]
        public bool IsTopic => ShowAddressType && addressType == AmqpOptions.ADDRESS_TOPIC;

        [PropertyHidden]
        public bool ShowDurableSubscription => IsTopic;

        [PropertyHidden]
        public bool ShowFilterOptions => !IsServiceBus;

        [PropertyHidden]
        public bool ShowRules => ShowFilterOptions && filterType == AmqpOptions.FILTER_RULES;

        [PropertyHidden]
        public bool ShowExpression => ShowFilterOptions && filterType == AmqpOptions.FILTER_EXPRESSION;

        [PropertyHidden]
        public bool ShowCustomFilters => ShowFilterOptions && filterType == AmqpOptions.FILTER_CUSTOM;

        [PropertyHidden]
        public bool ShowStreamStartValue => IsRabbitMq &&
            (streamStartPosition == AmqpOptions.STREAM_OFFSET ||
             streamStartPosition == AmqpOptions.STREAM_TIMESTAMP ||
             streamStartPosition == AmqpOptions.STREAM_INTERVAL);

        private void OnFilterVisibilityChanged()
        {
            OnPropertyChanged(nameof(ShowRules));
            OnPropertyChanged(nameof(ShowExpression));
            OnPropertyChanged(nameof(ShowCustomFilters));
            OnPropertyChanged(nameof(ExpressionNote));
            OnPropertyChanged(nameof(GeneratedFilter));
        }

        // ===================== Export / import of the filter rows =====================

        public override void Read(ReadStream stream)
        {
            base.Read(stream);
            filterRules.Items = ReadRows<AmqpFilterRule>(stream, "filterRules");
            customFilters.Items = ReadRows<AmqpCustomFilter>(stream, "customFilters");
        }

        public override void Save(WriteStream stream)
        {
            base.Save(stream);
            WriteRows(stream, "filterRules", filterRules?.Items);
            WriteRows(stream, "customFilters", customFilters?.Items);
        }

        private static T[] ReadRows<T>(ReadStream stream, string prefix) where T : class
        {
            var rows = new List<T>();
            if (!stream.ContainsValue($"{prefix}_Count"))
                return rows.ToArray();
            int count = stream.GetValue<int>($"{prefix}_Count");
            for (int i = 1; i <= count; i++)
            {
                string key = $"{prefix}_{i}";
                if (stream.ContainsValue(key) && stream.GetValue<byte[]>(key) is byte[] bytes)
                {
                    if (ObjectGraphSerializer.Deserialize(bytes, DataGraph.ReferenceUpdatePolicy.Inherit) is T row)
                        rows.Add(row);
                }
            }
            return rows.ToArray();
        }

        private static void WriteRows<T>(WriteStream stream, string prefix, T[] rows)
        {
            if (rows == null || rows.Length == 0)
                return;
            stream.Add($"{prefix}_Count", rows.Length);
            for (int i = 0; i < rows.Length; i++)
                stream.Add($"{prefix}_{i + 1}", ObjectGraphSerializer.Serialize(rows[i]));
        }

        // ===================== Interfaces =====================

        public int GetEffectivePort() => UseDefaultPort ? AmqpPorts.Default(UseTls) : Port;

        public string GetPublishAddress() => string.IsNullOrWhiteSpace(PublishAddress) ? Address : PublishAddress;

        // ===================== Validation =====================

        public override ValidationIssue[] GetAdditionalValidationIssues()
        {
            var issues = new List<ValidationIssue>();

            if (string.IsNullOrWhiteSpace(Address))
                issues.Add(new ValidationIssue(this, "Address (Receive From) must be supplied", "", BreakLevel.Fatal, nameof(Address)));
            else if (IsRabbitMq && !Address.StartsWith("/queues/"))
                issues.Add(new ValidationIssue(this, "RabbitMQ 4.x receive addresses have the format '/queues/<queue name>'", "", BreakLevel.Warning, nameof(Address)));

            if (OverrideSettings && string.IsNullOrWhiteSpace(Server))
                issues.Add(new ValidationIssue(this, "Broker Host must be supplied when overriding the connection settings", "", BreakLevel.Fatal, nameof(Server)));

            if (OverrideSettings && !UseDefaultPort && (Port < 1 || Port > 65535))
                issues.Add(new ValidationIssue(this, "Port must be between 1 and 65535", "", BreakLevel.Fatal, nameof(Port)));

            if (PrefetchCount < 1)
                issues.Add(new ValidationIssue(this, "Prefetch Count must be at least 1", "", BreakLevel.Fatal, nameof(PrefetchCount)));

            if (OverrideCredentials)
                AmqpConnectionHelper.ValidateCredentials(this, this, issues);

            if (ShowFilterOptions)
            {
                foreach (string error in AmqpFilterBuilder.Validate(this))
                    issues.Add(new ValidationIssue(this, error, "", BreakLevel.Fatal));
            }

            if (IsTopic)
            {
                bool durable = !string.IsNullOrWhiteSpace(DurableSubscriptionName);
                if (!UseClusterLease && !(durable && SharedSubscription))
                    issues.Add(new ValidationIssue(this,
                        "A topic subscription delivers every message to every receiver. With 'All servers and threads' each server and thread " +
                        "processes a copy. Use 'One connection at a time' or a shared durable subscription to process each message once.",
                        "", BreakLevel.Warning, nameof(ConsumeMode)));
                if (durable && !SharedSubscription && string.IsNullOrWhiteSpace(ContainerIdOverride))
                    issues.Add(new ValidationIssue(this, "A durable subscription that is not shared needs a fixed Container ID Override", "", BreakLevel.Fatal, nameof(ContainerIdOverride)));
                if (durable && !SharedSubscription && !UseClusterLease)
                    issues.Add(new ValidationIssue(this, "A durable subscription that is not shared only allows one receiver. Use 'One connection at a time'.", "", BreakLevel.Fatal, nameof(ConsumeMode)));
            }

            if (!string.IsNullOrWhiteSpace(ContainerIdOverride) && !UseClusterLease)
                issues.Add(new ValidationIssue(this,
                    "All receivers will use the same Container ID. Brokers such as ActiveMQ Classic reject a second connection with the same ID. " +
                    "Use 'One connection at a time' unless your broker allows duplicate container IDs.",
                    "", BreakLevel.Warning, nameof(ContainerIdOverride)));

            return issues.ToArray();
        }
    }
}
