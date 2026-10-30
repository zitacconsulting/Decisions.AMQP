using System.Runtime.Serialization;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.Properties.Attributes;
using Decisions.MessageQueues;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// Module (System > Settings > Message Queue Settings) and project level AMQP settings.
    /// Queues use these unless they override the connection and/or the credentials.
    /// Environment variables: DECISIONS_AMQPQUEUE_&lt;PROPERTY&gt;.
    /// </summary>
    public class AmqpQueueSettings : BaseMqSettings<AmqpMessageQueue>, IAmqpConnectionSettings, IAmqpCredentialSettings
    {
        public const string MODULE_NAME = "Decisions.AMQP";

        public override string LogCategory => "AMQP Settings";
        protected override string AddQueueActionText => "Add AMQP Queue";
        protected override string QueueTypeName => "AMQP";
        public override string ModuleName => MODULE_NAME;

        // --- Connection ---

        [ORMField]
        [WritableValue]
        private string server;

        [DataMember]
        [PropertyClassification(1, "Broker Host", "Connection")]
        public string Server
        {
            get { return server; }
            set { server = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private bool useTls;

        [DataMember]
        [PropertyClassification(2, "Use TLS/SSL", "Connection")]
        public bool UseTls
        {
            get { return useTls; }
            set { useTls = value; OnPropertyChanged(); OnPropertyChanged(nameof(EffectivePortNote)); }
        }

        [ORMField]
        [WritableValue]
        private bool allowUntrustedCertificates;

        [DataMember]
        [PropertyClassification(3, "Allow Untrusted Certificates", "Connection")]
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
        [PropertyClassification(4, "Use Default Port", "Connection")]
        public bool UseDefaultPort
        {
            get { return useDefaultPort; }
            set { useDefaultPort = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(5, "Effective Port", "Connection")]
        [BooleanPropertyHidden(nameof(UseDefaultPort), false)]
        public string EffectivePortNote
        {
            get => $"Using default port: {GetEffectivePort()}";
            set { }
        }

        [ORMField]
        [WritableValue]
        private int port = AmqpPorts.AMQP;

        [DataMember]
        [PropertyClassification(6, "Port", "Connection")]
        [BooleanPropertyHidden(nameof(UseDefaultPort), true)]
        public int Port
        {
            get { return port; }
            set { port = value; OnPropertyChanged(); }
        }

        [ReadonlyEditor]
        [PropertyClassification(7, "Virtual Host Info", "Connection")]
        public string VirtualHostNote
        {
            get => AmqpNotes.VirtualHost;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string virtualHost;

        [DataMember]
        [PropertyClassification(8, "Virtual Host / Open Hostname", "Connection")]
        public string VirtualHost
        {
            get { return virtualHost; }
            set { virtualHost = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private int idleTimeoutSeconds = 60;

        [DataMember]
        [PropertyClassification(9, "Heartbeat / Idle Timeout (seconds)", "Connection")]
        public int IdleTimeoutSeconds
        {
            get { return idleTimeoutSeconds; }
            set { idleTimeoutSeconds = value; OnPropertyChanged(); }
        }

        [ORMField]
        [WritableValue]
        private int connectionTimeoutSeconds = 15;

        [DataMember]
        [PropertyClassification(10, "Connection Timeout (seconds)", "Connection")]
        public int ConnectionTimeoutSeconds
        {
            get { return connectionTimeoutSeconds; }
            set { connectionTimeoutSeconds = value; OnPropertyChanged(); }
        }

        // --- Credentials ---

        [ReadonlyEditor]
        [PropertyClassification(1, "Credentials Info", "Credentials")]
        public string CredentialsNote
        {
            get => AmqpNotes.SettingsCredentials;
            set { }
        }

        [ORMField]
        [WritableValue]
        private string authenticationMethod = AmqpOptions.AUTH_PLAIN;

        [DataMember]
        [PropertyClassification(2, "Authentication", "Credentials")]
        [SelectStringEditor(nameof(AuthenticationMethods))]
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
        [PropertyClassification(3, "Username", "Credentials")]
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
        [PropertyClassification(4, "Password", "Credentials")]
        [PasswordText]
        [BooleanPropertyHidden(nameof(UsesPassword), false)]
        public string Password
        {
            get { return password; }
            set { password = value; OnPropertyChanged(); }
        }

        [PropertyClassification(5, "Access Token", "Credentials")]
        [PasswordText]
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
        [PropertyClassification(6, "Token Type", "Credentials")]
        [SelectStringEditor(nameof(CbsTokenTypes))]
        [BooleanPropertyHidden(nameof(UsesToken), false)]
        public string CbsTokenType
        {
            get { return cbsTokenType; }
            set { cbsTokenType = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] CbsTokenTypes => AmqpOptions.CbsTokenTypes;

        [ReadonlyEditor]
        [PropertyClassification(7, "Client Certificate Info", "Credentials")]
        public string ClientCertificateNote
        {
            get => AmqpNotes.ClientCertificate;
            set { }
        }

        [ORMField(typeof(TextFieldConverter))]
        [WritableValue]
        private string clientCertificate;

        [DataMember]
        [PropertyClassification(8, "Client Certificate (Base64 PFX)", "Credentials")]
        [PasswordText]
        public string ClientCertificate
        {
            get { return clientCertificate; }
            set { clientCertificate = value; OnPropertyChanged(); }
        }

        [ORMField(4000, typeof(FixedLengthStringFieldConverter))]
        [WritableValue]
        private string clientCertificatePassword;

        [DataMember]
        [PropertyClassification(9, "Client Certificate Password", "Credentials")]
        [PasswordText]
        public string ClientCertificatePassword
        {
            get { return clientCertificatePassword; }
            set { clientCertificatePassword = value; OnPropertyChanged(); }
        }

        public int GetEffectivePort() => UseDefaultPort ? AmqpPorts.Default(UseTls) : Port;
    }
}
