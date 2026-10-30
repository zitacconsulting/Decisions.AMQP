namespace Decisions.AmqpMessageQueue
{
    /// <summary>Broker connection values, supplied by the module/project settings or by a queue that overrides them.</summary>
    public interface IAmqpConnectionSettings
    {
        string Server { get; }
        bool UseTls { get; }
        bool AllowUntrustedCertificates { get; }
        string VirtualHost { get; }
        int IdleTimeoutSeconds { get; }
        int ConnectionTimeoutSeconds { get; }
        int GetEffectivePort();
    }

    /// <summary>Authentication values, supplied by the module/project settings or by a queue that overrides them.</summary>
    public interface IAmqpCredentialSettings
    {
        string AuthenticationMethod { get; }
        string Username { get; }
        string Password { get; }
        string CbsTokenType { get; }
        string ClientCertificate { get; }
        string ClientCertificatePassword { get; }
    }

    internal static class AmqpPorts
    {
        public const int AMQP = 5672;
        public const int AMQPS = 5671;

        public static int Default(bool useTls) => useTls ? AMQPS : AMQP;
    }
}
