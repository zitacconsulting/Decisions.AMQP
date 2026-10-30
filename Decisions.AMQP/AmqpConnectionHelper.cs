using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;
using DecisionsFramework;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Services.Projects;
using DecisionsFramework.ServiceLayer.Services.Projects.Settings;
using DecisionsFramework.ServiceLayer.Utilities;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>An open connection + session + receiver link. Disposing closes all three.</summary>
    public sealed class AmqpReceiverHandle : IDisposable
    {
        public Connection Connection { get; internal set; }
        public Session Session { get; internal set; }
        public ReceiverLink Link { get; internal set; }

        public void Dispose()
        {
            AmqpConnectionHelper.SafeClose(Link);
            AmqpConnectionHelper.SafeClose(Session);
            AmqpConnectionHelper.SafeClose(Connection);
        }
    }

    public static class AmqpConnectionHelper
    {
        private static readonly Log Log = new Log("AMQP");
        private static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(5);

        // ---------------- Settings resolution ----------------

        /// <summary>Settings for the given project, or the module (system) settings if there is no project.</summary>
        internal static AmqpQueueSettings GetSettings(string projectId)
        {
            if (string.IsNullOrEmpty(projectId))
                return ModuleSettingsAccessor<AmqpQueueSettings>.Instance;
            return ProjectSettingsAccessor<AmqpQueueSettings>.GetSettings(projectId);
        }

        public static IAmqpConnectionSettings GetConnectionSettings(AmqpMessageQueue queue)
            => queue.OverrideSettings ? queue : GetSettings(queue.GetProjectId());

        public static IAmqpCredentialSettings GetCredentialSettings(AmqpMessageQueue queue)
            => queue.OverrideCredentials ? queue : GetSettings(queue.GetProjectId());

        /// <summary>
        /// All AMQP queues the project can use (its own and those of its dependencies). Same lookup as
        /// BaseMqSettings.AvailableQueues, which only exists in 9.32 and 10.3+ (not 10.0-10.2).
        /// </summary>
        public static AmqpMessageQueue[] GetQueues(string projectId)
            => (string.IsNullOrEmpty(projectId)
                   ? ProjectUtility.GetProjectEntities<AmqpMessageQueue>(null)
                   : ProjectUtility.FetchAvailableProjectEntities<AmqpMessageQueue>(projectId, null))
               ?? Array.Empty<AmqpMessageQueue>();

        public static AmqpMessageQueue FindQueue(string queueName, string projectId)
        {
            var matches = GetQueues(projectId).Where(q => q.DisplayName == queueName).ToArray();
            if (matches.Length > 1)
                Log.Warn($"[AMQP] Multiple queues found with name '{queueName}' - using the first match.");
            return matches.FirstOrDefault();
        }

        public static string GetDefaultContainerId(AmqpMessageQueue queue, string suffix)
            => !string.IsNullOrWhiteSpace(queue.ContainerIdOverride)
                ? queue.ContainerIdOverride.Trim()
                : $"decisions-{Environment.MachineName}-{suffix}";

        // ---------------- Validation ----------------

        internal static void ValidateCredentials(object owner, IAmqpCredentialSettings credentials, List<ValidationIssue> issues)
        {
            string method = credentials.AuthenticationMethod;
            if (method == AmqpOptions.AUTH_EXTERNAL && string.IsNullOrWhiteSpace(credentials.ClientCertificate))
                issues.Add(new ValidationIssue(owner, "Client Certificate authentication needs a client certificate", "", BreakLevel.Fatal, "ClientCertificate"));
            if (method == AmqpOptions.AUTH_CBS && string.IsNullOrWhiteSpace(credentials.Password))
                issues.Add(new ValidationIssue(owner, "An Access Token is required for CBS authentication (it can also be set by the 'Update AMQP Queue Credentials' step)", "", BreakLevel.Warning, "AccessToken"));
            if (!string.IsNullOrWhiteSpace(credentials.ClientCertificate) && !IsBase64(credentials.ClientCertificate))
                issues.Add(new ValidationIssue(owner, "Client Certificate must be a Base64-encoded PFX file", "", BreakLevel.Fatal, "ClientCertificate"));
        }

        private static bool IsBase64(string value)
        {
            Span<byte> buffer = new byte[value.Length];
            return Convert.TryFromBase64String(value.Trim(), buffer, out _);
        }

        // ---------------- Connecting ----------------

        /// <summary>Opens a connection (TCP/TLS + SASL) using the queue's effective settings and credentials.</summary>
        public static Connection Connect(AmqpMessageQueue queue, string containerId)
        {
            IAmqpConnectionSettings connection = GetConnectionSettings(queue);
            IAmqpCredentialSettings credentials = GetCredentialSettings(queue);
            if (connection == null || string.IsNullOrWhiteSpace(connection.Server))
                throw new InvalidOperationException(
                    $"AMQP queue '{queue.DisplayName}': Broker Host is not configured. Set it in the AMQP settings or override the connection settings on the queue.");

            string host = connection.Server.Trim();
            int port = connection.GetEffectivePort();

            var factory = new ConnectionFactory();
            factory.AMQP.ContainerId = containerId;
            if (!string.IsNullOrWhiteSpace(connection.VirtualHost))
                factory.AMQP.HostName = connection.VirtualHost.Trim();
            if (connection.IdleTimeoutSeconds > 0)
                factory.AMQP.IdleTimeout = connection.IdleTimeoutSeconds * 1000;

            if (connection.UseTls)
            {
                // None = let the OS choose (TLS 1.2 and 1.3). The library default excludes TLS 1.3.
                factory.SSL.Protocols = SslProtocols.None;
                if (connection.AllowUntrustedCertificates)
                    factory.SSL.RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true;
            }

            if (!string.IsNullOrWhiteSpace(credentials?.ClientCertificate))
            {
                if (!connection.UseTls)
                    throw new InvalidOperationException($"AMQP queue '{queue.DisplayName}': a client certificate requires TLS.");
                factory.SSL.ClientCertificates.Add(LoadCertificate(credentials.ClientCertificate, credentials.ClientCertificatePassword));
            }

            string user = null;
            string password = null;
            switch (credentials?.AuthenticationMethod)
            {
                case AmqpOptions.AUTH_ANONYMOUS:
                case AmqpOptions.AUTH_CBS:
                    factory.SASL.Profile = SaslProfile.Anonymous;
                    break;
                case AmqpOptions.AUTH_EXTERNAL:
                    if (string.IsNullOrWhiteSpace(credentials.ClientCertificate))
                        throw new InvalidOperationException($"AMQP queue '{queue.DisplayName}': Client Certificate authentication needs a client certificate.");
                    factory.SASL.Profile = SaslProfile.External;
                    break;
                default:
                    if (string.IsNullOrEmpty(credentials?.Username) && string.IsNullOrEmpty(credentials?.Password))
                    {
                        factory.SASL.Profile = SaslProfile.Anonymous;
                    }
                    else
                    {
                        // The (host, port, user, password, ...) constructor doesn't parse a URL, so special
                        // characters in passwords and tokens need no escaping.
                        user = credentials.Username ?? string.Empty;
                        password = credentials.Password ?? string.Empty;
                    }
                    break;
            }

            var address = new Address(host, port, user, password, "/", connection.UseTls ? "AMQPS" : "AMQP");
            TimeSpan timeout = TimeSpan.FromSeconds(Math.Max(1, connection.ConnectionTimeoutSeconds));

            Task<Connection> connect = factory.CreateAsync(address);
            try
            {
                if (!connect.Wait(timeout))
                {
                    connect.ContinueWith(t => SafeClose(t.Result), TaskContinuationOptions.OnlyOnRanToCompletion);
                    throw new TimeoutException($"Timed out after {timeout.TotalSeconds:0} seconds connecting to AMQP broker {host}:{port}.");
                }
            }
            catch (AggregateException ex)
            {
                Exception inner = ex.GetBaseException();
                throw new InvalidOperationException($"Could not connect to AMQP broker {host}:{port}: {DescribeException(inner)}", inner);
            }
            return connect.Result;
        }

        private static X509Certificate2 LoadCertificate(string base64, string password)
        {
            try
            {
                return X509CertificateLoader.LoadPkcs12(Convert.FromBase64String(base64.Trim()), password);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"The client certificate could not be loaded (it must be a Base64-encoded PFX with the right password): {ex.Message}", ex);
            }
        }

        /// <summary>
        /// For CBS authentication (Azure Service Bus / Event Hubs): puts the access token for <paramref name="address"/>
        /// on the connection. Must be done before attaching a link to that address. No-op for other methods.
        /// </summary>
        public static void AuthorizeAddress(AmqpMessageQueue queue, Session session, string address)
        {
            IAmqpCredentialSettings credentials = GetCredentialSettings(queue);
            if (credentials?.AuthenticationMethod != AmqpOptions.AUTH_CBS)
                return;
            if (string.IsNullOrEmpty(credentials.Password))
                throw new InvalidOperationException($"AMQP queue '{queue.DisplayName}': no access token is configured for CBS authentication.");

            IAmqpConnectionSettings connection = GetConnectionSettings(queue);
            string audience = !string.IsNullOrWhiteSpace(queue.CbsAudience)
                ? queue.CbsAudience.Trim()
                : $"amqp://{connection.Server.Trim()}/{address.TrimStart('/')}";
            string tokenType = credentials.CbsTokenType == AmqpOptions.CBS_TOKEN_SAS ? "servicebus.windows.net:sastoken" : "jwt";
            TimeSpan timeout = TimeSpan.FromSeconds(Math.Max(1, connection.ConnectionTimeoutSeconds));

            string id = Guid.NewGuid().ToString("N");
            var sender = new SenderLink(session, "cbs-sender-" + id, "$cbs");
            var receiver = new ReceiverLink(session, "cbs-receiver-" + id, "$cbs");
            try
            {
                var request = new Message(credentials.Password)
                {
                    Properties = new Properties { MessageId = id, ReplyTo = "cbs" },
                    ApplicationProperties = new ApplicationProperties()
                };
                request.ApplicationProperties["operation"] = "put-token";
                request.ApplicationProperties["type"] = tokenType;
                request.ApplicationProperties["name"] = audience;
                sender.Send(request, timeout);

                Message response = receiver.Receive(timeout);
                if (response == null)
                    throw new TimeoutException("No response from the broker's CBS node.");
                receiver.Accept(response);

                int status = Convert.ToInt32(response.ApplicationProperties?["status-code"] ?? 0);
                if (status != 200 && status != 202)
                    throw new UnauthorizedAccessException(
                        $"The broker rejected the access token for '{audience}' (status {status}: {response.ApplicationProperties?["status-description"]}).");
            }
            finally
            {
                SafeClose(sender);
                SafeClose(receiver);
            }
        }

        /// <summary>
        /// Opens a receiver on the queue's address with its filters. Blocks until the broker has confirmed the
        /// link, and checks that the broker kept the filters (unless 'Stop If Broker Ignores Filter' is off).
        /// </summary>
        public static AmqpReceiverHandle OpenReceiver(AmqpMessageQueue queue, string containerId, string linkName,
            int credit, string expressionOverride = null, ClosedCallback onClosed = null)
        {
            Map filterSet = AmqpFilterBuilder.BuildFilterSet(queue, expressionOverride);
            var handle = new AmqpReceiverHandle();
            try
            {
                handle.Connection = Connect(queue, containerId);
                if (onClosed != null)
                    handle.Connection.AddClosedCallback(onClosed);
                handle.Session = new Session(handle.Connection);
                AuthorizeAddress(queue, handle.Session, queue.Address);

                var source = new Source { Address = queue.Address, FilterSet = filterSet };
                var capabilities = new List<Symbol>();
                if (queue.ShowAddressType && queue.AddressType == AmqpOptions.ADDRESS_QUEUE)
                    capabilities.Add(new Symbol("queue"));
                if (queue.IsTopic)
                {
                    capabilities.Add(new Symbol("topic"));
                    if (!string.IsNullOrWhiteSpace(queue.DurableSubscriptionName))
                    {
                        // Durable subscription: survives disconnects (Artemis, Qpid, Solace, Classic).
                        source.Durable = 2;
                        source.ExpiryPolicy = new Symbol("never");
                        linkName = queue.DurableSubscriptionName.Trim();
                        if (queue.SharedSubscription)
                        {
                            capabilities.Add(new Symbol("shared"));
                            capabilities.Add(new Symbol("global"));
                        }
                    }
                }
                if (capabilities.Count > 0)
                    source.Capabilities = capabilities.ToArray();

                Attach remoteAttach = null;
                Error linkError = null;
                // Not disposed: the closed callback can still fire long after this method returned.
                var attached = new ManualResetEventSlim(false);
                var closed = new ManualResetEventSlim(false);
                handle.Link = new ReceiverLink(handle.Session, linkName, source, (link, attach) =>
                {
                    remoteAttach = attach;
                    attached.Set();
                });
                handle.Link.AddClosedCallback((sender, error) =>
                {
                    linkError = error;
                    closed.Set();
                    onClosed?.Invoke(sender, error);
                });

                TimeSpan timeout = TimeSpan.FromSeconds(Math.Max(1, GetConnectionSettings(queue).ConnectionTimeoutSeconds));
                WaitHandle.WaitAny(new[] { attached.WaitHandle, closed.WaitHandle }, timeout);

                if (remoteAttach == null || remoteAttach.Source == null)
                {
                    // A refusing broker answers with an attach without source, immediately followed by a detach with the reason.
                    closed.Wait(TimeSpan.FromSeconds(2));
                    string reason = linkError != null ? $"{linkError.Condition}: {linkError.Description}" : "no response from the broker";
                    throw new InvalidOperationException($"The broker refused to receive from address '{queue.Address}' ({reason}).");
                }

                CheckFilters(queue, filterSet, remoteAttach);

                handle.Link.SetCredit(Math.Max(1, credit), true);
                return handle;
            }
            catch
            {
                handle.Dispose();
                throw;
            }
        }

        private static void CheckFilters(AmqpMessageQueue queue, Map requested, Attach remoteAttach)
        {
            string[] requestedKeys = AmqpFilterBuilder.GetKeys(requested);
            if (requestedKeys.Length == 0)
                return;

            string[] missing = requestedKeys.Where(k => !IsFilterApplied(queue, k, remoteAttach)).ToArray();
            if (missing.Length == 0)
                return;

            string message = $"The broker did not accept the filter(s) {string.Join(", ", missing.Select(m => $"'{m}'"))} for queue '{queue.DisplayName}' " +
                             $"(address '{queue.Address}'). Check that the broker and address support this kind of filter";
            if (queue.IsRabbitMq)
                message += " (RabbitMQ only supports filters on streams, and SQL filters need RabbitMQ 4.2+)";
            message += ".";

            if (queue.VerifyFilterAccepted)
                throw new InvalidOperationException(message + " The queue was stopped so it doesn't receive unfiltered messages.");
            Log.Warn("[AMQP] " + message + " Continuing because 'Stop If Broker Ignores Filter' is off.");
        }

        /// <summary>True if the broker's attach reply shows that it applies the filter with this key.</summary>
        internal static bool IsFilterApplied(AmqpMessageQueue queue, string key, Attach remoteAttach)
        {
            var remoteSource = remoteAttach?.Source as Source;
            if (!AmqpFilterBuilder.GetKeys(remoteSource?.FilterSet).Contains(key))
                return false;
            return !queue.IsRabbitMq || RabbitMqApplies(key, remoteAttach, remoteSource);
        }

        /// <summary>
        /// RabbitMQ (checked on 4.2.9) echoes SQL and property filters back on classic and quorum queues but doesn't
        /// apply them, so the echo alone proves nothing. Only streams offer the filter-expression capabilities, and
        /// selectors are only applied where the source offers the selector capability.
        /// </summary>
        private static bool RabbitMqApplies(string key, Attach remoteAttach, Source remoteSource)
        {
            var offered = new HashSet<string>(
                (remoteAttach.OfferedCapabilities ?? Array.Empty<Symbol>()).Select(c => c.ToString()), StringComparer.OrdinalIgnoreCase);
            var sourceCapabilities = new HashSet<string>(
                (remoteSource?.Capabilities ?? Array.Empty<Symbol>()).Select(c => c.ToString()), StringComparer.OrdinalIgnoreCase);
            bool isStream = offered.Contains("AMQP_FILTEX_SQL_V1_0") || offered.Contains("AMQP_FILTEX_PROP_V1_0");

            if (key.StartsWith("rabbitmq:stream-", StringComparison.Ordinal))
                return isStream;
            if (key == AmqpFilterBuilder.SQL_KEY)
                return offered.Contains("AMQP_FILTEX_SQL_V1_0");
            if (key == AmqpFilterBuilder.SELECTOR_KEY)
                return sourceCapabilities.Contains("APACHE.ORG:SELECTOR");
            return offered.Contains("AMQP_FILTEX_PROP_V1_0");
        }

        // ---------------- Sending ----------------

        /// <summary>Sends one message on a new connection, waiting for the broker to accept it.</summary>
        public static void Send(AmqpMessageQueue queue, string address, Message message)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new InvalidOperationException($"AMQP queue '{queue.DisplayName}' has no address to send to.");

            Connection connection = Connect(queue, GetDefaultContainerId(queue, "send-" + Guid.NewGuid().ToString("N")));
            try
            {
                var session = new Session(connection);
                AuthorizeAddress(queue, session, address);
                var sender = new SenderLink(session, "decisions-sender-" + Guid.NewGuid().ToString("N"), address);
                try
                {
                    sender.Send(message, TimeSpan.FromSeconds(Math.Max(1, GetConnectionSettings(queue).ConnectionTimeoutSeconds)));
                }
                finally
                {
                    SafeClose(sender);
                }
                SafeClose(session);
            }
            finally
            {
                SafeClose(connection);
            }
        }

        // ---------------- Helpers ----------------

        internal static void SafeClose(AmqpObject amqpObject)
        {
            if (amqpObject == null || amqpObject.IsClosed)
                return;
            try
            {
                amqpObject.Close(CloseTimeout);
            }
            catch (Exception ex)
            {
                Log.Debug($"[AMQP] Error while closing {amqpObject.GetType().Name}: {ex.Message}");
            }
        }

        public static string DescribeException(Exception ex)
        {
            if (ex is AmqpException amqp && amqp.Error != null)
                return $"{amqp.Error.Condition}: {amqp.Error.Description}";
            return ex.Message;
        }
    }
}
