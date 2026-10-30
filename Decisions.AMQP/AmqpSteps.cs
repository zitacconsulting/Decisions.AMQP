using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Amqp;
using Amqp.Framing;
using DecisionsFramework;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.Design.Flow.Mapping.InputImpl;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.Properties.Attributes;
using DecisionsFramework.ServiceLayer.Services.ContextData;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// Base class for the AMQP steps: picks a configured AMQP queue, either at design time from a dropdown
    /// or at runtime from a "Queue Name" input.
    /// </summary>
    [Writable]
    public abstract class BaseAmqpStep : BaseFlowAwareStep, IValidationSource
    {
        protected static readonly Log Log = new Log("AMQP Step");

        protected const string INPUT_QUEUE_NAME = "Queue Name";
        private const string NO_QUEUES = "-- Configure AMQP queues in Jobs & Events > Messaging";

        [WritableValue]
        protected string queueName;

        [WritableValue]
        protected bool queueNameFromInput;

        [PropertyClassification(1, "Queue Name From Input", "Queue")]
        public virtual bool QueueNameFromInput
        {
            get { return queueNameFromInput; }
            set { queueNameFromInput = value; OnPropertyChanged(); OnPropertyChanged(nameof(HideQueueDropdown)); OnPropertyChanged("InputData"); }
        }

        [PropertyHidden]
        public virtual bool HideQueueDropdown => queueNameFromInput;

        [PropertyClassification(2, "Queue", "Queue")]
        [SelectStringEditor(nameof(QueueNames))]
        [BooleanPropertyHidden(nameof(HideQueueDropdown), true)]
        public string QueueName
        {
            get { return queueName; }
            set { queueName = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] QueueNames
        {
            get
            {
                var queues = AmqpConnectionHelper.GetQueues(Flow?.GetProjectId());
                return queues.Length == 0 ? new[] { NO_QUEUES } : queues.Select(q => q.DisplayName).ToArray();
            }
            set { }
        }

        protected virtual bool NeedsQueueSelection => !queueNameFromInput;

        protected void AddQueueNameInput(List<DataDescription> inputs)
        {
            if (queueNameFromInput)
                inputs.Add(new DataDescription(typeof(string), INPUT_QUEUE_NAME) { Categories = new[] { "Queue" } });
        }

        protected AmqpMessageQueue GetQueue(StepStartData data)
        {
            string name = queueNameFromInput ? GetInput<string>(data, INPUT_QUEUE_NAME) : queueName;
            if (string.IsNullOrWhiteSpace(name) || name.StartsWith("--"))
                throw new InvalidOperationException("No AMQP queue selected.");
            return AmqpConnectionHelper.FindQueue(name, Flow?.GetProjectId())
                ?? throw new InvalidOperationException($"AMQP queue '{name}' not found.");
        }

        protected static T GetInput<T>(StepStartData data, string name)
        {
            if (!data.ContainsKey(name) || data[name] == null)
                return default;
            object value = data[name];
            if (value is T typed)
                return typed;
            return (T)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T));
        }

        public virtual ValidationIssue[] GetValidationIssues()
        {
            if (NeedsQueueSelection && (string.IsNullOrEmpty(queueName) || queueName.StartsWith("--")))
                return new[] { new ValidationIssue(this, "An AMQP queue must be selected", "", BreakLevel.Fatal) };
            return Array.Empty<ValidationIssue>();
        }
    }

    /// <summary>
    /// Sends a message using the selected queue's connection, to its publish address or an override.
    /// Optionally waits for a reply (request/reply).
    /// </summary>
    [Writable]
    [ValidationRules]
    [AutoRegisterStep("Send AMQP Message", "Integration/AMQP")]
    public class SendAmqpMessage : BaseAmqpStep, ISyncStep, IDataConsumer, IDataProducer, IDefaultInputMappingStep
    {
        private const string INPUT_MESSAGE = "Message";
        private const string INPUT_ADDRESS = "Address Override";
        private const string INPUT_APP_PROPERTIES = "Application Properties";
        private const string INPUT_ANNOTATIONS = "Message Annotations";
        private const string INPUT_SUBJECT = "Subject";
        private const string INPUT_MESSAGE_ID = "Message ID";
        private const string INPUT_CORRELATION_ID = "Correlation ID";
        private const string INPUT_CONTENT_TYPE = "Content Type";
        private const string INPUT_GROUP_ID = "Group ID";
        private const string INPUT_REPLY_TO = "Reply To";
        private const string INPUT_PRIORITY = "Priority";
        private const string INPUT_TTL = "Time To Live (ms)";
        private const string INPUT_DURABLE = "Durable";
        private const string INPUT_REPLY_ADDRESS = "Reply Address";
        private const string INPUT_REPLY_TIMEOUT = "Reply Timeout (seconds)";

        private const string OUTCOME_DONE = "Done";
        private const string OUTCOME_REPLY = "Reply Received";
        private const string OUTCOME_TIMEOUT = "Timeout";

        [WritableValue]
        private string bodyFormat = AmqpOptions.BODY_AUTO;

        [PropertyClassification(1, "Body Format", "Settings")]
        [SelectStringEditor(nameof(BodyFormats))]
        public string BodyFormat
        {
            get { return bodyFormat; }
            set { bodyFormat = value; OnPropertyChanged(); }
        }

        [PropertyHidden]
        public string[] BodyFormats => AmqpOptions.BodyFormats;

        [WritableValue]
        private bool expectReply;

        [PropertyClassification(2, "Wait For Reply", "Settings")]
        public bool ExpectReply
        {
            get { return expectReply; }
            set
            {
                expectReply = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(InputData));
                OnPropertyChanged(nameof(OutcomeScenarios));
            }
        }

        [ReadonlyEditor]
        [PropertyClassification(3, "Wait For Reply Info", "Settings")]
        [BooleanPropertyHidden(nameof(ExpectReply), false)]
        public string ExpectReplyNote
        {
            get => "Sends the message and waits for a reply whose Correlation ID equals the request's Correlation ID " +
                   "(or its Message ID when no Correlation ID is given). Leave 'Reply Address' empty to let the broker " +
                   "create a temporary reply queue (most brokers support this). With a fixed reply address the reply " +
                   "is selected with a broker filter, which needs filter support on that address.";
            set { }
        }

        public IInputMapping[] DefaultInputs
        {
            get
            {
                var mappings = new List<IInputMapping>
                {
                    new IgnoreInputMapping { InputDataName = INPUT_ADDRESS },
                    new IgnoreInputMapping { InputDataName = INPUT_APP_PROPERTIES },
                    new IgnoreInputMapping { InputDataName = INPUT_ANNOTATIONS },
                    new IgnoreInputMapping { InputDataName = INPUT_SUBJECT },
                    new IgnoreInputMapping { InputDataName = INPUT_MESSAGE_ID },
                    new IgnoreInputMapping { InputDataName = INPUT_CORRELATION_ID },
                    new IgnoreInputMapping { InputDataName = INPUT_CONTENT_TYPE },
                    new IgnoreInputMapping { InputDataName = INPUT_GROUP_ID },
                    new IgnoreInputMapping { InputDataName = INPUT_REPLY_TO },
                    new IgnoreInputMapping { InputDataName = INPUT_PRIORITY },
                    new IgnoreInputMapping { InputDataName = INPUT_TTL },
                    new ConstantInputMapping { InputDataName = INPUT_DURABLE, Value = true },
                    new IgnoreInputMapping { InputDataName = INPUT_REPLY_ADDRESS },
                    new ConstantInputMapping { InputDataName = INPUT_REPLY_TIMEOUT, Value = 30 }
                };
                return mappings.ToArray();
            }
        }

        public DataDescription[] InputData
        {
            get
            {
                var inputs = new List<DataDescription>();
                AddQueueNameInput(inputs);
                inputs.Add(new DataDescription(typeof(string), INPUT_MESSAGE) { Categories = new[] { "Message" } });
                inputs.Add(Optional(typeof(string), INPUT_ADDRESS, "Message"));
                inputs.Add(new DataDescription(new DecisionsNativeType(typeof(DataPair)), INPUT_APP_PROPERTIES, true, true, false) { Categories = new[] { "Message" } });
                inputs.Add(Optional(typeof(string), INPUT_CONTENT_TYPE, "Properties"));
                inputs.Add(Optional(typeof(string), INPUT_SUBJECT, "Properties"));
                inputs.Add(Optional(typeof(string), INPUT_MESSAGE_ID, "Properties"));
                inputs.Add(Optional(typeof(string), INPUT_CORRELATION_ID, "Properties"));
                inputs.Add(Optional(typeof(string), INPUT_GROUP_ID, "Properties"));
                if (!expectReply)
                    inputs.Add(Optional(typeof(string), INPUT_REPLY_TO, "Properties"));
                inputs.Add(Optional(typeof(int?), INPUT_PRIORITY, "Properties"));
                inputs.Add(Optional(typeof(long?), INPUT_TTL, "Properties"));
                inputs.Add(Optional(typeof(bool), INPUT_DURABLE, "Properties"));
                inputs.Add(new DataDescription(new DecisionsNativeType(typeof(DataPair)), INPUT_ANNOTATIONS, true, true, false) { Categories = new[] { "Properties" } });
                if (expectReply)
                {
                    inputs.Add(Optional(typeof(string), INPUT_REPLY_ADDRESS, "Reply"));
                    inputs.Add(Optional(typeof(int), INPUT_REPLY_TIMEOUT, "Reply"));
                }
                return inputs.ToArray();
            }
        }

        private static DataDescription Optional(Type type, string name, string category)
            => new DataDescription(new DecisionsNativeType(type), name, false, true, false) { Categories = new[] { category } };

        public override OutcomeScenarioData[] OutcomeScenarios
        {
            get
            {
                if (!expectReply)
                    return new[] { new OutcomeScenarioData(OUTCOME_DONE, new DataDescription(typeof(string), "Message ID")) };
                return new[]
                {
                    new OutcomeScenarioData(OUTCOME_REPLY, new DataDescription(typeof(AmqpReceivedMessage), "Reply")),
                    new OutcomeScenarioData(OUTCOME_TIMEOUT)
                };
            }
        }

        public ResultData Run(StepStartData data)
        {
            AmqpMessageQueue queue = GetQueue(data);
            string address = GetInput<string>(data, INPUT_ADDRESS);
            if (string.IsNullOrWhiteSpace(address))
                address = queue.GetPublishAddress();

            var outgoing = new AmqpOutgoingMessage
            {
                Body = GetInput<string>(data, INPUT_MESSAGE),
                BinaryBody = AmqpMessageConverter.UseBinaryBody(bodyFormat, queue.BrokerType),
                ApplicationProperties = GetInput<DataPair[]>(data, INPUT_APP_PROPERTIES),
                MessageAnnotations = GetInput<DataPair[]>(data, INPUT_ANNOTATIONS),
                ContentType = GetInput<string>(data, INPUT_CONTENT_TYPE),
                Subject = GetInput<string>(data, INPUT_SUBJECT),
                MessageId = GetInput<string>(data, INPUT_MESSAGE_ID),
                CorrelationId = GetInput<string>(data, INPUT_CORRELATION_ID),
                GroupId = GetInput<string>(data, INPUT_GROUP_ID),
                ReplyTo = GetInput<string>(data, INPUT_REPLY_TO),
                Priority = GetInput<int?>(data, INPUT_PRIORITY),
                TimeToLiveMs = GetInput<long?>(data, INPUT_TTL),
                Durable = !data.ContainsKey(INPUT_DURABLE) || data[INPUT_DURABLE] == null || (bool)data[INPUT_DURABLE]
            };
            if (string.IsNullOrEmpty(outgoing.MessageId))
                outgoing.MessageId = Guid.NewGuid().ToString();

            if (!expectReply)
            {
                AmqpConnectionHelper.Send(queue, address, AmqpMessageConverter.Build(outgoing));
                Log.Debug($"[AMQP] Sent message {outgoing.MessageId} to '{address}' using queue '{queue.DisplayName}'");
                return new ResultData(OUTCOME_DONE, new Dictionary<string, object> { { "Message ID", outgoing.MessageId } });
            }

            int timeoutSeconds = GetInput<int?>(data, INPUT_REPLY_TIMEOUT) ?? 30;
            AmqpReceivedMessage reply = SendAndWaitForReply(queue, address, outgoing, GetInput<string>(data, INPUT_REPLY_ADDRESS),
                TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds)));
            if (reply == null)
                return new ResultData(OUTCOME_TIMEOUT);
            return new ResultData(OUTCOME_REPLY, new Dictionary<string, object> { { "Reply", reply } });
        }

        private static AmqpReceivedMessage SendAndWaitForReply(AmqpMessageQueue queue, string address, AmqpOutgoingMessage outgoing,
            string replyAddress, TimeSpan timeout)
        {
            string expectedCorrelationId = string.IsNullOrEmpty(outgoing.CorrelationId) ? outgoing.MessageId : outgoing.CorrelationId;
            string id = Guid.NewGuid().ToString("N");
            Connection connection = AmqpConnectionHelper.Connect(queue, AmqpConnectionHelper.GetDefaultContainerId(queue, "rpc-" + id));
            try
            {
                var session = new Session(connection);
                ReceiverLink receiver = AttachReplyReceiver(queue, session, replyAddress, expectedCorrelationId, id, out string replyTo);
                outgoing.ReplyTo = replyTo;

                AmqpConnectionHelper.AuthorizeAddress(queue, session, address);
                var sender = new SenderLink(session, "decisions-request-" + id, address);
                sender.Send(AmqpMessageConverter.Build(outgoing), timeout);
                AmqpConnectionHelper.SafeClose(sender);

                var stopwatch = Stopwatch.StartNew();
                while (stopwatch.Elapsed < timeout)
                {
                    Message message = receiver.Receive(timeout - stopwatch.Elapsed);
                    if (message == null)
                        break;
                    receiver.Accept(message);
                    AmqpReceivedMessage reply = AmqpMessageConverter.ToReceivedMessage(message);
                    if (reply.CorrelationId == expectedCorrelationId)
                        return reply;
                    Log.Warn($"[AMQP] Ignored a message on reply address '{replyTo}' with correlation ID '{reply.CorrelationId}' (expected '{expectedCorrelationId}')");
                }
                return null;
            }
            finally
            {
                AmqpConnectionHelper.SafeClose(connection);
            }
        }

        private static ReceiverLink AttachReplyReceiver(AmqpMessageQueue queue, Session session, string replyAddress,
            string correlationId, string id, out string replyTo)
        {
            bool dynamic = string.IsNullOrWhiteSpace(replyAddress);
            var source = new Source { Address = dynamic ? null : replyAddress, Dynamic = dynamic };
            if (dynamic)
            {
                // RabbitMQ 4.x only creates a dynamic node with this (JMS standard) capability.
                source.Capabilities = new[] { new Amqp.Types.Symbol("temporary-queue") };
                source.ExpiryPolicy = new Amqp.Types.Symbol("link-detach");
            }
            string filterKey = null;
            if (!dynamic && !queue.IsServiceBus)
            {
                source.FilterSet = new Amqp.Types.Map();
                AmqpFilterBuilder.AddExpression(source.FilterSet, queue.BrokerType, AmqpFilterBuilder.CorrelationIdExpression(queue.BrokerType, correlationId));
                filterKey = AmqpFilterBuilder.GetKeys(source.FilterSet)[0];
            }
            if (!dynamic)
                AmqpConnectionHelper.AuthorizeAddress(queue, session, replyAddress);

            Attach remoteAttach = null;
            var attached = new ManualResetEventSlim(false);
            var receiver = new ReceiverLink(session, "decisions-reply-" + id, source, (link, attach) =>
            {
                remoteAttach = attach;
                attached.Set();
            });
            attached.Wait(TimeSpan.FromSeconds(Math.Max(1, AmqpConnectionHelper.GetConnectionSettings(queue).ConnectionTimeoutSeconds)));

            Source remoteSource = remoteAttach?.Source as Source;
            if (remoteSource == null)
                throw new InvalidOperationException(dynamic
                    ? "The broker refused to create a temporary reply queue. Enter a Reply Address instead."
                    : $"The broker refused to receive from reply address '{replyAddress}'.");

            if (filterKey != null && !AmqpConnectionHelper.IsFilterApplied(queue, filterKey, remoteAttach))
                throw new InvalidOperationException(
                    $"The broker doesn't support filtering on reply address '{replyAddress}', so the reply can't be selected by correlation ID. " +
                    "Leave Reply Address empty to use a temporary reply queue.");

            replyTo = dynamic ? remoteSource.Address : replyAddress;
            receiver.SetCredit(10, true);
            return receiver;
        }
    }

    /// <summary>Waits for one message on the selected queue's address (outside of a queue handler).</summary>
    [Writable]
    [ValidationRules]
    [AutoRegisterStep("Receive AMQP Message", "Integration/AMQP")]
    public class ReceiveAmqpMessage : BaseAmqpStep, ISyncStep, IDataConsumer, IDataProducer, IDefaultInputMappingStep
    {
        private const string INPUT_TIMEOUT = "Timeout (seconds)";
        private const string INPUT_FILTER = "Filter Expression Override";
        private const string OUTCOME_RECEIVED = "Message Received";
        private const string OUTCOME_NONE = "No Message";

        [ReadonlyEditor]
        [PropertyClassification(1, "Receive Info", "Settings")]
        public string ReceiveNote
        {
            get => "Receives one message from the queue's address using the queue's filter, and removes it from the broker. " +
                   "'Filter Expression Override' replaces the queue's rules or expression for this step (JMS selector syntax, " +
                   "or SQL filter syntax for RabbitMQ). Don't use this on a queue that also has a running queue handler, " +
                   "unless you want them to compete for messages.";
            set { }
        }

        public IInputMapping[] DefaultInputs => new IInputMapping[]
        {
            new ConstantInputMapping { InputDataName = INPUT_TIMEOUT, Value = 10 },
            new IgnoreInputMapping { InputDataName = INPUT_FILTER }
        };

        public DataDescription[] InputData
        {
            get
            {
                var inputs = new List<DataDescription>();
                AddQueueNameInput(inputs);
                inputs.Add(new DataDescription(new DecisionsNativeType(typeof(int)), INPUT_TIMEOUT, false, true, false));
                inputs.Add(new DataDescription(new DecisionsNativeType(typeof(string)), INPUT_FILTER, false, true, false));
                return inputs.ToArray();
            }
        }

        public override OutcomeScenarioData[] OutcomeScenarios => new[]
        {
            new OutcomeScenarioData(OUTCOME_RECEIVED, new DataDescription(typeof(AmqpReceivedMessage), "Message")),
            new OutcomeScenarioData(OUTCOME_NONE)
        };

        public ResultData Run(StepStartData data)
        {
            AmqpMessageQueue queue = GetQueue(data);
            int timeoutSeconds = GetInput<int?>(data, INPUT_TIMEOUT) ?? 10;
            string filter = GetInput<string>(data, INPUT_FILTER);
            string id = Guid.NewGuid().ToString("N");

            using AmqpReceiverHandle receiver = AmqpConnectionHelper.OpenReceiver(queue,
                AmqpConnectionHelper.GetDefaultContainerId(queue, "receive-" + id), "decisions-receive-" + id, 1, filter);
            Message message = receiver.Link.Receive(TimeSpan.FromSeconds(Math.Max(0, timeoutSeconds)));
            if (message == null)
                return new ResultData(OUTCOME_NONE);

            receiver.Link.Accept(message);
            return new ResultData(OUTCOME_RECEIVED, new Dictionary<string, object> { { "Message", AmqpMessageConverter.ToReceivedMessage(message) } });
        }
    }

    /// <summary>
    /// Stores new credentials on one or more AMQP queues and restarts them on every server. Meant for a scheduled
    /// flow that fetches rotating credentials (passwords, OAuth tokens, SAS tokens) from an identity provider or vault.
    /// </summary>
    [Writable]
    [ValidationRules]
    [AutoRegisterStep("Update AMQP Queue Credentials", "Integration/AMQP")]
    public class UpdateAmqpQueueCredentials : BaseAmqpStep, ISyncStep, IDataConsumer, IDataProducer, IDefaultInputMappingStep
    {
        public const string TARGET_SELECTED = "Selected Queue";
        public const string TARGET_INPUT = "Queue Name From Input";
        public const string TARGET_ALL_IN_PROJECT = "All AMQP Queues In This Project";

        private const string INPUT_USERNAME = "Username";
        private const string INPUT_SECRET = "Password or Token";
        private const string OUTPUT_UPDATED = "Restarted Queues";

        [WritableValue]
        private string target = TARGET_SELECTED;

        [PropertyClassification(0, "Queues To Update", "Queue")]
        [SelectStringEditor(nameof(Targets))]
        public string Target
        {
            get { return target; }
            set
            {
                target = value;
                queueNameFromInput = value == TARGET_INPUT;
                OnPropertyChanged();
                OnPropertyChanged(nameof(HideQueueDropdown));
                OnPropertyChanged(nameof(InputData));
            }
        }

        [PropertyHidden]
        public override bool HideQueueDropdown => !ShowQueueDropdown;

        [PropertyHidden]
        public string[] Targets => new[] { TARGET_SELECTED, TARGET_INPUT, TARGET_ALL_IN_PROJECT };

        // Driven by Target instead.
        [PropertyHidden]
        public override bool QueueNameFromInput
        {
            get { return queueNameFromInput; }
            set { }
        }

        [PropertyHidden]
        public bool ShowQueueDropdown => target == TARGET_SELECTED || string.IsNullOrEmpty(target);

        protected override bool NeedsQueueSelection => ShowQueueDropdown;

        [ReadonlyEditor]
        [PropertyClassification(3, "Update Credentials Info", "Queue")]
        public string UpdateNote
        {
            get => "Stores the new credentials on the queue ('Override Credentials' is turned on automatically, keeping the " +
                   "authentication method from the settings) and restarts the queue on every Decisions server so it " +
                   "reconnects with them. Queues whose credentials didn't change are not restarted.\n\n" +
                   "'Password or Token' is the password, the RabbitMQ OAuth 2.0 access token, the Azure Service Bus shared " +
                   "access key, or the CBS access token, depending on the queue's authentication. Leave 'Username' empty to keep the current username.";
            set { }
        }

        public IInputMapping[] DefaultInputs => new IInputMapping[]
        {
            new IgnoreInputMapping { InputDataName = INPUT_USERNAME }
        };

        public DataDescription[] InputData
        {
            get
            {
                var inputs = new List<DataDescription>();
                AddQueueNameInput(inputs);
                inputs.Add(new DataDescription(new DecisionsNativeType(typeof(string)), INPUT_USERNAME, false, true, false) { Categories = new[] { "Credentials" } });
                inputs.Add(new DataDescription(typeof(string), INPUT_SECRET) { Categories = new[] { "Credentials" } });
                return inputs.ToArray();
            }
        }

        public override OutcomeScenarioData[] OutcomeScenarios => new[]
        {
            new OutcomeScenarioData("Done", new DataDescription(typeof(string), OUTPUT_UPDATED, true))
        };

        public ResultData Run(StepStartData data)
        {
            string username = GetInput<string>(data, INPUT_USERNAME);
            string secret = GetInput<string>(data, INPUT_SECRET);
            if (string.IsNullOrEmpty(secret))
                throw new InvalidOperationException("'Password or Token' is empty. The credentials were not updated.");

            AmqpMessageQueue[] queues = target == TARGET_ALL_IN_PROJECT
                ? AmqpConnectionHelper.GetSettings(Flow?.GetProjectId())?.Queues ?? Array.Empty<AmqpMessageQueue>()
                : new[] { GetQueue(data) };

            var restarted = new List<string>();
            foreach (AmqpMessageQueue queue in queues)
            {
                if (UpdateQueue(queue, username, secret))
                    restarted.Add(queue.DisplayName);
            }

            Log.Info($"[AMQP] Credentials updated and queue restarted for: {(restarted.Count == 0 ? "(no changes)" : string.Join(", ", restarted))}");
            return new ResultData("Done", new Dictionary<string, object> { { OUTPUT_UPDATED, restarted.ToArray() } });
        }

        private static bool UpdateQueue(AmqpMessageQueue queue, string username, string secret)
        {
            bool changed = false;
            if (!queue.OverrideCredentials)
            {
                // Take over the settings' authentication so the queue keeps working the same way.
                AmqpQueueSettings settings = AmqpConnectionHelper.GetSettings(queue.GetProjectId());
                queue.OverrideCredentials = true;
                queue.AuthenticationMethod = settings?.AuthenticationMethod ?? AmqpOptions.AUTH_PLAIN;
                queue.CbsTokenType = settings?.CbsTokenType ?? AmqpOptions.CBS_TOKEN_JWT;
                queue.Username = settings?.Username;
                queue.ClientCertificate = settings?.ClientCertificate;
                queue.ClientCertificatePassword = settings?.ClientCertificatePassword;
                changed = true;
            }

            if (!string.IsNullOrEmpty(username) && username != queue.Username)
            {
                queue.Username = username;
                changed = true;
            }

            if (secret != queue.Password)
            {
                queue.Password = secret;
                changed = true;
            }

            if (!changed)
            {
                Log.Debug($"[AMQP] Credentials for queue '{queue.DisplayName}' are unchanged - not restarting it");
                return false;
            }

            queue.CredentialsLastUpdated = DateTime.UtcNow;
            // Saving restarts the queue here (BaseMqDefinition.AfterSave) and notifies the other servers in the
            // cluster, which reload the queue from the database and restart it too.
            queue.Store();
            return true;
        }
    }
}
