using System;
using System.Linq;
using System.Text;
using DecisionsFramework;
using DecisionsFramework.Data.Messaging;
using Decisions.MessageQueues;

namespace Decisions.AmqpMessageQueue
{
    public class AmqpMessageQueueImpl : SimpleMessageQueueImpl<AmqpMessageQueue>
    {
        private static readonly Log Log = new Log("AMQP");

        protected override IMqThreadManager CreateThreadManager()
            => new BaseMqThreadManager<AmqpThreadJob, AmqpMessageQueue>("AMQP", QueueDefinition);

        protected override string QueueTypeName => "AMQP";

        public AmqpMessageQueueImpl(AmqpMessageQueue queueDef) : base(queueDef)
        {
            // A new impl means the queue (re)started, e.g. after a save or credential update: connect right away.
            AmqpThreadJob.ResetRetryDelay(queueDef.Id);
        }

        public override MessageQueueMessage GetMessage(string handlerId, MessageFetchType messageFetchType, bool blockForMessage)
        {
            Log.Warn("GetMessage is not supported for AMQP queues - use a queue handler or the 'Receive AMQP Message' step");
            return null;
        }

        // AMQP 1.0 has no standard way to ask a broker for a queue's message count.
        public override long? GetMessageCount() => null;

        public override long MessageCount
            => throw new NotSupportedException("Message count is not available for AMQP");

        /// <summary>Used by the queue's "Test" action: connects, attaches with the filters and reports the result.</summary>
        public override string GetAlternativeTestQueueResult()
        {
            try
            {
                string containerId = AmqpConnectionHelper.GetDefaultContainerId(QueueDefinition, "test-" + Guid.NewGuid().ToString("N"));
                using (AmqpConnectionHelper.OpenReceiver(QueueDefinition, containerId, "decisions-test-" + Guid.NewGuid().ToString("N"), 1))
                {
                }

                var result = new StringBuilder($"Connected to the AMQP broker and attached to '{QueueDefinition.Address}' for '{QueueDefinition.DisplayName}'.");
                string[] keys = AmqpFilterBuilder.GetKeys(AmqpFilterBuilder.BuildFilterSet(QueueDefinition));
                if (keys.Length > 0)
                    result.Append($" Filters accepted by the broker: {string.Join(", ", keys)}.");
                return result.ToString();
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"AMQP connection test failed for '{QueueDefinition.DisplayName}'");
                return $"AMQP connection test failed for '{QueueDefinition.DisplayName}': {AmqpConnectionHelper.DescribeException(ex)}";
            }
        }

        /// <summary>Called by the generic message queue steps/APIs; sends the bytes to the queue's publish address.</summary>
        public override void PushMessage(string id, byte[] message)
        {
            var outgoing = new AmqpOutgoingMessage
            {
                Body = Encoding.UTF8.GetString(message ?? Array.Empty<byte>()),
                BinaryBody = AmqpMessageConverter.UseBinaryBody(AmqpOptions.BODY_AUTO, QueueDefinition.BrokerType),
                MessageId = id
            };
            AmqpConnectionHelper.Send(QueueDefinition, QueueDefinition.GetPublishAddress(), AmqpMessageConverter.Build(outgoing));
        }
    }
}
