using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using Decisions.MessageQueues;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// One receiver thread of a running AMQP queue. The Decisions.MessageQueues thread manager creates as many of
    /// these as the queue handler's "Active Flow Count" allows. Each thread has its own connection, session and
    /// receiver link. When the connection or link is lost, ReceiveMessages throws and the thread manager starts a
    /// replacement thread, which reconnects with the queue's current settings and credentials.
    /// </summary>
    public class AmqpThreadJob : BaseMqThreadJob<AmqpMessageQueue>
    {
        public override string LogCategory => "AMQP Flow Worker";

        private static readonly TimeSpan LeaseRenewalInterval = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan StandbyPollInterval = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(1);

        private string threadId;
        private bool isLeaseHolder;
        private DateTime lastLeaseRenewal = DateTime.MinValue;
        private AmqpReceiverHandle receiver;
        private volatile bool closing;
        private volatile string connectionLostReason;

        private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60);

        // Consecutive failed connection attempts per queue. The thread manager replaces a failed thread
        // immediately, so without a delay a broker that rejects the connection (e.g. expired credentials)
        // gets several attempts per second (measured ~6/s on 9.32) and the log fills with errors.
        private static readonly ConcurrentDictionary<string, int> ConnectFailures = new ConcurrentDictionary<string, int>();

        private bool UseLease => queueDefinition.UseClusterLease;

        /// <summary>Called when the queue (re)starts, e.g. after a credential update, so it reconnects without delay.</summary>
        internal static void ResetRetryDelay(string queueId) => ConnectFailures.TryRemove(queueId, out _);

        protected override void SetUp()
        {
            threadId = $"{Environment.MachineName}-{Guid.NewGuid():N}";

            if (!UseLease)
            {
                isLeaseHolder = true;
                Connect();
                return;
            }

            isLeaseHolder = AmqpLeaseManager.AcquireLease(queueDefinition.Id, threadId);
            if (isLeaseHolder)
            {
                lastLeaseRenewal = DateTime.UtcNow;
                Connect();
            }
            else
            {
                log.Info($"[AMQP] Thread {threadId} is in standby for queue '{queueDefinition.DisplayName}'");
            }
        }

        protected override void ReceiveMessages()
        {
            if (!isLeaseHolder)
            {
                // Standby ("One connection at a time" only): wait, then try to take over the lease.
                Thread.Sleep(StandbyPollInterval);
                if (isShuttingDown)
                    return;
                isLeaseHolder = AmqpLeaseManager.AcquireLease(queueDefinition.Id, threadId);
                if (isLeaseHolder)
                {
                    lastLeaseRenewal = DateTime.UtcNow;
                    Connect();
                    log.Info($"[AMQP] Thread {threadId} acquired the lease for queue '{queueDefinition.DisplayName}'");
                }
                return;
            }

            if (connectionLostReason != null)
                throw new InvalidOperationException(
                    $"[AMQP] Lost the connection for queue '{queueDefinition.DisplayName}': {connectionLostReason}. Decisions will restart this thread.");

            if (UseLease && DateTime.UtcNow - lastLeaseRenewal >= LeaseRenewalInterval)
            {
                if (!AmqpLeaseManager.AcquireLease(queueDefinition.Id, threadId))
                {
                    log.Warn($"[AMQP] Thread {threadId} lost the lease for queue '{queueDefinition.DisplayName}', going to standby");
                    Disconnect();
                    isLeaseHolder = false;
                    return;
                }
                lastLeaseRenewal = DateTime.UtcNow;
            }

            Message message = receiver.Link.Receive(ReceiveTimeout);
            if (message == null)
            {
                IsActive = false;
                return;
            }

            IsActive = true;
            HandleMessage(message);
        }

        private void HandleMessage(Message message)
        {
            ReceiverLink link = receiver.Link;
            bool ackAfterFlow = queueDefinition.AckAfterFlow;
            if (!ackAfterFlow)
                link.Accept(message);

            bool success;
            try
            {
                AmqpMessageConverter.DecodeBody(message, out byte[] payload, out string text);
                string messageId = AmqpMessageConverter.IdToString(message.Properties?.GetMessageId()) ?? Guid.NewGuid().ToString();
                List<DataPair> headers = new List<DataPair>(AmqpMessageConverter.GetApplicationProperties(message));
                List<DataPair> otherData = AmqpMessageConverter.GetStandardFields(message, queueDefinition.Address);

                success = ProcessMessage(messageId, payload, headers, otherData, AmqpMessageConverter.GetRoutingKey(message), text);
            }
            catch (Exception ex)
            {
                log.Error(ex, $"[AMQP] Could not process a message from queue '{queueDefinition.DisplayName}'");
                success = false;
            }

            if (!ackAfterFlow)
                return;

            if (success)
            {
                link.Accept(message);
                return;
            }

            switch (queueDefinition.FailureAction)
            {
                case AmqpOptions.FAILURE_RELEASE:
                    link.Release(message);
                    break;
                case AmqpOptions.FAILURE_REJECT:
                    link.Reject(message, new Error(new Symbol("amqp:internal-error")) { Description = "The Decisions handler flow failed" });
                    break;
                case AmqpOptions.FAILURE_ACCEPT:
                    link.Accept(message);
                    break;
                default:
                    link.Modify(message, true);
                    break;
            }
            log.Warn($"[AMQP] Handler flow failed for a message on queue '{queueDefinition.DisplayName}' - {queueDefinition.FailureAction}");
        }

        protected override void CleanUp()
        {
            Disconnect();
            if (UseLease && isLeaseHolder)
                AmqpLeaseManager.ReleaseLease(queueDefinition.Id, threadId);
        }

        private void Connect()
        {
            closing = false;
            connectionLostReason = null;

            // Each connection gets its own container ID unless the queue sets a fixed one.
            string containerId = AmqpConnectionHelper.GetDefaultContainerId(queueDefinition, threadId);
            string linkName = $"decisions-{queueDefinition.DisplayName}-{threadId}";

            try
            {
                receiver = AmqpConnectionHelper.OpenReceiver(queueDefinition, containerId, linkName, queueDefinition.PrefetchCount,
                    onClosed: (sender, error) =>
                    {
                        if (closing)
                            return;
                        connectionLostReason = error != null ? $"{error.Condition}: {error.Description}" : "closed by the broker";
                        log.Warn($"[AMQP] {sender?.GetType().Name} for queue '{queueDefinition.DisplayName}' closed: {connectionLostReason}");
                    });
            }
            catch (Exception ex)
            {
                WaitBeforeRetry(ex);
                throw;
            }

            ResetRetryDelay(queueDefinition.Id);
            log.Info($"[AMQP] Thread {threadId} receiving from '{queueDefinition.Address}' for queue '{queueDefinition.DisplayName}'");
        }

        /// <summary>Delays the failing thread (1s, 2s, 4s ... 60s) before it exits and the thread manager replaces it.</summary>
        private void WaitBeforeRetry(Exception ex)
        {
            int failures = ConnectFailures.AddOrUpdate(queueDefinition.Id, 1, (_, n) => n + 1);
            TimeSpan delay = TimeSpan.FromSeconds(Math.Min(MaxRetryDelay.TotalSeconds, Math.Pow(2, Math.Min(failures - 1, 6))));
            log.Warn($"[AMQP] Could not start receiving for queue '{queueDefinition.DisplayName}' (attempt {failures}): " +
                     $"{AmqpConnectionHelper.DescribeException(ex).TrimEnd('.')}. Retrying in {delay.TotalSeconds:0} seconds.");

            DateTime until = DateTime.UtcNow + delay;
            while (!isShuttingDown && DateTime.UtcNow < until)
                Thread.Sleep(500);
        }

        private void Disconnect()
        {
            closing = true;
            AmqpReceiverHandle current = receiver;
            receiver = null;
            current?.Dispose();
        }
    }
}
