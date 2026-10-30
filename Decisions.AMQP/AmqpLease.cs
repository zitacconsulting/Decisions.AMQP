using System;
using System.Linq;
using System.Runtime.Serialization;
using DecisionsFramework;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Actions;
using DecisionsFramework.ServiceLayer.Utilities;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>Database row marking which receiver thread in the cluster owns a "One connection at a time" queue.</summary>
    [DataContract]
    [ORMEntity("amqp_lease")]
    public class AmqpLease : AbstractEntity
    {
        [ORMPrimaryKeyField]
        [PropertyHidden]
        [DataMember]
        public string Id { get; set; }

        [ORMField("lease_owner")]
        [DataMember]
        [WritableValue]
        public string LeaseOwner { get; set; }

        [ORMField("lease_expiration_time")]
        [DataMember]
        [WritableValue]
        public DateTime LeaseExpirationTime { get; set; }

        [PropertyHidden]
        [DataMember]
        public override string EntityName
        {
            get { return $"AMQP Lease: {Id}"; }
            set { }
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityDescription
        {
            get { return $"Owner: {LeaseOwner}, Expires: {LeaseExpirationTime:u}"; }
            set { }
        }

        public AmqpLease() { }

        public AmqpLease(string queueId, string leaseOwner, DateTime leaseExpirationTime)
        {
            Id = AmqpLeaseManager.LeaseId(queueId);
            LeaseOwner = leaseOwner;
            LeaseExpirationTime = leaseExpirationTime;
        }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
            => Array.Empty<BaseActionType>();
    }

    public static class AmqpLeaseManager
    {
        private static readonly Log Log = new Log("AMQP");
        private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(60);

        internal static string LeaseId(string queueId) => $"lease_{queueId}";

        public static bool AcquireLease(string queueId, string threadId)
        {
            var orm = new ORM<AmqpLease>();
            try
            {
                string leaseId = LeaseId(queueId);
                var existing = orm.Fetch(new WhereCondition[]
                {
                    new FieldWhereCondition("id", QueryMatchType.Equals, leaseId)
                }).FirstOrDefault();

                if (existing == null)
                {
                    orm.Store(new AmqpLease(queueId, threadId, DateTime.UtcNow.Add(LeaseDuration)), true);
                    Log.Info($"[AMQP] Acquired new lease for queue {queueId}, thread {threadId}");
                    return true;
                }

                if (existing.LeaseExpirationTime >= DateTime.UtcNow)
                {
                    if (existing.LeaseOwner == threadId)
                    {
                        existing.LeaseExpirationTime = DateTime.UtcNow.Add(LeaseDuration);
                        orm.Store(existing, false, false, "lease_expiration_time");
                        return true;
                    }
                    return false;
                }

                // Lease has expired - take ownership
                try
                {
                    orm.Delete(existing, true);
                    orm.Store(new AmqpLease(queueId, threadId, DateTime.UtcNow.Add(LeaseDuration)), true);
                    Log.Info($"[AMQP] Took over expired lease for queue {queueId}, thread {threadId}");
                    return true;
                }
                catch (Exception ex)
                {
                    // Another node likely acquired the lease concurrently (race on delete+insert)
                    Log.Debug($"[AMQP] Concurrent lease acquisition for queue {queueId}: {ex.Message}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"[AMQP] Error acquiring lease for queue {queueId}, thread {threadId}");
                return false;
            }
        }

        public static void ReleaseLease(string queueId, string threadId)
        {
            var orm = new ORM<AmqpLease>();
            try
            {
                var lease = orm.Fetch(new WhereCondition[]
                {
                    new FieldWhereCondition("id", QueryMatchType.Equals, LeaseId(queueId)),
                    new FieldWhereCondition("lease_owner", QueryMatchType.Equals, threadId)
                }).FirstOrDefault();

                if (lease != null)
                {
                    lease.LeaseOwner = null;
                    lease.LeaseExpirationTime = DateTime.UtcNow;
                    orm.Store(lease, false, false, "lease_owner", "lease_expiration_time");
                    Log.Info($"[AMQP] Released lease for queue {queueId}, thread {threadId}");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"[AMQP] Error releasing lease for queue {queueId}, thread {threadId}");
            }
        }
    }
}
