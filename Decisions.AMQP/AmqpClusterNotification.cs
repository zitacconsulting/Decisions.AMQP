using DecisionsFramework.Data.ORMapper;
using Decisions.MessageQueues;

namespace Decisions.AmqpMessageQueue
{
    public class AmqpClusterNotification : BaseMqClusterNotification
    {
        public override BaseMqDefinition GetQueueDefinition(string queueEntityId)
        {
            return new ORM<AmqpMessageQueue>().Fetch(queueEntityId);
        }
    }
}
