using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using DecisionsFramework.ServiceLayer.Utilities;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>A received AMQP message, as returned by the Receive and Send (request/reply) steps.</summary>
    [AutoRegisterNativeType]
    [DataContract]
    [Writable]
    public class AmqpReceivedMessage
    {
        [DataMember] [WritableValue] public string Body { get; set; }
        [DataMember] [WritableValue] public string MessageId { get; set; }
        [DataMember] [WritableValue] public string CorrelationId { get; set; }
        [DataMember] [WritableValue] public string Subject { get; set; }
        [DataMember] [WritableValue] public string ContentType { get; set; }
        [DataMember] [WritableValue] public string ContentEncoding { get; set; }
        [DataMember] [WritableValue] public string ReplyTo { get; set; }
        [DataMember] [WritableValue] public string To { get; set; }
        [DataMember] [WritableValue] public string UserId { get; set; }
        [DataMember] [WritableValue] public string GroupId { get; set; }
        [DataMember] [WritableValue] public DateTime? CreationTime { get; set; }
        [DataMember] [WritableValue] public DateTime? AbsoluteExpiryTime { get; set; }
        [DataMember] [WritableValue] public bool Durable { get; set; }
        [DataMember] [WritableValue] public int Priority { get; set; }
        [DataMember] [WritableValue] public long? TimeToLiveMs { get; set; }
        [DataMember] [WritableValue] public int DeliveryCount { get; set; }
        [DataMember] [WritableValue] public DataPair[] ApplicationProperties { get; set; }
        [DataMember] [WritableValue] public DataPair[] MessageAnnotations { get; set; }
    }

    /// <summary>Values to put on an outgoing message.</summary>
    public class AmqpOutgoingMessage
    {
        public string Body { get; set; }
        public bool BinaryBody { get; set; }
        public string MessageId { get; set; }
        public string CorrelationId { get; set; }
        public string Subject { get; set; }
        public string ContentType { get; set; }
        public string ReplyTo { get; set; }
        public string GroupId { get; set; }
        public bool Durable { get; set; } = true;
        public int? Priority { get; set; }
        public long? TimeToLiveMs { get; set; }
        public DataPair[] ApplicationProperties { get; set; }
        public DataPair[] MessageAnnotations { get; set; }
    }

    public static class AmqpMessageConverter
    {
        public static bool UseBinaryBody(string bodyFormat, string brokerType)
        {
            if (bodyFormat == AmqpOptions.BODY_BINARY) return true;
            if (bodyFormat == AmqpOptions.BODY_TEXT) return false;
            // RabbitMQ (and its AMQP 0.9.1 consumers) and the Azure SDKs expect the body in a data section.
            // JMS brokers turn an AMQP value string into a TextMessage.
            return AmqpOptions.IsRabbitMq(brokerType) || AmqpOptions.IsServiceBus(brokerType);
        }

        public static Message Build(AmqpOutgoingMessage outgoing)
        {
            Message message;
            if (outgoing.BinaryBody)
                message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(outgoing.Body ?? string.Empty) } };
            else
                message = new Message(outgoing.Body ?? string.Empty);

            message.Header = new Header { Durable = outgoing.Durable };
            if (outgoing.Priority.HasValue)
                message.Header.Priority = (byte)Math.Clamp(outgoing.Priority.Value, 0, 255);
            if (outgoing.TimeToLiveMs.HasValue && outgoing.TimeToLiveMs.Value > 0)
                message.Header.Ttl = (uint)Math.Min(outgoing.TimeToLiveMs.Value, uint.MaxValue);

            message.Properties = new Properties
            {
                MessageId = string.IsNullOrEmpty(outgoing.MessageId) ? Guid.NewGuid().ToString() : outgoing.MessageId,
                CreationTime = DateTime.UtcNow
            };
            if (!string.IsNullOrEmpty(outgoing.CorrelationId)) message.Properties.CorrelationId = outgoing.CorrelationId;
            if (!string.IsNullOrEmpty(outgoing.Subject)) message.Properties.Subject = outgoing.Subject;
            if (!string.IsNullOrEmpty(outgoing.ContentType)) message.Properties.ContentType = new Symbol(outgoing.ContentType);
            if (!string.IsNullOrEmpty(outgoing.ReplyTo)) message.Properties.ReplyTo = outgoing.ReplyTo;
            if (!string.IsNullOrEmpty(outgoing.GroupId)) message.Properties.GroupId = outgoing.GroupId;

            if (outgoing.ApplicationProperties != null && outgoing.ApplicationProperties.Length > 0)
            {
                message.ApplicationProperties = new ApplicationProperties();
                foreach (DataPair pair in outgoing.ApplicationProperties.Where(p => !string.IsNullOrEmpty(p?.Name)))
                    message.ApplicationProperties[pair.Name] = ToAmqpValue(pair.OutputValue);
            }

            if (outgoing.MessageAnnotations != null && outgoing.MessageAnnotations.Length > 0)
            {
                message.MessageAnnotations = new MessageAnnotations();
                foreach (DataPair pair in outgoing.MessageAnnotations.Where(p => !string.IsNullOrEmpty(p?.Name)))
                    message.MessageAnnotations[new Symbol(pair.Name)] = ToAmqpValue(pair.OutputValue);
            }

            return message;
        }

        /// <summary>AMQP can't encode every .NET type a flow may supply (e.g. decimal); convert those.</summary>
        private static object ToAmqpValue(object value)
        {
            switch (value)
            {
                case null:
                case string _:
                case bool _:
                case byte _:
                case sbyte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                case float _:
                case double _:
                case Guid _:
                case byte[] _:
                    return value;
                case DateTime dt:
                    return dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                case decimal d:
                    return (double)d;
                default:
                    return Convert.ToString(value, CultureInfo.InvariantCulture);
            }
        }

        // ---------------- Receiving ----------------

        public static void DecodeBody(Message message, out byte[] bytes, out string text)
        {
            object body = message.BodySection == null ? null : message.Body;
            switch (body)
            {
                case null:
                    bytes = Array.Empty<byte>();
                    text = string.Empty;
                    break;
                case byte[] b:
                    bytes = b;
                    text = Encoding.UTF8.GetString(b);
                    break;
                case string s:
                    text = s;
                    bytes = Encoding.UTF8.GetBytes(s);
                    break;
                default:
                    text = JsonSerializer.Serialize(ToPlain(body));
                    bytes = Encoding.UTF8.GetBytes(text);
                    break;
            }
        }

        public static AmqpReceivedMessage ToReceivedMessage(Message message)
        {
            DecodeBody(message, out _, out string text);
            Properties p = message.Properties;
            Header h = message.Header;
            return new AmqpReceivedMessage
            {
                Body = text,
                MessageId = IdToString(p?.GetMessageId()),
                CorrelationId = IdToString(p?.GetCorrelationId()),
                Subject = p?.Subject,
                ContentType = p?.ContentType?.ToString(),
                ContentEncoding = p?.ContentEncoding?.ToString(),
                ReplyTo = p?.ReplyTo,
                To = p?.To,
                UserId = p?.UserId == null ? null : Encoding.UTF8.GetString(p.UserId),
                GroupId = p?.GroupId,
                CreationTime = p == null || p.CreationTime == default ? null : p.CreationTime,
                AbsoluteExpiryTime = p == null || p.AbsoluteExpiryTime == default ? null : p.AbsoluteExpiryTime,
                Durable = h?.Durable ?? false,
                Priority = h?.Priority ?? 4,
                TimeToLiveMs = h == null || h.Ttl == 0 ? null : h.Ttl,
                DeliveryCount = (int)(h?.DeliveryCount ?? 0),
                ApplicationProperties = GetApplicationProperties(message),
                MessageAnnotations = GetMessageAnnotations(message)
            };
        }

        /// <summary>Application properties, passed to the handler flow as "Headers".</summary>
        public static DataPair[] GetApplicationProperties(Message message)
            => message.ApplicationProperties?.Map == null
                ? Array.Empty<DataPair>()
                : message.ApplicationProperties.Map.Select(e => new DataPair(e.Key?.ToString(), ToFlowValue(e.Value))).ToArray();

        public static DataPair[] GetMessageAnnotations(Message message)
            => message.MessageAnnotations?.Map == null
                ? Array.Empty<DataPair>()
                : message.MessageAnnotations.Map.Select(e => new DataPair(e.Key?.ToString(), ToFlowValue(e.Value))).ToArray();

        /// <summary>Standard AMQP properties and header fields, passed to the handler flow as "Other Data".</summary>
        public static List<DataPair> GetStandardFields(Message message, string address)
        {
            AmqpReceivedMessage m = ToReceivedMessage(message);
            var data = new List<DataPair>
            {
                new DataPair("Address", address),
                new DataPair("MessageId", m.MessageId),
                new DataPair("CorrelationId", m.CorrelationId),
                new DataPair("Subject", m.Subject),
                new DataPair("ContentType", m.ContentType),
                new DataPair("ContentEncoding", m.ContentEncoding),
                new DataPair("ReplyTo", m.ReplyTo),
                new DataPair("To", m.To),
                new DataPair("UserId", m.UserId),
                new DataPair("GroupId", m.GroupId),
                new DataPair("CreationTime", m.CreationTime),
                new DataPair("AbsoluteExpiryTime", m.AbsoluteExpiryTime),
                new DataPair("Durable", m.Durable),
                new DataPair("Priority", m.Priority),
                new DataPair("TimeToLiveMs", m.TimeToLiveMs),
                new DataPair("DeliveryCount", m.DeliveryCount)
            };
            foreach (DataPair annotation in m.MessageAnnotations)
                data.Add(new DataPair("Annotation." + annotation.Name, annotation.OutputValue));
            return data;
        }

        /// <summary>The routing key: RabbitMQ's x-routing-key annotation, otherwise the AMQP subject.</summary>
        public static string GetRoutingKey(Message message)
        {
            object routingKey = message.MessageAnnotations?.Map?
                .FirstOrDefault(e => e.Key?.ToString() == "x-routing-key").Value;
            return routingKey?.ToString() ?? message.Properties?.Subject;
        }

        public static string IdToString(object id)
        {
            switch (id)
            {
                case null: return null;
                case byte[] b: return Convert.ToBase64String(b);
                default: return Convert.ToString(id, CultureInfo.InvariantCulture);
            }
        }

        private static object ToFlowValue(object value)
        {
            object plain = ToPlain(value);
            return plain is Dictionary<string, object> || plain is List<object> ? JsonSerializer.Serialize(plain) : plain;
        }

        /// <summary>Converts AMQP types to plain .NET values (maps, lists, strings, numbers).</summary>
        private static object ToPlain(object value)
        {
            switch (value)
            {
                case null: return null;
                case Symbol s: return s.ToString();
                case DescribedValue d: return ToPlain(d.Value);
                case byte[] b: return Convert.ToBase64String(b);
                case Guid g: return g.ToString();
                case Map map:
                    return map.ToDictionary(e => e.Key?.ToString() ?? "", e => ToPlain(e.Value));
                case string _:
                    return value;
                case System.Collections.IEnumerable list:
                    return list.Cast<object>().Select(ToPlain).ToList();
                default:
                    return value;
            }
        }
    }
}
