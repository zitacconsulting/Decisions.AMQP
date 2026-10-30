using System;
using System.Runtime.Serialization;
using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Actions;
using DecisionsFramework.ServiceLayer.Utilities;
using DecisionsFramework.Utilities;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// One row of the queue's "Simple Rules" filter. The rules are compiled into a JMS selector or a
    /// RabbitMQ SQL filter expression by <see cref="AmqpFilterBuilder"/>.
    /// </summary>
    [ORMEntity("amqp_filter_rule")]
    [DataContract]
    public class AmqpFilterRule : AbstractEntity
    {
        public const string FIELD_APPLICATION_PROPERTY = "Application Property";
        public const string FIELD_SUBJECT = "Subject";
        public const string FIELD_CORRELATION_ID = "Correlation ID";
        public const string FIELD_MESSAGE_ID = "Message ID";
        public const string FIELD_CONTENT_TYPE = "Content Type";
        public const string FIELD_PRIORITY = "Priority";

        public const string OP_EQUALS = "Equals";
        public const string OP_NOT_EQUALS = "Not Equals";
        public const string OP_GREATER = "Greater Than";
        public const string OP_GREATER_OR_EQUAL = "Greater Than or Equal";
        public const string OP_LESS = "Less Than";
        public const string OP_LESS_OR_EQUAL = "Less Than or Equal";
        public const string OP_STARTS_WITH = "Starts With";
        public const string OP_ENDS_WITH = "Ends With";
        public const string OP_CONTAINS = "Contains";
        public const string OP_IN = "Is One Of (comma separated)";
        public const string OP_NOT_IN = "Is Not One Of (comma separated)";
        public const string OP_EXISTS = "Exists";
        public const string OP_NOT_EXISTS = "Does Not Exist";

        public const string TYPE_TEXT = "Text";
        public const string TYPE_NUMBER = "Number";
        public const string TYPE_BOOLEAN = "True/False";

        [ORMPrimaryKeyField]
        [PropertyHidden]
        [DataMember]
        public string Id { get; set; }

        [ORMField("field")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(1, "Field", "Rule")]
        [SelectStringEditor(nameof(Fields))]
        public string Field { get; set; } = FIELD_APPLICATION_PROPERTY;

        [ORMField("property_name")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(2, "Property Name", "Rule")]
        public string PropertyName { get; set; }

        [ORMField("operator")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(3, "Operator", "Rule")]
        [SelectStringEditor(nameof(Operators))]
        public string Operator { get; set; } = OP_EQUALS;

        [ORMField("value_type")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(4, "Value Type", "Rule")]
        [SelectStringEditor(nameof(ValueTypes))]
        public string ValueType { get; set; } = TYPE_TEXT;

        [ORMField("value", 4000, typeof(FixedLengthStringFieldConverter))]
        [DataMember]
        [WritableValue]
        [PropertyClassification(5, "Value", "Rule")]
        public string Value { get; set; }

        [PropertyHidden]
        public string[] Fields => new[]
        {
            FIELD_APPLICATION_PROPERTY, FIELD_SUBJECT, FIELD_CORRELATION_ID, FIELD_MESSAGE_ID, FIELD_CONTENT_TYPE, FIELD_PRIORITY
        };

        [PropertyHidden]
        public string[] Operators => new[]
        {
            OP_EQUALS, OP_NOT_EQUALS, OP_GREATER, OP_GREATER_OR_EQUAL, OP_LESS, OP_LESS_OR_EQUAL,
            OP_STARTS_WITH, OP_ENDS_WITH, OP_CONTAINS, OP_IN, OP_NOT_IN, OP_EXISTS, OP_NOT_EXISTS
        };

        [PropertyHidden]
        public string[] ValueTypes => new[] { TYPE_TEXT, TYPE_NUMBER, TYPE_BOOLEAN };

        [PropertyHidden]
        public bool NeedsValue => Operator != OP_EXISTS && Operator != OP_NOT_EXISTS;

        [PropertyHidden]
        [DataMember]
        public override string EntityName
        {
            get
            {
                string name = Field == FIELD_APPLICATION_PROPERTY ? PropertyName : Field;
                return NeedsValue ? $"{name} {Operator} {Value}" : $"{name} {Operator}";
            }
            set { }
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityDescription
        {
            get { return $"Filter rule: {EntityName}"; }
            set { }
        }

        public AmqpFilterRule()
        {
            Id = IDUtility.GetNewIdString();
        }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
            => Array.Empty<BaseActionType>();
    }

    /// <summary>
    /// One raw entry of the AMQP filter set (Source.filter), for broker-specific filters.
    /// </summary>
    [ORMEntity("amqp_custom_filter")]
    [DataContract]
    public class AmqpCustomFilter : AbstractEntity
    {
        public const string VALUE_STRING = "String";
        public const string VALUE_SYMBOL = "Symbol";
        public const string VALUE_BOOLEAN = "Boolean";
        public const string VALUE_LONG = "Long";
        public const string VALUE_TIMESTAMP = "Timestamp (UTC, ISO 8601)";
        public const string VALUE_STRING_LIST = "List of Strings (comma separated)";
        public const string VALUE_MAP = "Map, string keys (key=value; key=value)";
        public const string VALUE_SYMBOL_MAP = "Map, symbol keys (key=value; key=value)";

        [ORMPrimaryKeyField]
        [PropertyHidden]
        [DataMember]
        public string Id { get; set; }

        [ORMField("filter_key")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(1, "Filter Key", "Filter")]
        public string Key { get; set; }

        [ORMField("descriptor")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(2, "Descriptor", "Filter")]
        public string Descriptor { get; set; }

        [ORMField("value_type")]
        [DataMember]
        [WritableValue]
        [PropertyClassification(3, "Value Type", "Filter")]
        [SelectStringEditor(nameof(ValueTypes))]
        public string ValueType { get; set; } = VALUE_STRING;

        [ORMField("value", 4000, typeof(FixedLengthStringFieldConverter))]
        [DataMember]
        [WritableValue]
        [PropertyClassification(4, "Value", "Filter")]
        public string Value { get; set; }

        [PropertyHidden]
        public string[] ValueTypes => new[]
        {
            VALUE_STRING, VALUE_SYMBOL, VALUE_BOOLEAN, VALUE_LONG, VALUE_TIMESTAMP, VALUE_STRING_LIST, VALUE_MAP, VALUE_SYMBOL_MAP
        };

        [PropertyHidden]
        [DataMember]
        public override string EntityName
        {
            get { return $"{Key}: {Descriptor} = {Value}"; }
            set { }
        }

        [PropertyHidden]
        [DataMember]
        public override string EntityDescription
        {
            get { return $"Custom AMQP filter {EntityName}"; }
            set { }
        }

        public AmqpCustomFilter()
        {
            Id = IDUtility.GetNewIdString();
        }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
            => Array.Empty<BaseActionType>();
    }
}
