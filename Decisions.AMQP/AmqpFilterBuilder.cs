using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Amqp.Types;

namespace Decisions.AmqpMessageQueue
{
    /// <summary>
    /// Turns a queue's filter configuration into the AMQP filter set sent on the receiver's source.
    ///
    /// Filter keys matter: RabbitMQ only recognizes the SQL filter under the key "sql-filter" and a
    /// JMS selector under "jms-selector" (the name Qpid JMS uses, which Artemis/Classic/Qpid also accept).
    /// Descriptors: OASIS AMQP Filter Expressions 1.0 and the Apache filter registry.
    /// </summary>
    public static class AmqpFilterBuilder
    {
        public const string SELECTOR_KEY = "jms-selector";
        public const string SELECTOR_DESCRIPTOR = "apache.org:selector-filter:string";
        public const string SQL_KEY = "sql-filter";
        public const string SQL_DESCRIPTOR = "amqp:sql-filter";
        public const string STREAM_OFFSET = "rabbitmq:stream-offset-spec";
        public const string STREAM_FILTER = "rabbitmq:stream-filter";
        public const string STREAM_MATCH_UNFILTERED = "rabbitmq:stream-match-unfiltered";

        private static readonly Regex JmsIdentifier = new Regex(@"^[A-Za-z_$][A-Za-z0-9_$]*$");
        private static readonly Regex SqlIdentifier = new Regex(@"^[A-Za-z_][A-Za-z0-9_]*$");
        private static readonly Regex StreamInterval = new Regex(@"^\d+[YMDhms]$");

        private static readonly HashSet<string> ReservedWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "ESCAPE", "TRUE", "FALSE", "BETWEEN", "UTC"
        };

        /// <summary>
        /// The filter set for a receiver on this queue, or null for no filter.
        /// <paramref name="expressionOverride"/> (used by the Receive step) replaces the queue's own rules/expression.
        /// Throws <see cref="InvalidOperationException"/> if the configuration is invalid.
        /// </summary>
        public static Map BuildFilterSet(AmqpMessageQueue queue, string expressionOverride = null)
        {
            var errors = new List<string>();
            Map map = Build(queue, expressionOverride, errors);
            if (errors.Count > 0)
                throw new InvalidOperationException($"AMQP queue '{queue.DisplayName}' has an invalid filter: {string.Join(" ", errors)}");
            return map;
        }

        public static string[] Validate(AmqpMessageQueue queue)
        {
            var errors = new List<string>();
            Build(queue, null, errors);
            return errors.ToArray();
        }

        /// <summary>Human readable description of what is sent to the broker (shown on the queue as "Generated Filter").</summary>
        public static string Describe(AmqpMessageQueue queue)
        {
            if (queue.IsServiceBus)
                return "Not supported by Azure Service Bus.";

            var errors = new List<string>();
            Map map;
            try
            {
                map = Build(queue, null, errors);
            }
            catch (Exception ex)
            {
                return $"Invalid filter: {ex.Message}";
            }

            if (errors.Count > 0)
                return "Invalid filter: " + string.Join(" ", errors);
            if (map == null || map.Count == 0)
                return "No filter: every message on the address is received.";

            var sb = new StringBuilder();
            foreach (var entry in map)
            {
                sb.Append(entry.Key);
                if (entry.Value is DescribedValue described)
                    sb.Append(" [").Append(described.Descriptor).Append("]: ").Append(FormatValue(described.Value));
                else
                    sb.Append(": ").Append(FormatValue(entry.Value));
                sb.AppendLine();
            }
            return sb.ToString().TrimEnd();
        }

        /// <summary>The filter keys of the set, used to check that the broker confirmed them on attach.</summary>
        public static string[] GetKeys(Map filterSet)
            => filterSet == null ? Array.Empty<string>() : filterSet.Keys.Select(k => k?.ToString()).ToArray();

        /// <summary>The selector/SQL expression the queue's rules or expression produce, or null.</summary>
        public static string GetExpression(AmqpMessageQueue queue, List<string> errors)
        {
            if (queue.FilterType == AmqpOptions.FILTER_RULES)
                return CompileRules(queue.FilterRules, queue.RuleMatchMode == AmqpOptions.MATCH_ANY, queue.BrokerType, errors);
            if (queue.FilterType == AmqpOptions.FILTER_EXPRESSION)
            {
                if (string.IsNullOrWhiteSpace(queue.FilterExpression))
                    errors.Add("Filter Expression must be supplied.");
                return queue.FilterExpression?.Trim();
            }
            return null;
        }

        /// <summary>Adds a selector or SQL filter (depending on the broker) for <paramref name="expression"/>.</summary>
        public static void AddExpression(Map map, string brokerType, string expression)
        {
            if (AmqpOptions.IsRabbitMq(brokerType))
                map[new Symbol(SQL_KEY)] = new DescribedValue(new Symbol(SQL_DESCRIPTOR), expression);
            else
                map[new Symbol(SELECTOR_KEY)] = new DescribedValue(new Symbol(SELECTOR_DESCRIPTOR), expression);
        }

        /// <summary>A filter expression matching one correlation ID, in the broker's syntax (used for request/reply).</summary>
        public static string CorrelationIdExpression(string brokerType, string correlationId)
        {
            string field = AmqpOptions.IsRabbitMq(brokerType) ? "p.correlation_id" : "JMSCorrelationID";
            return $"{field} = {Quote(correlationId)}";
        }

        private static Map Build(AmqpMessageQueue queue, string expressionOverride, List<string> errors)
        {
            if (queue.IsServiceBus)
                return null;

            var map = new Map();

            string expression = expressionOverride;
            if (string.IsNullOrWhiteSpace(expression))
                expression = GetExpression(queue, errors);
            if (!string.IsNullOrWhiteSpace(expression))
                AddExpression(map, queue.BrokerType, expression);

            if (queue.FilterType == AmqpOptions.FILTER_CUSTOM)
                AddCustomFilters(map, queue.CustomFilters, errors);

            if (queue.IsRabbitMq)
                AddStreamFilters(map, queue, errors);

            return map.Count == 0 ? null : map;
        }

        // ---------------- Simple rules ----------------

        private static string CompileRules(AmqpFilterRule[] rules, bool matchAny, string brokerType, List<string> errors)
        {
            if (rules == null || rules.Length == 0)
            {
                errors.Add("Add at least one filter rule, or set Filter Type to None.");
                return null;
            }

            bool sql = AmqpOptions.IsRabbitMq(brokerType);
            var parts = new List<string>();
            for (int i = 0; i < rules.Length; i++)
            {
                string part = CompileRule(rules[i], sql, $"Rule {i + 1}", errors);
                if (part != null)
                    parts.Add(rules.Length > 1 ? $"({part})" : part);
            }
            return parts.Count == 0 ? null : string.Join(matchAny ? " OR " : " AND ", parts);
        }

        private static string CompileRule(AmqpFilterRule rule, bool sql, string label, List<string> errors)
        {
            string field = GetRuleField(rule, sql, label, errors);
            if (field == null)
                return null;

            string op = rule.Operator ?? AmqpFilterRule.OP_EQUALS;
            string valueType = rule.Field == AmqpFilterRule.FIELD_PRIORITY ? AmqpFilterRule.TYPE_NUMBER : (rule.ValueType ?? AmqpFilterRule.TYPE_TEXT);

            switch (op)
            {
                case AmqpFilterRule.OP_EXISTS:
                    return $"{field} IS NOT NULL";
                case AmqpFilterRule.OP_NOT_EXISTS:
                    return $"{field} IS NULL";
                case AmqpFilterRule.OP_STARTS_WITH:
                case AmqpFilterRule.OP_ENDS_WITH:
                case AmqpFilterRule.OP_CONTAINS:
                {
                    if (valueType != AmqpFilterRule.TYPE_TEXT)
                    {
                        errors.Add($"{label}: '{op}' only works with Value Type Text.");
                        return null;
                    }
                    if (string.IsNullOrEmpty(rule.Value))
                    {
                        errors.Add($"{label}: a value is required.");
                        return null;
                    }
                    bool escaped = rule.Value.IndexOfAny(new[] { '%', '_', '\\' }) >= 0;
                    string pattern = rule.Value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");
                    pattern = op == AmqpFilterRule.OP_STARTS_WITH ? pattern + "%"
                        : op == AmqpFilterRule.OP_ENDS_WITH ? "%" + pattern
                        : "%" + pattern + "%";
                    return $"{field} LIKE {Quote(pattern)}" + (escaped ? " ESCAPE '\\'" : "");
                }
                case AmqpFilterRule.OP_IN:
                case AmqpFilterRule.OP_NOT_IN:
                {
                    string[] values = (rule.Value ?? "").Split(',').Select(v => v.Trim()).Where(v => v.Length > 0).ToArray();
                    if (values.Length == 0)
                    {
                        errors.Add($"{label}: enter one or more comma separated values.");
                        return null;
                    }
                    var literals = new List<string>();
                    foreach (string v in values)
                    {
                        string literal = Literal(v, valueType, label, errors);
                        if (literal == null)
                            return null;
                        literals.Add(literal);
                    }
                    return $"{field} {(op == AmqpFilterRule.OP_IN ? "IN" : "NOT IN")} ({string.Join(", ", literals)})";
                }
                default:
                {
                    string sqlOp = op switch
                    {
                        AmqpFilterRule.OP_NOT_EQUALS => "<>",
                        AmqpFilterRule.OP_GREATER => ">",
                        AmqpFilterRule.OP_GREATER_OR_EQUAL => ">=",
                        AmqpFilterRule.OP_LESS => "<",
                        AmqpFilterRule.OP_LESS_OR_EQUAL => "<=",
                        _ => "="
                    };
                    if (sqlOp != "=" && sqlOp != "<>" && valueType == AmqpFilterRule.TYPE_BOOLEAN)
                    {
                        errors.Add($"{label}: '{op}' can't be used with True/False values.");
                        return null;
                    }
                    if (rule.Value == null && valueType == AmqpFilterRule.TYPE_TEXT && sqlOp == "=")
                    {
                        errors.Add($"{label}: a value is required (use 'Does Not Exist' to match a missing property).");
                        return null;
                    }
                    string literal = Literal(rule.Value ?? "", valueType, label, errors);
                    return literal == null ? null : $"{field} {sqlOp} {literal}";
                }
            }
        }

        private static string GetRuleField(AmqpFilterRule rule, bool sql, string label, List<string> errors)
        {
            switch (rule.Field ?? AmqpFilterRule.FIELD_APPLICATION_PROPERTY)
            {
                case AmqpFilterRule.FIELD_CORRELATION_ID:
                    return sql ? "p.correlation_id" : "JMSCorrelationID";
                case AmqpFilterRule.FIELD_MESSAGE_ID:
                    return sql ? "p.message_id" : "JMSMessageID";
                case AmqpFilterRule.FIELD_PRIORITY:
                    return sql ? "h.priority" : "JMSPriority";
                case AmqpFilterRule.FIELD_SUBJECT:
                    if (sql) return "p.subject";
                    errors.Add($"{label}: filtering on Subject is only supported for RabbitMQ. Use an application property instead.");
                    return null;
                case AmqpFilterRule.FIELD_CONTENT_TYPE:
                    if (sql) return "p.content_type";
                    errors.Add($"{label}: filtering on Content Type is only supported for RabbitMQ. Use an application property instead.");
                    return null;
                default:
                {
                    string name = rule.PropertyName?.Trim();
                    if (string.IsNullOrEmpty(name))
                    {
                        errors.Add($"{label}: Property Name is required.");
                        return null;
                    }
                    if (sql)
                    {
                        if (SqlIdentifier.IsMatch(name) && !ReservedWords.Contains(name))
                            return "a." + name;
                        if (name.Contains(']'))
                        {
                            errors.Add($"{label}: property name '{name}' can't contain ']'.");
                            return null;
                        }
                        return $"a.[{name}]";
                    }
                    if (!JmsIdentifier.IsMatch(name) || ReservedWords.Contains(name))
                    {
                        errors.Add($"{label}: '{name}' can't be used in a JMS selector. Property names must start with a letter, '_' or '$', " +
                                   "contain only letters, digits, '_' and '$', and must not be a reserved word.");
                        return null;
                    }
                    return name;
                }
            }
        }

        private static string Literal(string value, string valueType, string label, List<string> errors)
        {
            switch (valueType)
            {
                case AmqpFilterRule.TYPE_NUMBER:
                    if (decimal.TryParse(value.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number))
                        return number.ToString(CultureInfo.InvariantCulture);
                    errors.Add($"{label}: '{value}' is not a number (use '.' as decimal separator).");
                    return null;
                case AmqpFilterRule.TYPE_BOOLEAN:
                    if (bool.TryParse(value.Trim(), out bool b))
                        return b ? "TRUE" : "FALSE";
                    errors.Add($"{label}: '{value}' is not true or false.");
                    return null;
                default:
                    return Quote(value);
            }
        }

        private static string Quote(string value) => "'" + (value ?? "").Replace("'", "''") + "'";

        // ---------------- Custom filters ----------------

        private static void AddCustomFilters(Map map, AmqpCustomFilter[] filters, List<string> errors)
        {
            if (filters == null || filters.Length == 0)
            {
                errors.Add("Add at least one custom filter, or set Filter Type to None.");
                return;
            }

            for (int i = 0; i < filters.Length; i++)
            {
                AmqpCustomFilter filter = filters[i];
                string label = $"Custom filter {i + 1}";
                if (string.IsNullOrWhiteSpace(filter.Key))
                {
                    errors.Add($"{label}: Filter Key is required.");
                    continue;
                }

                object value = ParseCustomValue(filter, label, errors);
                if (value == null)
                    continue;

                object descriptor = ParseDescriptor(filter.Descriptor, label, errors);
                map[new Symbol(filter.Key.Trim())] = descriptor == null ? value : new DescribedValue(descriptor, value);
            }
        }

        private static object ParseDescriptor(string descriptor, string label, List<string> errors)
        {
            if (string.IsNullOrWhiteSpace(descriptor))
                return null;
            descriptor = descriptor.Trim();
            if (descriptor.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            {
                string hex = descriptor.Substring(2).Replace(":0x", "").Replace(":", "");
                if (ulong.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out ulong code))
                    return code;
                errors.Add($"{label}: '{descriptor}' is not a valid numeric descriptor.");
                return null;
            }
            return new Symbol(descriptor);
        }

        private static object ParseCustomValue(AmqpCustomFilter filter, string label, List<string> errors)
        {
            string value = filter.Value ?? "";
            switch (filter.ValueType)
            {
                case AmqpCustomFilter.VALUE_SYMBOL:
                    return new Symbol(value);
                case AmqpCustomFilter.VALUE_BOOLEAN:
                    if (bool.TryParse(value.Trim(), out bool b)) return b;
                    errors.Add($"{label}: '{value}' is not true or false.");
                    return null;
                case AmqpCustomFilter.VALUE_LONG:
                    if (long.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out long l)) return l;
                    errors.Add($"{label}: '{value}' is not a whole number.");
                    return null;
                case AmqpCustomFilter.VALUE_TIMESTAMP:
                    if (TryParseUtc(value, out DateTime ts)) return ts;
                    errors.Add($"{label}: '{value}' is not a valid timestamp (use ISO 8601, e.g. 2026-01-31T12:00:00Z).");
                    return null;
                case AmqpCustomFilter.VALUE_STRING_LIST:
                {
                    var list = new List();
                    foreach (string item in value.Split(',').Select(s => s.Trim()).Where(s => s.Length > 0))
                        list.Add(item);
                    return list;
                }
                case AmqpCustomFilter.VALUE_MAP:
                case AmqpCustomFilter.VALUE_SYMBOL_MAP:
                {
                    var map = new Map();
                    foreach (string pair in value.Split(new[] { ';', '\n' }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        int eq = pair.IndexOf('=');
                        if (eq <= 0)
                        {
                            errors.Add($"{label}: '{pair.Trim()}' is not in the format key=value.");
                            return null;
                        }
                        string k = pair.Substring(0, eq).Trim();
                        string v = pair.Substring(eq + 1).Trim();
                        if (filter.ValueType == AmqpCustomFilter.VALUE_SYMBOL_MAP)
                            map[new Symbol(k)] = v;
                        else
                            map[k] = v;
                    }
                    return map;
                }
                default:
                    return value;
            }
        }

        // ---------------- RabbitMQ streams ----------------

        private static void AddStreamFilters(Map map, AmqpMessageQueue queue, List<string> errors)
        {
            string value = queue.StreamStartValue?.Trim();
            object offset = null;
            switch (queue.StreamStartPosition)
            {
                case AmqpOptions.STREAM_FIRST: offset = "first"; break;
                case AmqpOptions.STREAM_LAST: offset = "last"; break;
                case AmqpOptions.STREAM_NEXT: offset = "next"; break;
                case AmqpOptions.STREAM_OFFSET:
                    if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long o) && o >= 0)
                        offset = o;
                    else
                        errors.Add("Stream Start Position Value must be an offset (a whole number, 0 or higher).");
                    break;
                case AmqpOptions.STREAM_TIMESTAMP:
                    if (TryParseUtc(value, out DateTime ts))
                        offset = ts;
                    else
                        errors.Add("Stream Start Position Value must be a UTC timestamp, e.g. 2026-01-31T12:00:00Z.");
                    break;
                case AmqpOptions.STREAM_INTERVAL:
                    if (!string.IsNullOrEmpty(value) && StreamInterval.IsMatch(value))
                        offset = value;
                    else
                        errors.Add("Stream Start Position Value must be an age such as 30s, 15m, 2h, 7D, 1M or 1Y.");
                    break;
            }
            if (offset != null)
                map[new Symbol(STREAM_OFFSET)] = new DescribedValue(new Symbol(STREAM_OFFSET), offset);

            string[] bloom = (queue.StreamFilterValues ?? "").Split(',').Select(s => s.Trim()).Where(s => s.Length > 0).ToArray();
            if (bloom.Length == 1)
            {
                map[new Symbol(STREAM_FILTER)] = new DescribedValue(new Symbol(STREAM_FILTER), bloom[0]);
            }
            else if (bloom.Length > 1)
            {
                var list = new List();
                list.AddRange(bloom);
                map[new Symbol(STREAM_FILTER)] = new DescribedValue(new Symbol(STREAM_FILTER), list);
            }

            if (bloom.Length > 0 && queue.StreamMatchUnfiltered)
                map[new Symbol(STREAM_MATCH_UNFILTERED)] = new DescribedValue(new Symbol(STREAM_MATCH_UNFILTERED), true);
        }

        private static bool TryParseUtc(string value, out DateTime result)
        {
            bool ok = DateTime.TryParse(value?.Trim(), CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out result);
            if (ok)
                result = DateTime.SpecifyKind(result, DateTimeKind.Utc);
            return ok;
        }

        private static string FormatValue(object value)
        {
            switch (value)
            {
                case null: return "null";
                case string s: return s;
                case DateTime dt: return dt.ToString("o", CultureInfo.InvariantCulture);
                case List list: return "[" + string.Join(", ", list.Select(FormatValue)) + "]";
                case Map map: return "{" + string.Join(", ", map.Select(e => $"{e.Key}={FormatValue(e.Value)}")) + "}";
                default: return Convert.ToString(value, CultureInfo.InvariantCulture);
            }
        }
    }
}
