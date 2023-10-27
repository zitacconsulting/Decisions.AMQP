using System.Runtime.Serialization;
using Amqp;
using Amqp.Framing;
using DecisionsFramework.ServiceLayer.Services.ContextData;

namespace Zitac.AMQP.Steps;

[DataContract]
public class AMQPResult
{

    [DataMember]
    public string? Body { get; set; }

    [DataMember]
    public string? ContentType {get; set; }

    [DataMember]
    public DataPair[]? ApplicationProperties { get; set; }

    public AMQPResult(){}
    public AMQPResult(Message message)
    {
        this.Body = message.Body.ToString();
        this.ContentType = message.Properties.ContentType;

        var dataPairs = new List<DataPair>();
        foreach (var property in message.ApplicationProperties.Map) {
            dataPairs.Add(new DataPair(property.Key.ToString(), property.Value));
        }
        this.ApplicationProperties = dataPairs.ToArray();
    }

}