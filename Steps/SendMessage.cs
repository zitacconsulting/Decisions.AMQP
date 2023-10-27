using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.Design.Flow.Mapping.InputImpl;
using DecisionsFramework.Design.Properties;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.ServiceLayer.Services.ContextData;
using Amqp;
using Amqp.Framing;
using Amqp.Types;


namespace Zitac.AMQP.Steps;

[AutoRegisterStep("Send Message", "Integration", "AMQP")]
[Writable]
public class SendMessage : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer, IDefaultInputMappingStep
{

    [WritableValue]
    private bool expectResponse;

    [PropertyClassification(0, "Expect Response", new string[] { "Settings" })]
    public bool ExpectResponse
    {
        get { return expectResponse; }
        set
        {
            expectResponse = value;
            this.OnPropertyChanged("InputData");
            this.OnPropertyChanged("OutcomeScenarios");
        }

    }

    public IInputMapping[] DefaultInputs
    {
        get
        {
            IInputMapping[] inputMappingArray = new IInputMapping[4];
            inputMappingArray[0] = (IInputMapping)new ConstantInputMapping() { InputDataName = "Port", Value = "5672" };
            inputMappingArray[1] = (IInputMapping)new ConstantInputMapping() { InputDataName = "Content Type", Value = "text/plain" };
            inputMappingArray[2] = (IInputMapping)new ConstantInputMapping() { InputDataName = "Use SSL", Value = true };
            inputMappingArray[3] = (IInputMapping)new ConstantInputMapping() { InputDataName = "Timeout in Sec", Value = 10 };
            return inputMappingArray;
        }
    }

    public DataDescription[] InputData
    {
        get
        {

            List<DataDescription> dataDescriptionList = new List<DataDescription>();
            // Connection Settings
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Server") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Int32)), "Port") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Credentials)), "Credentials") { Categories = new string[] { "Connection Settings" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(bool)), "Use SSL") { Categories = new string[] { "Connection Settings" } });

            // Outgoing Message
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Outgoing Queue Name") { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Payload") { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(DataPair)), "Application Properties", true, true, false) { Categories = new string[] { "Outgoing Message" } });
            dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Content Type") { Categories = new string[] { "Outgoing Message" } });

            // Response
            if (expectResponse)
            {
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Response Queue Name") { Categories = new string[] { "Response" } });
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(String)), "Correlation ID") { Categories = new string[] { "Response" } });
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Int32)), "Timeout in Sec") { Categories = new string[] { "Response" } });
            }
            return dataDescriptionList.ToArray();
        }
    }
    public override OutcomeScenarioData[] OutcomeScenarios
    {
        get
        {
            List<OutcomeScenarioData> outcomeScenarioDataList = new List<OutcomeScenarioData>();


            if (expectResponse)
            {
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Done", new DataDescription(typeof(AMQPResult), "Result")));
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Timeout"));
            }
            else
            {
                outcomeScenarioDataList.Add(new OutcomeScenarioData("Done"));
            }
            outcomeScenarioDataList.Add(new OutcomeScenarioData("Error", new DataDescription(typeof(string), "Error Message")));
            return outcomeScenarioDataList.ToArray();
        }
    }
    public ResultData Run(StepStartData data)
    {
        // Connection Settings
        string Server = (string)data.Data["Server"];
        Int32 Port = (Int32)data.Data["Port"];
        Credentials Credentials = (Credentials)data.Data["Credentials"];
        bool UseSSL = (bool)data.Data["Use SSL"];

        // Outgoing Message
        string QueueName = (string)data.Data["Outgoing Queue Name"];
        string Payload = (string)data.Data["Outgoing Payload"];
        DataPair[] ApplicationProperties = (DataPair[])data.Data["Application Properties"];
        string ContentType = (string)data.Data["Content Type"];

        // Response
        string CorrelationID = (string)data.Data["Correlation ID"];
        string ResponseQueueName = (string)data.Data["Response Queue Name"];
        Int32 TimeOutInSec = (Int32)data.Data["Timeout in Sec"];

        TimeSpan TimeoutSpan = TimeSpan.FromSeconds(TimeOutInSec);


        string Protocol = "amqp";

        if (UseSSL)
        {
            Protocol = "amqps";
        }

        try
        {
            Connection connection = new Connection(new Address(Protocol + "://" + Credentials.Username + ":" + Credentials.Password + "@" + Server + ":" + Port));

            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-link", QueueName);

            Message message = new Message(Payload)
            {
                Properties = new Properties() { ContentType = ContentType },
            };

            foreach (var dataPair in ApplicationProperties)
            {
                message.ApplicationProperties[dataPair.Name] = dataPair.OutputValue;
            }

            if (expectResponse)
            {
                message.ApplicationProperties["correlationId"] = CorrelationID;
            }

            sender.Send(message);
            sender.Close();

            if (expectResponse)
            {

                Map filters = new Map();
                filters.Add(new Symbol("f1"), new DescribedValue(new Symbol("apache.org:selector-filter:string"), "correlationId = '" + CorrelationID + "'"));

                Source source = new Source() { Address = ResponseQueueName, FilterSet = filters };

                ReceiverLink receiver = new ReceiverLink(session, "response-link", source, null);
                Message responseMessage = receiver.Receive(TimeoutSpan);
                if (responseMessage != null)
                {
                    Console.WriteLine($"Received response: {responseMessage.Body} for request ID: {responseMessage.Properties.CorrelationId}");
                    receiver.Accept(responseMessage);
                    receiver.Close();
                    session.Close();
                    connection.Close();
                    AMQPResult Result = new AMQPResult(responseMessage);
                    Dictionary<string, object> dictionary = new Dictionary<string, object>();
                    dictionary.Add("Result", (object)Result);
                    return new ResultData("Done", (IDictionary<string, object>)dictionary);
                }
                else
                {
                    receiver.Close();
                    session.Close();
                    connection.Close();
                    return new ResultData("Timeout");
                }
            }

            session.Close();
            connection.Close();

        }
        catch (Exception e)
        {
            string ExceptionMessage = e.ToString();
            return new ResultData("Error", (IDictionary<string, object>)new Dictionary<string, object>()
                {
                {
                    "Error Message",
                    (object) ExceptionMessage
                }
                });
        }

        return new ResultData("Done");


    }
}
