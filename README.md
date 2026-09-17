# helsemelding-inbound-processing-service

Processes inbound dialog messages from Kafka. The service receives MsgHead XML messages, validates the Kafka record, converts valid messages to JSON, and publishes the JSON payload.

## Flow

```text
helsemelding.dialog.in.xml
    |
    v
InboundMessageProcessor
    |
    v
InboundMessageValidator
    |
    +-- invalid record --> logged and discarded
    |
    +-- valid XML --> message-converter --> helsemelding.dialog.in
```

## Validation

The service validates:

- Kafka record key exists and is a valid UUID
- Kafka record value exists, is not empty, and is valid XML

Invalid messages are logged as warnings and discarded.

## Topics

Default topic config:

- Input XML: `helsemelding.dialog.in.xml`
- Output JSON: `helsemelding.dialog.in`

## Error handling & failure behavior

The service handles failures during processing in a way to avoid losing messages.

**Possible failures**
- XML → JSON conversion can fail. In this case the conversion step throws a `FatalConversionException` which 
results in a `StreamsException` from Kafka Streams.
- Publishing to the output topic (`helsemelding.dialog.in`) can fail (e.g. broker/network issues).

**Intended behavior**
- When conversion fails the topology throws an exception and the problematic record is not forwarded to the output topic. The underlying issue must be fixed and the `helsemelding-inbound-processing-service` redeployed before previously failed records can be processed.
- Kafka Streams will not commit the offset for the failing record, so after a restart the service will attempt to re-process the record.
- This makes failures visible and ensures messages are not silently dropped.
