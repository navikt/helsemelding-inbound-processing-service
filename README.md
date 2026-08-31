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
    +-- valid XML --> message-converter --> helsemelding.dialog.in.json
```

## Validation

The service validates:

- Kafka record key exists and is a valid UUID
- Kafka record value exists, is not empty, and is valid XML

Invalid messages are logged as warnings and discarded.

## Topics

Default topic config:

- Input XML: `helsemelding.dialog.in.xml`
- Output JSON: `helsemelding.dialog.in.json`
