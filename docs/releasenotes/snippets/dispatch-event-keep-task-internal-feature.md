* The dispatcher now receives an `ApplicationOutboxEvent` instead of the internal `OutboxTask`, keeping task tracking details internal to the framework.
* Serialization of the event payload is defined via `ApplicationOutboxEvent.serializePayload`; deserialization is handled by the new `ApplicationOutboxDispatcher.deserialize` method.
