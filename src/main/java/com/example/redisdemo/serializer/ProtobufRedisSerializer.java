package com.example.redisdemo.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class ProtobufRedisSerializer<T extends Message> implements RedisSerializer<T> {

    private final Message defaultInstance;

    public ProtobufRedisSerializer(Message defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    @Override
    public byte[] serialize(T message) throws SerializationException {
        return message.toByteArray();
    }

    @Override
    public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            T message = (T) defaultInstance.getParserForType().parseFrom(bytes);
            return message;
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("Error deserializing protobuf message", e);
        }
    }
}
