package com.simpledb.api.serializer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Created by yvladimirov on 10/23/15.
 */
public class Serializer {
    private ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(LinkedBuffer::allocate);


    public byte[] serialize(Object t) {
        buffer.get().clear();
        return ProtostuffIOUtil.toByteArray(t, RuntimeSchema.getSchema((Class<Object>) t.getClass()), buffer.get());
    }


    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws Exception {
        T t = clazz.newInstance();
        ProtostuffIOUtil.mergeFrom(bytes, t, RuntimeSchema.getSchema(clazz));
        return t;
    }
}
