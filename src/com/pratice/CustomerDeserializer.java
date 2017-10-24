package com.pratice;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {

    int id;
    int nameSize;
    String name;

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Customer deserialize(String s, byte[] data) {


        if (data != null) {


            if (data.length < 8) {
                throw new SerializationException("Size of data received by IntegerDeserialize " +
                        " is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);

            id = buffer.getInt();
            String nameSize = String.valueOf(buffer.getInt());

            byte[] nameBytes = new byte[nameSize.length()];

            buffer.get(nameBytes);

            try {
                name = new String(nameBytes, "UTF-8");
            } catch (Exception e) {
                e.printStackTrace();
            }


            return new Customer(id, name);
        } else {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
