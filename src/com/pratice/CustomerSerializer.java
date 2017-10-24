package com.pratice;

import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements org.apache.kafka.common.serialization.Serializer<Customer> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Customer customer) {

        try {

            byte[] serializedName;
            int stringSize;

            if (customer == null) {
                return null;
            } else {
                if (customer.getCustomerName() != null) {
                    serializedName = customer.getCustomerName().getBytes("UTF-8");
                    stringSize = serializedName.length;

                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }

            }


            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(customer.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();

        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException("Error while serializing customer to byte[] " + e);
        }


    }

    @Override
    public void close() {

    }
}