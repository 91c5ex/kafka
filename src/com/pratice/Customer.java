package com.pratice;

public class Customer {
    private int customerId;
    private String customerName;



    public Customer(int Id, String customerName) {
        this.customerId = Id;
        this.customerName = customerName;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Customer{");
        sb.append("customerId=").append(customerId);
        sb.append(", customerName='").append(customerName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}


