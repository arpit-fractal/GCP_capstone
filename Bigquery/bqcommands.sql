#This will create a basic database named starschema and stored in asia-south1.
CREATE SCHEMA fractal1a.starschema
  OPTIONS (location = 'asia-south1');


#The below queries will create the needed tables.
CREATE OR REPLACE TABLE starschema.dim_order
(orderid INTEGER NOT NULL,
order_status_update_timestamp TIMESTAMP NOT NULL,
order_status string NOT NULL);


CREATE OR REPLACE TABLE starschema.fact_daily_orders(
customerid INTEGER NOT NULL,
orderid	INTEGER NOT NULL,
order_received_timestamp TIMESTAMP NOT NULL,
order_delivery_timestamp TIMESTAMP NOT NULL,
pincode	INTEGER NOT NULL,
order_amount INTEGER NOT NULL,
item_count INTEGER NOT NULL,
order_delivery_time_seconds INTEGER NOT NULL);


CREATE OR REPLACE TABLE starschema.dim_customer(
customerid INTEGER NOT NULL,
name string NOT NULL,
address_id INTEGER NOT NULL,
start_date DATE,
end_date DATE );


CREATE OR REPLACE TABLE starschema.dim_address(
address_id INTEGER NOT NULL,
address string NOT NULL,
city string NOT NULL,
state string NOT NULL,
pincode INTEGER NOT NULL);


CREATE OR REPLACE TABLE starschema.dim_product(
productid INTEGER NOT NULL,
productcode string NOT NULL,
productname string NOT NULL,
sku string NOT NULL,
rate INTEGER NOT NULL,
isactive BOOL NOT NULL,
start_date DATE,
end_date DATE );


CREATE OR REPLACE TABLE starschema.f_order_details(
orderid INTEGER NOT NULL,
order_delivery_timestamp TIMESTAMP NOT NULL,
productid INTEGER NOT NULL,
quantity INTEGER NOT NULL);