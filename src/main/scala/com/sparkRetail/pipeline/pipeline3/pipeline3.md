![img.png](img.png)


![img_1.png](img_1.png)

# 📁 S3 Directory Structure & Expected Output (Pipeline 2 + Pipeline 3)

## ✔ Directory structure in S3 (Pipeline 2 + Pipeline 3)

### **Pipeline 2 Output — Parquet (Partitioned by customer_id)**

When Pipeline 2 ran, Spark wrote Parquet files into S3 and automatically created one folder per customer.  
I did **not** manually create any folders — Spark generated everything because of:

```scala
.partitionBy("customer_id")

Location

s3://spark-bucks/sales/parquet/

Structure
sales/parquet/
    ├── customer_id=1/
    ├── customer_id=2/
    ├── customer_id=3/
    ├── customer_id=4/
    ├── ...
    ├── customer_id=33/
    └── _temporary/   (created by Spark during write)


Location

s3://spark-bucks/aggregates/products.json/

What Spark created
products.json/
    ├── part-00000-287bca40-57ea-4a1b-b616-dcb3340b4feb-c000.json
    └── _SUCCESS

{"product_name":"Bluetooth Earbuds","total_quantity":1,"total_revenue":750.9}
{"product_name":"Wireless Keyboard","total_quantity":1,"total_revenue":2500.75}
{"product_name":"Monitor 24-inch","total_quantity":1,"total_revenue":890.0}
{"product_name":"Bluetooth Speaker","total_quantity":1,"total_revenue":890.2}
{"product_name":"Water Bottle","total_quantity":4,"total_revenue":1500.0}
{"product_name":"Keyboard Cover","total_quantity":1,"total_revenue":650.0}
{"product_name":"Tripod Stand","total_quantity":1,"total_revenue":2200.0}
{"product_name":"Pen Drive 64GB","total_quantity":2,"total_revenue":975.5}
{"product_name":"Office Chair","total_quantity":1,"total_revenue":980.0}
{"product_name":"Laptop Skin","total_quantity":1,"total_revenue":1900.2}
{"product_name":"Gaming Mouse","total_quantity":2,"total_revenue":3400.0}
{"product_name":"Laptop Stand","total_quantity":1,"total_revenue":1999.99}
{"product_name":"Power Bank","total_quantity":2,"total_revenue":1150.0}
{"product_name":"Smartwatch","total_quantity":1,"total_revenue":2300.0}
{"product_name":"Ring Light","total_quantity":2,"total_revenue":1250.75}
{"product_name":"Smartphone","total_quantity":1,"total_revenue":1200.75}
{"product_name":"AC Cooler","total_quantity":1,"total_revenue":4000.0}
{"product_name":"Phone Case","total_quantity":2,"total_revenue":660.0}
{"product_name":"Mouse Pad","total_quantity":3,"total_revenue":420.0}
{"product_name":"HDMI Cable","total_quantity":2,"total_revenue":780.8}
{"product_name":"Mic Stand","total_quantity":1,"total_revenue":3300.55}
{"product_name":"HD Webcam","total_quantity":1,"total_revenue":499.99}
{"product_name":"LED Strip","total_quantity":3,"total_revenue":880.0}
{"product_name":"USB Cable","total_quantity":3,"total_revenue":500.0}
{"product_name":"Earphones","total_quantity":1,"total_revenue":999.99}
{"product_name":"RAM 16GB","total_quantity":1,"total_revenue":120.0}
{"product_name":"Backpack","total_quantity":1,"total_revenue":510.5}
{"product_name":"Notebook","total_quantity":5,"total_revenue":350.0}
{"product_name":"SSD 1TB","total_quantity":1,"total_revenue":320.0}
{"product_name":"Charger","total_quantity":2,"total_revenue":550.4}
{"product_name":"Mouse","total_quantity":2,"total_revenue":1500.5}
{"product_name":"T-Shirt","total_quantity":3,"total_revenue":760.0}
{"product_name":"Keyboard","total_quantity":1,"total_revenue":2000.0}
{"product_name":"Shoes","total_quantity":1,"total_revenue":1450.5}
