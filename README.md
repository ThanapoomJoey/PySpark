# 📘 Day 1: รู้จัก PySpark และ SparkSession

## 🎯 เป้าหมาย
เข้าใจพื้นฐาน Spark, สร้าง SparkSession และโหลด DataFrame

---

## 📘 PySpark คืออะไร ทำไมถึงใช้  
**PySpark** คือ interface ของ Apache Spark สำหรับภาษา Python  
ใช้จัดการข้อมูลขนาดใหญ่แบบ distributed รองรับงาน ETL, Machine Learning, Analytics  
จุดเด่นคือทำงานเร็ว กระจายโหลดได้หลายเครื่องแบบ scale-out (หลาย node)

---

## ⚙️ SparkSession คืออะไร  
`SparkSession` คือจุดเริ่มต้นของการใช้ PySpark  
ใช้เพื่อสื่อสารกับ Spark engine และใช้โหลด/จัดการข้อมูล

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyFirstApp").getOrCreate()
 ```

## 📂 อ่านไฟล์ CSV / สร้าง DataFrame จาก Python dict
**จากไฟล์ CSV:**
```python
df = spark.read.csv("/FileStore/tables/supermarket_sales_20241231.csv", header=True , inferSchema=True)
 ```

###### `header=True`
พารามิเตอร์นี้บอกให้ PySpark ใช้แถวแรกในไฟล์ CSV เป็นชื่อคอลัมน์  
ถ้าตั้งค่าเป็น `False` PySpark จะถือว่าแถวแรกเป็นข้อมูลปกติ ไม่ใช่ชื่อคอลัมน์

###### `inferSchema=True`
ใช้ให้ PySpark ตรวจสอบประเภทข้อมูลในแต่ละคอลัมน์โดยอัตโนมัติ  
เช่น ถ้าคอลัมน์นั้นๆ เป็นตัวเลข PySpark จะกำหนดประเภทเป็น `IntegerType` หรือ `DoubleType` เป็นต้น  
ถ้าไม่ตั้งค่าเป็น `True` หรือไม่ระบุ ค่าเริ่มต้นจะเป็นการอ่านข้อมูลทั้งหมดเป็น `String` (ข้อความ)

**ตรวจสอบไฟล์ที่ Upload ใน dbft**
```python
%fs ls dbfs:/FileStore/tables/
 ```

**ลบไฟล์ ใน dbfs**
```python
%fs rm -r  dbfs:/FileStore/tables/supermarket_sales_20241231.csv
 ```
```python
%fs rm -r dbfs:/FileStore/tables/supermarket_sales_20241231.parquet/
 ```

**จาก list ของ dict:**
```python
data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
df = spark.createDataFrame(data)
 ```

**จาก List of Tuples + Schema**
```python
data = [("John", 30), ("Jane", 25)]
schema = ["name", "age"]
df = spark.createDataFrame(data, schema)
 ```

## 🔍 ใช้ .show() และ .printSchema()
```python
df.show(5)           # แสดง 5 แถวแรกของข้อมูล
df.printSchema()     # แสดง schema ของ DataFrame
df.dtypes            # Output: [('Name', 'string'), ('Age', 'bigint')]
print((df.count(), len(df.columns)))  # (10000000, 8)
  ```

# 📘 Day 2: คำสั่งพื้นฐานที่ใช้บ่อยใน PySpark

## 🎯 เป้าหมาย
ใช้คำสั่งพื้นฐานต่าง ๆ เช่น `select`, `filter`, `withColumn`, `drop` และเข้าใจการใช้งานฟังก์ชันจาก `pyspark.sql.functions`

---

## 📂 คำสั่ง `select()`, `filter()`, `where()`

- **`select()`**: ใช้เพื่อเลือกคอลัมน์จาก DataFrame ที่ต้องการแสดงผล
    ```python
    df.select("sale_id","customer_id").show(5)
     ```
- **`filter()`** และ **`where()`**: ใช้กรองข้อมูลตามเงื่อนไขที่กำหนด
    ```python
    df2 = df.filter(df.sale_date == "2024-11-08")
    df2.show(5)
    df2.count()
     ```
    ```python
    df.where(df.sale_date == "2024-11-08")
     ```

---

## ⚙️ การใช้ `withColumn()`, `drop()`

- **`withColumn()`**: ใช้เพิ่มหรือเปลี่ยนแปลงคอลัมน์ใน DataFrame โดยสามารถใช้ฟังก์ชันหรือคำนวณค่าต่าง ๆ ได้
    ```python
    from pyspark.sql.functions import col

    df = df.withColumn("age_plus_10", col("age") + 10)
    df.show()
     ```
    การใช้งาน `withColumn()` อื่นๆดูได้ที่ : [withColumn.md](withColumn.md)

- **`drop()`**: ใช้เพื่อลบคอลัมน์ที่ไม่ต้องการออกจาก DataFrame
    ```python
    df = df.drop("age_plus_10")
    df.show()
     ```

---

## 🔢 ฟังก์ชันใน `pyspark.sql.functions`

- **`col()`**: ใช้เลือกคอลัมน์จาก DataFrame โดยใช้ชื่อคอลัมน์
    ```python
    from pyspark.sql.functions import col
    df.select(col("name"), col("age")).show()
     ```

- **`lit()`**: ใช้เพิ่มค่าคงที่ใน DataFrame โดยสามารถนำค่าคงที่มาใช้ร่วมกับคอลัมน์
    ```python
    from pyspark.sql.functions import lit
    df = df.withColumn("constant_value", lit(100))
    df.show()
     ```
- **`when()`**: ใช้เพื่อสร้างคอลัมน์ใหม่โดยการเปลี่ยนแปลงค่าตามเงื่อนไข เช่น การใช้ `if-else` ใน DataFrame
    ```python
    from pyspark.sql.functions import when
    df = df.withColumn("age_group", when(df.age > 30, "สูงกว่า 30").otherwise("ต่ำกว่า 30"))
    df.show()
     ```

---
