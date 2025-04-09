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

# 📘 Day 3: GroupBy, Aggregate, Sort

## 🎯 เป้าหมาย
- เข้าใจการใช้ `.groupBy()` และฟังก์ชัน Aggregate เช่น `count()`, `sum()`, `avg()`
- เรียนรู้การจัดเรียงข้อมูลด้วย `.orderBy()`
- ทำ Mini Project ที่รวมทุกอย่าง

---

## 🧠 เนื้อหา

### 1. `groupBy() + agg()`

ใช้สำหรับ **จัดกลุ่มข้อมูล** ตามคอลัมน์ แล้วใช้ฟังก์ชัน aggregate เพื่อสรุปข้อมูล

```python
from pyspark.sql.functions import count, sum, avg, max, min

df.groupBy("category").agg(
    count("*").alias("total_rows"),
    sum("sales").alias("total_sales"),
    avg("sales").alias("avg_sales")
).show()
```

---

### 2. ฟังก์ชัน Aggregate ที่สำคัญ

| ฟังก์ชัน | ใช้ทำอะไร |
|----------|-----------|
| `count()` | นับจำนวนแถว |
| `sum()` | รวมค่าตัวเลข |
| `avg()` | หาค่าเฉลี่ย |
| `max()` | หาค่าสูงสุด |
| `min()` | หาค่าต่ำสุด |

```python
from pyspark.sql.functions import sum , format_number

df.groupBy("branch_id").agg(format_number(sum("total_amount"),2).alias("total_branch_sales")).show()
 ```
---

### 3. `orderBy()` การจัดเรียงข้อมูล

```python
df.orderBy("sales").show()  # เรียงจากน้อยไปมาก
df.orderBy(col("sales").desc()).show()  # เรียงจากมากไปน้อย แบบ 1
df.orderBy(col("branch_id"),ascending = False).show() # เรียงจากมากไปน้อย แบบที่ 2
```

**หลายเงื่อนไข:**
```python
df.orderBy(df.category.asc(), df.sales.desc()).show()
```

---

### 4. กรองข้อมูลหลัง groupBy ด้วย `filter()`

```python
agg_df = df.groupBy("category").agg(sum("sales").alias("total_sales"))
agg_df.filter(agg_df.total_sales > 1000).show()
```

---

### 5. ใช้ `.alias()` ตั้งชื่อใหม่ให้คอลัมน์

```python
df.groupBy("region").agg(avg("score").alias("average_score")).show()
```

**รวมการใช้งาน**
```python
from pyspark.sql.functions import sum , col

df.groupBy(col("sale_date") , col("branch_id"))\
  .agg(sum("quantity").alias("total_quantity"))\
  .orderBy(col("sale_date").asc(),col("total_quantity").desc())\
  .filter(col("total_quantity") > 25000).show()
 ```

# 📘 Day 4: Join + SQL  

## 🎯 เป้าหมาย:
- ใช้ `.join()` เชื่อมข้อมูล 2 ชุด
- ใช้ SQL ผ่าน `spark.sql()` เพื่อเขียน query บน DataFrame
- ทำ Mini Project เพื่อฝึก Join + SQL แบบครบถ้วน

---

## 🧠 เนื้อหา

### 🔹 1. PySpark `.join()`

```python
df1.join(df2, on="id", how="inner")
 ```

### 🔹 2. ประเภทของ Join

| ประเภท Join | คำอธิบาย |
|-------------|----------|
| `inner`     | แสดงเฉพาะแถวที่ match กันทั้ง 2 ฝั่ง |
| `left`      | แสดงทุกแถวจากฝั่งซ้าย ถ้าไม่ match จะเป็น NULL |
| `right`     | แสดงทุกแถวจากฝั่งขวา ถ้าไม่ match จะเป็น NULL |
| `outer`     | แสดง ทุกข้อมูลที่มีอยู่ จากทั้งสองตาราง แม้ไม่มี match ก็ตาม |

```python
df1.join(df2, df1.id == df2.id, how="left")
 ```

---

### 🔹 3. แก้ชื่อซ้ำก่อน Join

```python
df1 = df1.withColumnRenamed("name", "name_df1")
df2 = df2.withColumnRenamed("name", "name_df2")
 ```

---

### 🔹 4. การใช้ SQL บน DataFrame

```python
sales_df.createOrReplaceTempView("temp_sale")
product_df.createOrReplaceTempView("temp_product")

spark.sql("""
          select s.customer , s.sale_date, sum((p.price - p.cost)*s.quantity) as total_profit , sum(s.quantity * p.price) as total_sale
          from temp_sale s
          left join temp_product p
            on s.product_id = p.product_id
          group by s.customer,s.sale_date
          """).show()
 ```

---
