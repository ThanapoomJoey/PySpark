# 🔧 การใช้งาน `withColumn()` ใน PySpark

`withColumn()` เป็นฟังก์ชันสำคัญใน PySpark ที่ใช้สำหรับการสร้างหรือเปลี่ยนแปลงคอลัมน์ใน DataFrame

---

## ✅ ใช้งานหลักของ `withColumn()`

### 1. สร้างคอลัมน์ใหม่  
เพิ่มคอลัมน์ใหม่จากการคำนวณหรือฟังก์ชันต่าง ๆ
```python
from pyspark.sql.functions import col
df = df.withColumn("age_plus_5", col("age") + 5)
 ```

### 2. เปลี่ยนแปลงค่าของคอลัมน์เดิม  
สามารถใช้ชื่อคอลัมน์เดิมใน `withColumn()` เพื่ออัปเดตค่าทับของเดิมได้เลย
```python
df = df.withColumn("age", col("age") + 10)  # แก้ age ให้เพิ่มขึ้น 10 ปี
 ```

### 3. เปลี่ยนชนิดข้อมูล (Data Type Casting)  
ใช้แปลงประเภทข้อมูล เช่น จากจำนวนเต็มเป็นสตริง หรือจากสตริงเป็นวันที่

```python
df = df.withColumn("age", col("age").cast("int"))
 ```

| ชนิดข้อมูล (string ที่ใช้ใน cast) | ชนิดจริงใน PySpark | หมายเหตุ |
|-----------------------------------|---------------------|----------|
| `"string"`                        | `StringType`        | ข้อความ |
| `"int"` หรือ `"integer"`         | `IntegerType`       | จำนวนเต็ม 32-bit |
| `"long"`                          | `LongType`          | จำนวนเต็ม 64-bit |
| `"float"`                         | `FloatType`         | ทศนิยม 32-bit |
| `"double"`                        | `DoubleType`        | ทศนิยม 64-bit |
| `"boolean"`                       | `BooleanType`       | จริง / เท็จ |
| `"date"`                          | `DateType`          | วันที่ |
| `"timestamp"`                    | `TimestampType`     | วัน-เวลา |

### 4. ใช้เงื่อนไข if-else (`when-otherwise`)  
สามารถใช้เงื่อนไขแบบ if-else เพื่อกำหนดค่าคอลัมน์ตามเงื่อนไข
```python
from pyspark.sql.functions import when

df = df.withColumn("age_group", when(col("age") > 30, "สูงกว่า 30").otherwise("ต่ำกว่า 30"))
 ```

### 5. ปรับแต่งข้อมูลข้อความ (String Transformation)  
ใช้ร่วมกับฟังก์ชันจัดการข้อความ เช่น เปลี่ยนเป็นตัวพิมพ์ใหญ่ พิมพ์เล็ก หรือเชื่อมข้อความ
**`upper()`**: แปลงเป็นตัวพิมพ์ใหญ่
```python
from pyspark.sql.functions import upper

df = df.withcolumn("name_upper", upper(col("name")))
 ```
**`lower()`**: แปลงข้อความให้เป็นตัวพิมพ์เล็ก
```python
from pyspark.sql.functions import lower

df = df.withColumn("name_lower", lower(col("name")))
 ```
**`trim()`**: ตัดช่องว่างข้างหน้าและข้างหลังข้อความ
```python
from pyspark.sql.functions import trim

df = df.withColumn("trimmed_name", trim(col("name")))
 ```

**`ltrim()`**: ตัดช่องว่างที่ด้านซ้ายของข้อความ
```python
from pyspark.sql.functions import ltrim , length

df = df.withColumn("left", ltrim(col("name")))

df.withColumn("length_ltrim",length(col("left"))).show()
 ```

 🔁 สรุปฟังก์ชัน **String Transformation** ที่ใช้บ่อย

| ฟังก์ชัน           | คำอธิบาย                          | ตัวอย่าง                                            |
|---------------------|------------------------------------|-----------------------------------------------------|
| **`upper()`**        | แปลงเป็นตัวพิมพ์ใหญ่              | `upper(col("name"))`                                |
| **`lower()`**        | แปลงเป็นตัวพิมพ์เล็ก              | `lower(col("name"))`                                |
| **`trim()`**         | ตัดช่องว่างข้างหน้าและข้างหลัง    | `trim(col("name"))`                                 |
| **`ltrim()`**        | ตัดช่องว่างด้านซ้าย               | `ltrim(col("name"))`                                |
| **`rtrim()`**        | ตัดช่องว่างด้านขวา                | `rtrim(col("name"))`                                |
| **`concat()`**       | เชื่อมข้อความจากหลายคอลัมน์       | `concat(col("first_name"), col("last_name"))`       |
| **`substr()`**       | ตัดข้อความบางส่วนจาก string       | `substr(col("name"), 1, 5)`                         |
| **`initcap()`**      | แปลงให้ตัวแรกเป็นตัวพิมพ์ใหญ่     | `initcap(col("name"))`                              |
| **`translate()`**    | แทนที่ตัวอักษรในข้อความ           | `translate(col("name"), "aeiou", "12345")`          |
| **`regexp_replace()`**| แทนที่ข้อความตาม pattern          | `regexp_replace(col("name"), "John", "Johnny")`     |
| **`regexp_extract()`**| ดึงข้อความจาก pattern             | `regexp_extract(col("name"), "^(.)", 1)`            |

### 6. สร้างคอลัมน์จากหลายคอลัมน์  
รวมค่าจากหลายคอลัมน์มาไว้ในคอลัมน์ใหม่ เช่น การบวกคะแนนจากหลายวิชา
```python
df.withColumn("total",col("quantity") * col("unit_price")).show()
 ```

### 7. ใส่ค่าคงที่ (Literal)  
เพิ่มคอลัมน์ที่มีค่าคงที่ เช่น ประเทศ, สถานะ, หมวดหมู่
```python
from pyspark.sql.functions import lit

df = df.withColumn("country", lit("Thailand"))
 ```

### 8. ใช้ร่วมกับ UDF (User Defined Function)  
ใช้ฟังก์ชันที่ผู้ใช้เขียนเองเพื่อสร้างหรือแปลงค่าคอลัมน์ตาม logic ที่ซับซ้อน
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def custom_label(age):
    return "senior" if age >= 60 else "junior"

label_udf = udf(custom_label, StringType())
df = df.withColumn("label", label_udf(col("age")))
 ```