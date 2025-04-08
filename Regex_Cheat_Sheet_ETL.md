
# 📘 Regex Cheat Sheet สำหรับ ETL (PySpark / SQL)

รวม 10 regex สำเร็จรูปที่ใช้บ่อยในงาน Data Engineer, ETL, และการจัดการข้อความด้วย PySpark หรือ SQL

---

## ✅ 1. ดึงนามสกุลจากชื่อเต็ม (First Last)

```python
regexp_extract(col("full_name"), r"^\w+\s+(\w+)", 1)
```
📌 ดึงคำหลังช่องว่าง เช่น `"John Smith"` → `"Smith"`

---

## ✅ 2. ดึงชื่อ (First Name) จากชื่อเต็ม

```python
regexp_extract(col("full_name"), r"^(\w+)", 1)
```
📌 ดึงคำแรก เช่น `"John Smith"` → `"John"`

---

## ✅ 3. ดึง Domain จาก Email

```python
regexp_extract(col("email"), r"@([\w\.]+)$", 1)
```
📌 `"joe@gmail.com"` → `"gmail.com"`

---

## ✅ 4. ดึงตัวเลขจากข้อความ เช่น ID

```python
regexp_extract(col("employee_id"), r"(\d+)", 1)
```
📌 `"ID_00123"` → `"00123"`

---

## ✅ 5. ตรวจสอบว่าข้อความเริ่มด้วย A-Z (ไม่ดึง แค่ตรวจ)

```sql
column RLIKE '^[A-Z]'
```
📌 ตรวจว่าเริ่มด้วยตัวอักษรใหญ่หรือไม่

---

## ✅ 6. ดึงปีจากวันที่แบบ `YYYY-MM-DD`

```python
regexp_extract(col("date"), r"(\d{4})-\d{2}-\d{2}", 1)
```
📌 `"2023-12-31"` → `"2023"`

---

## ✅ 7. ดึงเดือนจากวันที่

```python
regexp_extract(col("date"), r"\d{4}-(\d{2})-\d{2}", 1)
```
📌 `"2023-12-31"` → `"12"`

---

## ✅ 8. ดึงคำสุดท้ายจากข้อความ

```python
regexp_extract(col("sentence"), r"(\w+)$", 1)
```
📌 `"hello world"` → `"world"`

---

## ✅ 9. ดึงรหัสไปรษณีย์ 5 หลัก

```python
regexp_extract(col("address"), r"(\d{5})", 1)
```
📌 `"Bangkok 10200"` → `"10200"`

---

## ✅ 10. ดึงค่าภายในวงเล็บ

```python
regexp_extract(col("note"), r"\(([^)]+)\)", 1)
```
📌 `"Price (USD)"` → `"USD"`

---

## 🧠 Tips:
- `(\w+)`: จับคำ
- `(\d+)`: จับตัวเลข
- `^`, `$`: เริ่ม / จบข้อความ
- `+`, `{n}`: จำนวนตัวที่ต้องการ
- `[A-Z]`, `[a-z]`, `[0-9]`: กลุ่มตัวอักษร/ตัวเลข
- `[^)]`: ทุกอย่างยกเว้น `)`

## 🧠 สรุป Quantifier ที่ใช้บ่อยใน Regex

| Quantifier | ความหมาย                           |
|------------|-------------------------------------|
| `.`        | ตัวใดก็ได้ 1 ตัว                   |
| `+`        | อย่างน้อย 1 ตัว                    |
| `*`        | 0 ตัวขึ้นไป                         |
| `{n}`      | จำนวนเป๊ะ ๆ (ตัวอักษรหรือตัวเลข n ตัว) |
| `{n,m}`    | จำนวนระหว่าง n ถึง m ตัว          |
| `\w`       | ตัวอักษรหรือตัวเลข (word)         |
| `\d`       | ตัวเลข (digit)                     |
| `\s`       | ช่องว่าง (space)                   |

---

## 🧪 ตัวอย่างการใช้งาน:

- `\d{3}`: ตัวเลข 3 ตัว เช่น `123`
- `\w+`: ตัวอักษรหรือตัวเลข 1 ตัวขึ้นไป เช่น `abc123`
- `\s*`: ช่องว่าง 0 ตัวขึ้นไป เช่น `   ` หรือไม่มีช่องว่างเลย


---

## 🧪 ตัวอย่างการใช้ใน PySpark

```python
from pyspark.sql.functions import regexp_extract

df.withColumn("last_name", regexp_extract("full_name", r"^\w+\s+(\w+)", 1)).show()
```

---

## 📍 ใช้ได้กับ:
- PySpark: `regexp_extract()`
- Spark SQL: `REGEXP_EXTRACT(col, pattern, group)`
- SQL ทั่วไป: `REGEXP_EXTRACT()` หรือ `RLIKE`

---
