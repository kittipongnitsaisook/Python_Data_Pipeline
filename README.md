# Python Data Pipeline Engineering — Data Warehouse ยอดขายค้าปลีก Omnichannel

ไปป์ไลน์ ETL แบบ **Incremental** และ **Idempotent** ที่แปลงข้อมูล `orders_batch_*`
ที่ยุ่งเหยิงทั้ง 3 ไฟล์ให้กลายเป็น Star Schema สะอาดใน SQLite โดยข้อมูลแถวที่มีปัญหา
จะถูกกักไว้ใน quarantine แทนที่จะทำให้ไปป์ไลน์ล่ม

## 1. การติดตั้ง (Setup)

```bash
pip install pandas openpyxl
```

ต้องใช้ Python 3.10 ขึ้นไป (โค้ดใช้ `list[int]`, `tuple[...]`,
`from __future__ import annotations`)

วาง source workbook ไว้ในโฟลเดอร์เดียวกับ `pipeline.py` โดยตั้งชื่อไฟล์ว่า
`source_data.xlsx` (ต้องมีชีตชื่อ `customers`, `products`, `orders_batch_1`,
`orders_batch_2`, `orders_batch_3`, `data_dictionary`) ไปป์ไลน์จะ**ไม่แก้ไข
ไฟล์ต้นฉบับเด็ดขาด** — อ่านข้อมูลผ่าน `pandas.read_excel` เท่านั้น

## 2. การรัน (Run)

```bash
python pipeline.py
```

เมื่อรันแล้วจะทำงานตามลำดับนี้:

1. **Run 1** — โหลด `batch_1` เข้าสู่ `retail_dw.db` ที่ว่างเปล่า
2. **Run 2** — โหลด `batch_1` ซ้ำอีกครั้ง (ทดสอบ idempotency — จำนวนแถวใน
   `fact_sales` ต้อง**ไม่เพิ่มขึ้น**)
3. **Run 3** — โหลด `batch_2`
4. **Run 4** — โหลด `batch_3`

หลังแต่ละขั้นตอนจะพิมพ์ run log ออกมา และเมื่อจบทั้งหมดจะสรุป KPI พร้อมสร้างไฟล์:

| ไฟล์ | เนื้อหา |
|---|---|
| `retail_dw.db` | ฐานข้อมูล SQLite — Star Schema ที่โหลดข้อมูลครบแล้ว |
| `quarantine.csv` | ทุกแถวที่ถูกปฏิเสธ พร้อม `reason_code` และ `source_batch` |
| `pipeline_run_log.csv` | หนึ่งแถวต่อหนึ่งการรัน: จำนวนแถวที่อ่าน/ผ่าน/ถูกปฏิเสธ/ซ้ำ/โหลดสำเร็จ, เวลาที่ใช้, สถานะ |

หากต้องการรันเฉพาะบางแบตช์ผ่านโค้ด:

```python
from pipeline import PipelineConfig, run_pipeline

config = PipelineConfig(
    input_path="source_data.xlsx",
    output_database="retail_dw.db",
    batches=[2],               # เลือกได้จาก [1, 2, 3]
    error_mode="quarantine",   # หรือ "strict" เพื่อยกเลิกทั้งแบตช์ถ้าเจอแถวเสีย
)
result = run_pipeline(config)
print(result["kpi"])
```

## 3. Star Schema

```
dim_customer(customer_key PK, customer_id UNIQUE, customer_name, province, segment)
dim_product (product_key  PK, product_id  UNIQUE, product_name, category)
dim_date    (date_key     PK, full_date   UNIQUE, day, month, quarter, year)

fact_sales (
  order_id       PK,                       -- grain: หนึ่งออร์เดอร์ที่ผ่านการตรวจสอบต่อหนึ่งแถว
  date_key       FK -> dim_date,
  customer_key   FK -> dim_customer,
  product_key    FK -> dim_product,
  quantity, unit_price, discount_pct,
  gross_amount, net_amount,
  payment_method, sales_channel,
  source_batch, updated_at
)
```

**Grain ของ `fact_sales`**: หนึ่งรายการขายที่ผ่านการตรวจสอบต่อหนึ่ง `order_id`
เมื่อพบ `order_id` ซ้ำ (ไม่ว่าจะซ้ำภายในแบตช์เดียวกันหรือข้ามแบตช์) ระบบจะเก็บ
เฉพาะแถวที่มี `updated_at` ล่าสุด และใช้วิธี **upsert**
(`INSERT ... ON CONFLICT DO UPDATE ... WHERE excluded.updated_at >=
fact_sales.updated_at`) ทำให้การรันซ้ำไม่มีทางสร้างแถว fact ที่สองสำหรับ
ออร์เดอร์เดียวกัน

ตารางสนับสนุนอื่น ๆ:
- `quarantine` — แถวที่ถูกปฏิเสธ พร้อม `reason_code` (คั่นด้วยเซมิโคลอนถ้าผิดหลาย
  เงื่อนไข) และ `source_batch`
- `pipeline_run_log` — หนึ่งแถวต่อการรันหนึ่งครั้ง พร้อมจำนวนแถวและสถานะ
- `batch_watermark` — เก็บค่า `updated_at` ล่าสุดที่เคยประมวลผลของแต่ละแบตช์
  ใช้สำหรับ incremental loading

## 4. กฎการตรวจสอบคุณภาพข้อมูล (Data Quality Rules)

| การตรวจสอบ | Reason code |
|---|---|
| `order_datetime` / `updated_at` แปลงเป็นวันที่ไม่ได้ | `INVALID_DATE` / `INVALID_UPDATED_AT` |
| `quantity` ไม่ใช่จำนวนเต็มระหว่าง 1–20 | `INVALID_QUANTITY` |
| `unit_price` ไม่ใช่ตัวเลข > 0 (ตัดคำนำหน้า `THB` ออกก่อน) | `INVALID_UNIT_PRICE` |
| `discount_pct` ไม่อยู่ในช่วง 0–100 | `INVALID_DISCOUNT_PCT` |
| `customer_id` ว่างเปล่า / หาไม่พบใน `dim_customer` | `MISSING_CUSTOMER_ID` / `CUSTOMER_NOT_FOUND` |
| `product_id` ว่างเปล่า / หาไม่พบใน `dim_product` | `MISSING_PRODUCT_ID` / `PRODUCT_NOT_FOUND` |
| สินค้ามีอยู่จริงแต่ `active_flag != 'Y'` | `INACTIVE_PRODUCT` |
| `payment_method` ไม่ใช่ Cash/Credit Card/PromptPay/Bank Transfer (ไม่สนตัวพิมพ์เล็กใหญ่) | `INVALID_PAYMENT_METHOD` |
| `sales_channel` ไม่ใช่ Store/Online/Marketplace (`E-Commerce` → `Online`) | `INVALID_SALES_CHANNEL` |

`gross_amount = quantity * unit_price` และ
`net_amount = gross_amount * (1 - discount_pct/100)` จะคำนวณ**เฉพาะแถวที่ผ่าน
ทุกเงื่อนไขข้างต้นเท่านั้น** จึงไม่มีทางติดลบ

## 5. Idempotency และ Incremental Loading

- **Idempotency**: `fact_sales.order_id` เป็น primary key และการโหลดใช้
  `INSERT ... ON CONFLICT(order_id) DO UPDATE` การรันแบตช์เดิมซ้ำจะอัปเดต
  แถวเดิมแทนการเพิ่มแถวใหม่
- **Incremental loading**: ตาราง `batch_watermark` เก็บค่า `updated_at`
  สูงสุดที่เคยเห็นของแต่ละแบตช์ ทุกครั้งที่รัน แถวที่มี `updated_at` เท่ากับ
  หรือเก่ากว่า watermark จะถูกข้ามไปตั้งแต่ก่อนขั้นตอน validation (ส่วนแถวที่
  `updated_at` แปลงค่าไม่ได้จะไม่ถูกข้ามเด็ดขาด เพื่อให้ยังคงถูกตรวจสอบและ
  ไปลง quarantine พร้อม reason code) และ watermark จะขยับไปข้างหน้าโดยนับจาก
  ทุกแถวที่ "อ่าน" ในรอบนั้น รวมถึงแถวที่ถูกปฏิเสธด้วย เพื่อไม่ให้แบตช์ที่รัน
  จบไปแล้วถูกประมวลผลซ้ำอีก

หลักฐาน: รัน `python pipeline.py` — คอนโซลจะระบุชัดเจนว่าแต่ละ 4 การรันคือ
อะไร (`batch_1` ครั้งแรก, `batch_1` รันซ้ำ, `batch_2`, `batch_3`) และพิมพ์ตาราง
run log หลังแต่ละรอบ ส่วน `pipeline_run_log.csv` ก็เก็บหลักฐานเดียวกันไว้บนดิสก์

## 6. สูตร KPI (Acceptance Test ข้อ 7)

ทุกแบตช์ที่รันต้องเป็นจริงตามสมการนี้: **`rows_read == rows_valid + rows_rejected`**
โดยวัด**ก่อน**ขั้นตอน deduplicate (ฟังก์ชัน `transform_batch` คำนวณ
`rows_valid` / `rows_rejected` จากข้อมูลทั้งหมดที่ผ่านการกรองด้วย watermark
แล้ว) ส่วนการ deduplicate จะเกิดขึ้นทีหลังในฟังก์ชัน `load_facts` และรายงาน
แยกต่างหากเป็น `rows_duplicated` ดังนั้น
`rows_loaded = rows_valid - rows_duplicated` คือจำนวนแถวที่ upsert เข้า
`fact_sales` จริง ๆ

## 7. Reflection — ทำไม Availability จึงสำคัญกว่า Strictness ในไปป์ไลน์จริง

ไปป์ไลน์ที่หยุดทำงานทันทีที่เจอข้อมูลเสียเพียงแถวเดียว แม้จะดูถูกต้องในทาง
ทฤษฎี แต่กลับขัดกับเป้าหมายที่แท้จริงของมัน คือการทำให้ data warehouse
ทันสมัยอยู่เสมอ ระบบต้นทางมักมีข้อมูลไม่สมบูรณ์เป็นธรรมชาติ — ส่วนลดพิมพ์ผิด
สกุลเงินติดมาด้วย ลูกค้าที่ยังไม่ sync เข้าระบบ — ซึ่งล้วนอยู่นอกเหนือการควบคุม
ของไปป์ไลน์ ถ้าแถวเสียเพียงแถวเดียวสามารถทำให้ทั้งแบตช์ล้มได้ แถวที่ถูกต้อง
อีกจำนวนมากก็จะติดค้างอยู่หลังแถวนั้นไปด้วย และแดชบอร์ดก็จะเก่าไปเรื่อย ๆ
โดยไม่มีใครรู้ตัว จนกว่าจะมีคนไล่หาแถวที่ทำให้พัง การกักแถวเสียไว้ใน
quarantine พร้อม `reason_code` ที่ชัดเจน ช่วยให้ข้อมูลดีกว่า 90% เข้า
warehouse ได้ตรงเวลา และเปลี่ยนแถวเสียให้กลายเป็นคิวงานที่ตรวจสอบทีหลังได้
แทนที่จะเป็นเหตุการณ์ระบบล่ม ความเข้มงวด (strictness) ยังคงจำเป็นอยู่ — เพียง
แต่ควรบังคับใช้ที่ระดับแถว (ปฏิเสธแถวนั้น) ไม่ใช่ระดับแบตช์หรือทั้งไปป์ไลน์
จุดเดียวที่ควรเข้มงวดจริง ๆ คือเมื่อข้อผิดพลาดเป็นปัญหาเชิงโครงสร้าง ไม่ใช่
ปัญหาระดับข้อมูล เช่น ไฟล์ต้นทางหายไปหรือทั้งแบตช์อ่านไม่ได้เลย เพราะการเดา
โครงสร้างที่ผิดรูปแบบมีความเสี่ยงที่จะโหลดข้อมูลผิดพลาดเป็นระบบ มากกว่าแค่
ข้อมูลเสียไม่กี่แถว นี่คือเหตุผลที่ไปป์ไลน์นี้จะบันทึก log ว่าแบตช์ล้มเหลว
แล้วข้ามไปทำแบตช์ถัดไปโดยไม่แตะข้อมูลที่โหลดจากแบตช์อื่นไปแล้ว แต่จะไม่มีวัน
ปล่อยให้แถวเสียเพียงแถวเดียวหยุดทั้งแบตช์ที่ยังอ่านได้ปกติ
