# Airflow ETL Pipeline: PostgreSQL to MySQL

Project ini adalah **ETL pipeline otomatis menggunakan Apache Airflow** yang memindahkan dan mentransformasi data dari **PostgreSQL (operasional)** ke **MySQL (data warehouse)**. Pipeline dijalankan setiap 6 jam untuk mendukung laporan real-time.

---

## Tujuan Project
- Mengambil data terbaru dari database operasional PostgreSQL.
- Membersihkan dan mentransformasi data sesuai business rules.
- Memuat data ke MySQL menggunakan UPSERT untuk menjaga integritas.
- Menyediakan pipeline ETL yang robust dengan logging dan error handling.

---

## Proses ETL

### 1. Extract
- Mengambil data **Customers, Products, Orders** dari PostgreSQL.
- Filter incremental: hanya data yang diupdate dalam 1 hari terakhir.
- Data disimpan sementara menggunakan **XCom Airflow** untuk diproses tahap selanjutnya.

### 2. Transform & Load
- **Customers:** format nomor telepon, kode negara bagian uppercase.
- **Products:** hitung margin keuntungan, normalisasi kategori.
- **Orders:** status lowercase, total amount divalidasi.
- Data di-load ke MySQL menggunakan **UPSERT** ke tabel `dim_customers`, `dim_products`, dan `fact_orders`.

### 3. DAG & Scheduling
- Nama DAG: `postgres_to_mysql_etl`
- Schedule: setiap **6 jam**
- Error handling: retry 2 kali dengan delay 5 menit
- Logging: mencatat jumlah record extract, transform, load, serta error jika terjadi

---

## Hasil & Insight
- Pipeline berhasil menyinkronkan data operasional ke data warehouse secara otomatis.
- Data siap digunakan untuk **analytics dan reporting real-time**.
- Logging memudahkan monitoring dan debugging.

---

## Tools & Teknologi
- **Python** (Airflow, Hooks, XCom)  
- **SQL** (PostgreSQL & MySQL)  
- **Apache Airflow**  
- **Data Warehouse & ETL best practices**

---
