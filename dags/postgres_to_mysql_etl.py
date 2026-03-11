from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import logging
import re

# Konfigurasi DAG
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_to_mysql_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
)

# Extract
def extract_customers_from_postgres(**context):
    """
    Ekstrak data customer dari PostgreSQL.
    
    Filter: Customer yang di-update dalam 1 hari terakhir.
    Output: Push ke XCom dengan key 'customers_data'.
    """
    try:
        logging.info("Mulai ekstraksi customers dari PostgreSQL")
        
        # Koneksi ke PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Query ekstraksi
        query = """
            SELECT customer_id, customer_name, email, phone, address, 
                   city, state, zip_code, country, updated_at
            FROM raw_data.customers
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Konversi ke list of dictionaries
        customers_data = [dict(zip(columns, row)) for row in rows]
        
        logging.info(f"Berhasil ekstrak {len(customers_data)} customers")
        
        # Push ke XCom
        context['task_instance'].xcom_push(key='customers_data', value=customers_data)
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat ekstrak customers: {str(e)}")
        raise


def extract_products_from_postgres(**context):
    """
    Ekstrak data product dari PostgreSQL dengan JOIN ke suppliers.
    
    Filter: Product yang di-update dalam 1 hari terakhir.
    Output: Push ke XCom dengan key 'products_data'.
    """
    try:
        logging.info("Mulai ekstraksi products dari PostgreSQL")
        
        # Koneksi ke PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Query dengan JOIN
        query = """
            SELECT p.product_id, p.product_name, p.category, p.price, 
                   p.cost, p.stock_quantity, s.supplier_name, p.updated_at
            FROM raw_data.products p
            JOIN raw_data.suppliers s ON p.supplier_id = s.supplier_id
            WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Konversi ke list of dictionaries
        products_data = [dict(zip(columns, row)) for row in rows]
        
        logging.info(f"Berhasil ekstrak {len(products_data)} products")
        
        # Push ke XCom
        context['task_instance'].xcom_push(key='products_data', value=products_data)
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat ekstrak products: {str(e)}")
        raise


def extract_orders_from_postgres(**context):
    """
    Ekstrak data order dari PostgreSQL.
    
    Filter: Order yang di-update dalam 1 hari terakhir.
    Output: Push ke XCom dengan key 'orders_data'.
    """
    try:
        logging.info("Mulai ekstraksi orders dari PostgreSQL")
        
        # Koneksi ke PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Query ekstraksi
        query = """
            SELECT order_id, customer_id, product_id, quantity, 
                   total_amount, order_status, order_date, updated_at
            FROM raw_data.orders
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Konversi ke list of dictionaries
        orders_data = [dict(zip(columns, row)) for row in rows]
        
        logging.info(f"Berhasil ekstrak {len(orders_data)} orders")
        
        # Push ke XCom
        context['task_instance'].xcom_push(key='orders_data', value=orders_data)
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat ekstrak orders: {str(e)}")
        raise

# Transform & Load
def transform_and_load_customers(**context):
    """
    Transform data customer dan load ke MySQL.
    
    Transformasi:
    - Phone: Format ke (XXX) XXX-XXXX
    - State: Konversi ke UPPERCASE
    
    Loading: UPSERT ke dim_customers
    """
    try:
        logging.info("Mulai transform dan load customers")
        
        # Pull data dari XCom
        customers = context['task_instance'].xcom_pull(
            task_ids='extract_customers',
            key='customers_data'
        )
        
        if not customers:
            logging.warning("Tidak ada data customers untuk di-load")
            return
        
        # Transform data
        for customer in customers:
            if customer.get('phone'):
                # Hapus karakter non-digit
                digits = re.sub(r'\D', '', str(customer['phone']))
                if len(digits) >= 10:
                    customer['phone'] = f"({digits[:3]}) {digits[3:6]}-{digits[6:10]}"
            
            # Uppercase state
            if customer.get('state'):
                customer['state'] = customer['state'].upper()
        
        # Load ke MySQL
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # UPSERT query
        insert_query = """
            INSERT INTO dim_customers 
            (customer_id, customer_name, email, phone, address, 
             city, state, zip_code, country, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                email = VALUES(email),
                phone = VALUES(phone),
                address = VALUES(address),
                city = VALUES(city),
                state = VALUES(state),
                zip_code = VALUES(zip_code),
                country = VALUES(country),
                updated_at = VALUES(updated_at)
        """
        
        # Execute batch insert
        for customer in customers:
            cursor.execute(insert_query, (
                customer['customer_id'],
                customer['customer_name'],
                customer['email'],
                customer['phone'],
                customer['address'],
                customer['city'],
                customer['state'],
                customer['zip_code'],
                customer['country'],
                customer['updated_at']
            ))
        
        conn.commit()
        logging.info(f"Berhasil load {len(customers)} customers ke MySQL")
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat transform dan load customers: {str(e)}")
        raise


def transform_and_load_products(**context):
    """
    Transform data product dan load ke MySQL.
    
    Transformasi:
    - Profit Margin: Hitung ((price - cost) / price) * 100
    - Category: Konversi ke Title Case
    
    Loading: UPSERT ke dim_products
    """
    try:
        logging.info("Mulai transform dan load products")
        
        # Pull data dari XCom
        products = context['task_instance'].xcom_pull(
            task_ids='extract_products',
            key='products_data'
        )
        
        if not products:
            logging.warning("Tidak ada data products untuk di-load")
            return
        
        # Transform data
        for product in products:
            # Hitung profit margin
            price = float(product.get('price', 0))
            cost = float(product.get('cost', 0))
            if price > 0:
                product['profit_margin'] = ((price - cost) / price) * 100
            else:
                product['profit_margin'] = 0
            
            # Title Case category
            if product.get('category'):
                product['category'] = product['category'].title()
        
        # Load ke MySQL
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # UPSERT query
        insert_query = """
            INSERT INTO dim_products 
            (product_id, product_name, category, price, cost, 
             profit_margin, stock_quantity, supplier_name, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                category = VALUES(category),
                price = VALUES(price),
                cost = VALUES(cost),
                profit_margin = VALUES(profit_margin),
                stock_quantity = VALUES(stock_quantity),
                supplier_name = VALUES(supplier_name),
                updated_at = VALUES(updated_at)
        """
        
        # Execute batch insert
        for product in products:
            cursor.execute(insert_query, (
                product['product_id'],
                product['product_name'],
                product['category'],
                product['price'],
                product['cost'],
                product['profit_margin'],
                product['stock_quantity'],
                product['supplier_name'],
                product['updated_at']
            ))
        
        conn.commit()
        logging.info(f"Berhasil load {len(products)} products ke MySQL")
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat transform dan load products: {str(e)}")
        raise


def transform_and_load_orders(**context):
    """
    Transform data order dan load ke MySQL.
    
    Transformasi:
    - Status: Konversi ke lowercase
    - Total Amount: Validasi positif (jika negatif set ke 0)
    
    Loading: UPSERT ke fact_orders
    """
    try:
        logging.info("Mulai transform dan load orders")
        
        # Pull data dari XCom
        orders = context['task_instance'].xcom_pull(
            task_ids='extract_orders',
            key='orders_data'
        )
        
        if not orders:
            logging.warning("Tidak ada data orders untuk di-load")
            return
        
        # Transform data
        for order in orders:
            # Lowercase status
            if order.get('order_status'):
                order['order_status'] = order['order_status'].lower()
            
            # Validasi total amount
            total_amount = float(order.get('total_amount', 0))
            if total_amount < 0:
                logging.warning(
                    f"Order ID {order['order_id']} memiliki total_amount negatif: {total_amount}. "
                    f"Di-set ke 0"
                )
                order['total_amount'] = 0
            else:
                order['total_amount'] = total_amount
        
        # Load ke MySQL
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # UPSERT query
        insert_query = """
            INSERT INTO fact_orders 
            (order_id, customer_id, product_id, quantity, 
             total_amount, order_status, order_date, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                customer_id = VALUES(customer_id),
                product_id = VALUES(product_id),
                quantity = VALUES(quantity),
                total_amount = VALUES(total_amount),
                order_status = VALUES(order_status),
                order_date = VALUES(order_date),
                updated_at = VALUES(updated_at)
        """
        
        # Execute batch insert
        for order in orders:
            cursor.execute(insert_query, (
                order['order_id'],
                order['customer_id'],
                order['product_id'],
                order['quantity'],
                order['total_amount'],
                order['order_status'],
                order['order_date'],
                order['updated_at']
            ))
        
        conn.commit()
        logging.info(f"Berhasil load {len(orders)} orders ke MySQL")
        
        # Tutup koneksi
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error saat transform dan load orders: {str(e)}")
        raise

# Task ekstraksi
extract_customers_task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers_from_postgres,
    dag=dag,
)

extract_products_task = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products_from_postgres,
    dag=dag,
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_from_postgres,
    dag=dag,
)

# Task transformasi dan loading
transform_load_customers_task = PythonOperator(
    task_id='transform_load_customers',
    python_callable=transform_and_load_customers,
    dag=dag,
)

transform_load_products_task = PythonOperator(
    task_id='transform_load_products',
    python_callable=transform_and_load_products,
    dag=dag,
)

transform_load_orders_task = PythonOperator(
    task_id='transform_load_orders',
    python_callable=transform_and_load_orders,
    dag=dag,
)

# Set dependencies
extract_customers_task >> transform_load_customers_task
extract_products_task >> transform_load_products_task
extract_orders_task >> transform_load_orders_task