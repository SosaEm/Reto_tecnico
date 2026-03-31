import pandas as pd
from sqlalchemy import create_engine

DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "codeable"

def get_engine():
    return create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

def load_data_to_mysql(df):
    print(f"DEBUG: Intentando cargar {len(df)} filas a MySQL...")
    if df.empty:
        print("DEBUG: El DataFrame está vacío, cancelando carga.")
        return
    engine = get_engine()
    
    try:
        # Payment Methods
        if 'payment_method' in df.columns:
            methods = pd.DataFrame(df['payment_method'].unique(), columns=['method_name'])
            methods.to_sql('dim_payment_methods', engine, if_exists='append', index=False)

        # Get IDs
        query = "SELECT payment_method_id, method_name FROM dim_payment_methods"
        df_methods_db = pd.read_sql(query, engine)
        
        # Join DataFrames
        df_fact = df.merge(df_methods_db, left_on='payment_method', right_on='method_name', how='left')

        # Dim Time 
        df_fact['timestamp'] = pd.to_datetime(df_fact['timestamp'])
        df_fact['time_id'] = df_fact['timestamp'].dt.strftime('%Y%m%d%H%M')

        # Insert Dims
        df[['user_id', 'country_code']].drop_duplicates().to_sql('dim_users', engine, if_exists='append', index=False)
        df[['merchant_id']].drop_duplicates().to_sql('dim_merchants', engine, if_exists='append', index=False)

        # Fact Table
        cols_fact = [
            'transaction_id', 'user_id', 'merchant_id', 
            'time_id', 'payment_method_id', 'amount', 'currency', 'status'
        ]
        df_fact[cols_fact].to_sql('fact_transactions', engine, if_exists='append', index=False)
        
        print("Carga completa: Hechos y Dimensiones (incluyendo Payment Methods) sincronizados.")

    except Exception as e:
        print(f"Nota: {e}") 