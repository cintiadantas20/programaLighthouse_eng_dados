# Airflow imports
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

# Other imports
import psycopg2
import pandas as pd
import base64

# Lê o arquivo sqlite e extrai a tabela Order
def extract_postgres():
    conn = psycopg2.connect("host=localhost port=5432 dbname=northwind user=postgres password=postgres")
    df = pd.read_sql_query('select * from orders',conn)
    df.to_csv('output_orders.csv',index=False)
    conn.close()

# Lê o arquivo csv e junta com a tabela extraída do banco de dados
def load_to_query():
    df_order_details = pd.read_csv("order_details.csv")
    df_output_orders = pd.read_csv("output_orders.csv")
    df_total = df_order_details.merge(df_output_orders, how="left", on="order_id")
    df_total.to_csv('df_total.csv',index=False)

# Cria um banco PostgreSQL e popula com os dados da tabela que foi unida
def run_query():
    df_total = pd.read_csv("df_total.csv")
    consulta = df_total[df_total["ship_city"] == "Rio de Janeiro"]
    consulta = consulta['quantity'].sum()
    with open("count.txt", "w") as f:
        f.write(str(consulta))

# Exporta o resultado final do desafio
def export_final_answer():

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2022, 2, 7),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        ELT Diária do banco de dados de ecommerce Northwind,
        começando em 2022-02-07. 
    """

    extract_postgres_task = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres,
    )

    extract_postgres_task.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task extrai os dados do banco de dados postgres, parte de baixo do step 1 da imagem:

    ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)

    """
    )

    load_to_query_task = PythonOperator(
        task_id='load_to_query',
        python_callable=load_to_query,
    )

    load_to_query_task.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task faz load dos dados extraidos do csv e junta com os extraídos do bd para o banco de dados no step 2 da imagem:

    ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)

    """
    )
  
    run_query_task = PythonOperator(
        task_id='run_query',
        python_callable=run_query,
        op_kwargs={"copy_sql": "COPY (SELECT * FROM df_total) TO STDOUT WITH CSV HEADER"}
    )

    run_query_task.doc_md = dedent(
        """\
    #### Task Documentation
        Query em cima do banco consolidado, pegando o valor das vendas para o dia
    """
    )
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True)
    


    extract_postgres_task >> load_to_query_task >> run_query_task >> export_final_output
