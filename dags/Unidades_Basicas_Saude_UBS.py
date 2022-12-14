from ast import Return
from asyncio import Task
from operator import index
import pandas as pd
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime


URL = "https://github.com/allansilveira3/pucminas-data-pipelines/blob/main/Unidades_Basicas_Saude_UBS.csv"

default_args = {
    'owner':"Allan",
    'depends_on_past': False,
    'start_date': datetime(2022,11,16)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['UBS'])
def trabalho_final_dag():

    inicio = DummyOperator(task_id='Inicio')

    @task #Aquisção e tratamento do dado
    def ingestao():
        df = pd.read_csv(URL,sep=";",error_bad_lines=False)
        df.to_csv(index=False, header=True, sep=";")
        return df

    @task #count por estado
    def quantidade_por_estado(nome_do_arquivo):
        df = pd.read_csv(nome_do_arquivo,sep=';')
        res = df.groupby(['UF']).agg({
            "CNES":"count"
        }).reset_index()
        print(res)
        
       
    @task #Count rio de janeiro
    def quantidade_rj(nome_do_arquivo):
        df = pd.read_csv(nome_do_arquivo,sep=';')
        res = df.where('UF=RIO_DE_JANEIRO').select("CNES","UF","NOME")
        print(res)
        


    fim = DummyOperator(task_id="Fim")
  
    ing = ingestao()
    indicador_1 = quantidade_por_estado(ing)
    indicador_2 = quantidade_rj(ing)
    inicio >> ing >>indicador_1 >> indicador_2 >> fim 

execucao = trabalho_final_dag()

