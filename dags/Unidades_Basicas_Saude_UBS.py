from ast import Return
from asyncio import Task
from operator import index
import pandas as pd
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

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
        NOME_DO_ARQUIVO = "/tmp/ubs.csv"
        df = pd.read_csv(URL,sep=";",error_bad_lines=False)
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        df['UF'] = df['UF'].map({"11":"Rondonia",
        "12":"Acre",
        "13":"Amazonas",
        "14":"Roraima",
        "15":"Para",
        "16":"Amapa",
        "17":"Tocantins",
        "21":"Maranhao",
        "22":"Piaui",
        "23":"Ceara",
        "24":"Rio_Grande_do_Norte",
        "25":"Paraiba",
        "26":"Pernambuco",
        "27":"Alagoas",
        "28":"Sergipe",
        "29":"Bahia",
        "31":"Minas_Gerais",
        "32":"Espirito_Santo",
        "33":"Rio_de_Janeiro",
        "35":"Sao_Paulo",
        "41":"Parana",
        "42":"Santa_Catarina",
        "43":"Rio_Grande_do_Sul",
        "50":"Mato_Grosso_do_Sul",
        "51":"Mato_Grosso",
        "52":"Goias",
        "53":"Distrito_Federal"},na_action=None)
        df.show(n=10)
        return NOME_DO_ARQUIVO

    @task #count por estado
    def quantidade_por_estado(nome_do_arquivo):
        NOME_TABELA = "/tmp/ubs_por_estado.csv"
        df = pd.read_csv(nome_do_arquivo,sep=';')
        res = df.groupby(['UF']).agg({
            "CNES":"count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA
    
    
    @task #Count rio de janeiro
    def quantidade_rj(nome_do_arquivo):
        NOME_TABELA = "/tmp/ubs_no_rio_de_janeiro.csv"
        df = pd.read_csv(nome_do_arquivo,sep=';')
        res = df.where('UF=Rio_de_Janeiro').select("CNES","UF","NOME").show()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA


    fim = DummyOperator(task_id="Fim")
  
    ing = ingestao()
    indicador_1 = quantidade_por_estado(ing)
    indicador_2 = quantidade_rj(ing)
    inicio >> ing >>indicador_1 >> indicador_2 >> fim 

execucao = trabalho_final_dag()

