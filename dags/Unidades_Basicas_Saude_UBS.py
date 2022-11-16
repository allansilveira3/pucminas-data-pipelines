from ast import Return
from asyncio import Task
from operator import index
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

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
        df = pd.read_csv(URL,sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        df = df(
            .withColumn("UF",f.regexp_replace("UF","11","Rondonia").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","12","Acre").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","13","Amazonas").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","14","Roraima").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","15","Para").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","16","Amapa").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","17","Tocantins").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","21","Maranhao").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","22","Piaui").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","23","Ceara").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","24","Rio_Grande_do_Norte").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","25","Paraiba").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","26","Pernambuco").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","27","Alagoas").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","28","Sergipe").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","29","Bahia").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","31","Minas_Gerais").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","32","Espirito_Santo").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","33","Rio_de_Janeiro").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","35","Sao_Paulo").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","41","Parana").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","42","Santa_Catarina").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","43","Rio_Grande_do_Sul").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","50","Mato_Grosso_do_Sul").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","51","Mato_Grosso").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","52","Goias").cast(StringType()))
            .withColumn("UF",f.regexp_replace("UF","53","Distrito_Federal").cast(StringType()))
            
        )
        df.show(n=10)
        return NOME_DO_ARQUIVO

    @task #Aquisção e tratamento do dado
    def quantidade_por_estado(nome_do_arquivo):
        NOME_TABELA = "/tmp/ubs_por_estado.csv"
        df = pd.read_csv(nome_do_arquivo,sep=';')
        res = df.groupby(['UF']).agg({
            "CNES":"count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA
    
    
    @task #Aquisção e tratamento do dado
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
    
    inicio
   
    
    inicio >> ing >>indicador_1 >> indicador_2 >> fim 

execucao = trabalho_final_dag()
