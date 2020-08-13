#!/usr/bin/env python3

import sys
import time
import argparse
import json
import six

from google.cloud import automl
from decimal import Decimal
from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.cloud.language_v1 import types
from google.cloud import bigquery
from google.cloud import translate_v2 as translate

def query(id, client):
    job_config = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "INT64", id)
            ]   
    )
    sql = """
        SELECT *
        FROM `webeye-internal-test.cloud_test.origin`
        WHERE id = @id
        """
    query_job = client.query(sql, job_config=job_config) 

    for row in query_job:
        id = row["id"]
        country = row["country"]
        comment = row["comments"]
        pub_time = row["pub_time"]

    comment_data = {}
    comment_data["id"] = id
    comment_data["country"] = country
    comment_data["comment"] = comment
    comment_data["pub_time"] = str(pub_time)
    
    return comment_data

def inline_text_payload(content):
    return {'text_snippet': {'content': content, 'mime_type': 'text/plain'} }

def classification_analyze(client, text):
    project_id = "839062387451"
    model_id = "TCN2731224266790928384"

    parser = argparse.ArgumentParser()
    parser.add_argument("--text_content", default=text)
    args = parser.parse_args()

    payload = inline_text_payload(args.text_content)
    model_full_id = prediction_client.model_path(project_id, "us-central1", model_id)
            
    response = prediction_client.predict(model_full_id, payload)

    topic = response.payload[0].display_name
    score = format(Decimal(response.payload[0].classification.score).quantize(Decimal('0.00')))    

    classification_data = {}
    classification_data["classification_topic"] = topic
    classification_data["classification_score"] = score

    return classification_data

# 情感分析函数
def sentiment_analyze(client, text):
    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT
    document = {"content": text, "type": type_, "language": "en"}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8
    annotations = nlp_client.analyze_sentiment(document=document, encoding_type=encoding_type)

    score = annotations.document_sentiment.score
    magnitude = annotations.document_sentiment.magnitude
    
    sentiment_data = {}
    sentiment_data["sentiment_score"] = score
    sentiment_data["sentiment_magnitude"] = magnitude
    return sentiment_data

# 文本翻译函数
def translate_text(client, text):
    result = translate_client.translate(text, target_language="en")
    
    translation_text = result['translatedText']
    source_language = result['detectedSourceLanguage']

    translate_data = {}
    translate_data["translation_text"] = translation_text
    translate_data["source_language"] = source_language

    #print(translate_data)
    return translate_data

# 实体情感分析函数
def entity_sentiment_analyze(client, text):
    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT
    document = {"content": text, "type": type_, "language": "en"}
    
    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8

    response = client.analyze_entity_sentiment(document, encoding_type=encoding_type)

    list_entity_name = []
    list_entity_type = []
    list_entity_salience = []
    list_sentiment_score = []
    list_sentiment_magnitude = []
    for entity in response.entities:
        entity_name = entity.name

        # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
        entity_type = enums.Entity.Type(entity.type).name

        # Get the salience score associated with the entity in the [0, 1.0] range
        salience_score = entity.salience

        # Get the aggregate sentiment expressed for this entity in the provided document.
        sentiment = entity.sentiment
        entity_sentiment_score = sentiment.score
        entity_sentiment_magnitude = sentiment.magnitude

        list_entity_name.append(entity_name)
        list_entity_type.append(entity_type)
        list_entity_salience.append(salience_score)
        list_sentiment_score.append(entity_sentiment_score)
        list_sentiment_magnitude.append(entity_sentiment_magnitude)

        entity_data = {}
        entity_data["entity_name"] = list_entity_name
        entity_data["entity_type"] = list_entity_type
        entity_data["salience_score"] = list_entity_salience
        entity_data["entity_sentiment_score"] = list_sentiment_score
        entity_data["entity_sentiment_magnitude"] = list_sentiment_magnitude
    
    return entity_data

# 转换为json格式的函数
def convert_json(text):
    return json.dumps(text)

# 本地json文件加载到BigQuery的函数
def import_to_bq(client, dataset_id, table_id, filename):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()

    # 定义导入BigQuery时文件的格式是json
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    # 导入BigQuery中表格的schema
    job_config.schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comment", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pub_time", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("translation_text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("classification_topic", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("classification_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_magnitude", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("entity_name", "STRING", mode="REPEATED"),
        bigquery.SchemaField("entity_type", "STRING", mode="REPEATED"),
        bigquery.SchemaField("salience_score", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("entity_sentiment_score", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("entity_sentiment_magnitude", "FLOAT", mode="REPEATED"),
    ]

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    return 0

if __name__ == '__main__':
    
    # 创建一个json文件
    g = open('analyze_results.json', 'a')

    # 初始化BigQuery的客户端实例
    bq_client = bigquery.Client()
    
    # 初始化automl prediction的客户端实例
    prediction_client = automl.PredictionServiceClient()

    # 初始化翻译的客户端实例
    translate_client = translate.Client()

    #初始化nlp的客户端实例
    nlp_client = language_v1.LanguageServiceClient()

    try:
        # 循环
        for id in range(1, 10):
            # 从BigQuery的原始表格中抽取出评论的文本，按每一条取出
            comment_data = query(id, bq_client)

            # 取出评论的内容
            comment = comment_data["comment"]

            # 翻译评论内容，统一翻译成英文
            translate_data = translate_text(translate_client, comment)

            # 评论分类预测
            classification_data = classification_analyze(prediction_client, translate_data["translation_text"])

            # 分析整个评论的情感
            sentiment_data = sentiment_analyze(nlp_client, translate_data["translation_text"])

            # 分析实体情感
            entity_data = entity_sentiment_analyze(nlp_client, translate_data["translation_text"])

            # 合并结果
            analyze_results = {**comment_data, **translate_data, **classification_data, **sentiment_data, **entity_data}
            
            # 将结果转换成json格式
            analyze_results_json = convert_json(analyze_results)
            
            # 写入本地的json文件中
            g.write(analyze_results_json + '\n')

            time.sleep(0.5)
        print('json file is ready!')
    except Exception as err:
        print("Error {}".format(err))
    finally:
        g.close()

        # 将本地生成的json格式数据，导入至BigQuery的analyze_data_all表中
        import_to_bq(bq_client, "cloud_test", "analyze_data_all", "analyze_results.json")
        print("finished!")