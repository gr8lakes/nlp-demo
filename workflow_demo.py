#!/usr/bin/env python3

import sys
import time
import argparse
import json
import six

from google.cloud import storage
from google.cloud import automl
from decimal import Decimal
from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.cloud.language_v1 import types
from google.cloud import bigquery
from google.cloud import translate

# Assuming all comments will enter the data warehouse first.
# Query the comment content from the BigQuery table.
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

# Translate the comment to English via Translate API v3.
def translate_text(client, project_id, text):
    parent = client.location_path(project_id, "global")
    response = client.translate_text(
        parent=parent,
        contents=[text],
        mime_type="text/plain",  # mime types: text/plain, text/html
        target_language_code="en",
    )
    for translation in response.translations:
        translation_text = translation.translated_text
        source_language = translation.detected_language_code

    translate_data = {}
    translate_data["translation_text"] = translation_text
    translate_data["source_language"] = source_language
    return translate_data

def inline_text_payload(content):
    return {'text_snippet': {'content': content, 'mime_type': 'text/plain'} }

# Use the custom model to predict the comment content.
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

# Use NLP API to analyze the comment contents.
def annotateText(client, text):
    document = { "content": text, "type": "PLAIN_TEXT", "language": "en" }
    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8

    features = {
        "extract_document_sentiment": True,
        "extract_entity_sentiment": True
    }
    response = client.annotate_text(document=document, features=features, encoding_type=encoding_type)

    data = {}

    # document sentiment
    score = response.document_sentiment.score
    magnitude = response.document_sentiment.magnitude

    data["sentiment_score"] = score
    data["sentiment_magnitude"] = magnitude

    # entity & entity sentiment
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

        data["entity_name"] = list_entity_name
        data["entity_type"] = list_entity_type
        data["entity_salience"] = list_entity_salience
        data["entity_sentiment_score"] = list_sentiment_score
        data["entity_sentiment_magnitude"] = list_sentiment_magnitude
    
    return data

def convert_json(text):
    return json.dumps(text)

def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    # Instantiates a client
    storage_client = storage.Client()

    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

# Import all the returned results into the new table of BigQuery.
def import_to_bq(client, dataset_id, table_id, filename):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
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
        bigquery.SchemaField("entity_salience", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("entity_sentiment_score", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("entity_sentiment_magnitude", "FLOAT", mode="REPEATED"),
    ]

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    # Waits for table load to complete.
    job.result()

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    return 0

if __name__ == '__main__':
    g = open('analyze_results.json', 'a')
    bq_client = bigquery.Client()
    prediction_client = automl.PredictionServiceClient()
    translate_client = translate.TranslationServiceClient()
    nlp_client = language_v1.LanguageServiceClient()
    project_id = 'webeye-internal-test'

    try:
        for id in range(1, 10):
            comment_data = query(id, bq_client)
            comment = comment_data["comment"]

            translate_data = translate_text(translate_client, project_id, comment)
            classification_data = classification_analyze(prediction_client, translate_data["translation_text"])
            document_entities_data = annotateText(nlp_client, translate_data["translation_text"])
            analyze_results = {**comment_data, **translate_data, **classification_data, **document_entities_data} 
            print(analyze_results)
            
            analyze_results_json = convert_json(analyze_results)
            g.write(analyze_results_json + '\n')
            time.sleep(0.5)
        print('json file is ready!')
    except Exception as err:
        print("Error {}".format(err))
    finally:
        g.close()
        
        # upload file to gcs
        upload_to_bucket("test-chent-we/shein", "analyze_results.json", "analyze_results.json")
        
        #print("Import to BigQuery Table!")
        #import_to_bq(bq_client, "cloud_test", "analyze_data_all", "analyze_results.json")
        print("finished!")
