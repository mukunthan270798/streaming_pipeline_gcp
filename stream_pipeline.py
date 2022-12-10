# -*- coding: utf-8 -*-
"""
Created on Fri Dec  9 12:57:23 2022

@author: Mukunthan
"""
'''
This apache beam pipeline has the following Ptransforms
1.Read messages from a pub-sub subscription
2.Removing the newline characters from the data
3.Split into columns
4.Providing a custom timestamp from our data for windowing operations, the default timestamp used would be the one provided by dataflow
5.Provide windowing details, we are using a tumbling windows of 10 second, the data will be processed for every 10 seconds
6.Converting the data into json format to pass it in the write to bigquery function
7.Writing the simulated realtime data into bigquery


'''

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window

def custom_timestamp(collection):
    timestamp_value = collection[9]
    return beam.window.TimestampedValue(collection, int(timestamp_value))
    
def to_json(val_list):
    json_data = {
        "index":val_list[0],
        "date":val_list[1],
        "open":val_list[2],
        "high":val_list[3],
        "low":val_list[4],
        "close":val_list[5],
        "adj_close":val_list[6],
        "volume":val_list[7],
        "closeUSD":val_list[8],
        "event_time":val_list[9]
        }
    return json_data
 
def run(input_subscription, table_name,pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, streaming=True,
                                       save_main_session=True)
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)
    project_name = pipeline_args.project
    bigquery_table = project_name+":"+table_name
    table_schema = 'index:STRING,date:STRING,open:STRING,high:STRING,low:STRING,close:STRING,adj_close:STRING,volume:STRING,closeUSD:STRING,event_time:STRING'
    bigquery_stream = (
                        p
                        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
 #                       |"Write to pub sub" >> beam.io.WriteToPubSub('projects/project-name/topics/stock_market_data')
                        | "Remove additional characters" >> beam.Map(lambda data : (data.rstrip().lstrip()))
                        | "split into columns" >> beam.Map(lambda row : row.decode("utf8").split(','))
                        | "Apply custom timestamp" >> beam.Map(custom_timestamp)
                        | "Apply window" >> beam.WindowInto(window.FixedWindows(10))
 #                       |"output element" >> beam.Map(lambda row : print(row))
                        | "Convert into json" >> beam.Map(to_json) 
                        | "Write to Bigquery" >> beam.io.WriteToBigQuery(
                           bigquery_table, schema = table_schema,
                            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
                     )
        )
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription")
    parser.add_argument("--table_name")
    
    known_args,pipeline_args = parser.parse_known_args()
    subscription_path = "projects/project-name/subscriptions/"+known_args.input_subscription   
    
    run(subscription_path,known_args.table_name,pipeline_args)