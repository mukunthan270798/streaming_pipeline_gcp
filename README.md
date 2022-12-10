# streaming_pipeline_gcp
We will build a streaming data processing pipeline using dataflow which reads the messages from the pub/sub subscription and write the data to google bigquery table

## Simulating real time streaming:
  Reading data from csv file
  Append the timestamp that will serve as the event time for windowing
  Publish the event to pubsub topic
  Stream rate is hardcoded to 1 message per second
  
## The Apache beam pipeline has the following Ptransforms
1.Read messages from a pub-sub subscription
2.Removing the newline characters from the data
3.Split into columns
4.Providing a custom timestamp from our data for windowing operations, the default timestamp used would be the one provided by dataflow
5.Provide windowing details, we are using a tumbling windows of 10 second, the data will be processed for every 10 seconds
6.Converting the data into json format to pass it in the write to bigquery function
7.Writing the simulated realtime data into bigquery
