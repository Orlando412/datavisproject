import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.auth.exceptions import DefaultCredentialsError


def run_pipeline():
    #error handling for the pipeline
    try:
        with beam.Pipeline() as p:
            
            #sets up the pipeline options 
            option = PipelineOptions()
            gcp_options = option.view_as(GoogleCloudOptions)
            gcp_options.project = 'datavisprojects'
            gcp_options.region = 'us-central1'
            gcp_options.job_name = 'pubsub-to-bigquery'
            gcp_options.staging_location = 'gs://buchet_number_1/staging'
            gcp_options.temp_location = 'gs://buchet_number_1/temp'

            # Enable streaming mode
            standard_options = option.view_as(StandardOptions)
            standard_options.streaming = True
            #option.view_as(PipelineOptions).streaming = True

            #define the pipeline
            p = beam.Pipeline(options=option)

            #define the pub / sub topic 
            topic_path = 'projects/datavisprojects/topics/visproject'

            #define the bigquery table
            table_specs = 'datavisprojects:dataset_for_buchet_1.buchet_table'

            #define the schema for the bigquery
            table_schema = 'data:STRING'

            #Create the pipeline
            (p 
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=topic_path)
            | 'Transform Data' >> beam.Map(lambda x: {'data': x.decode('utf-8')})
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                    table_specs,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )

            #runs the pipeline
            res = p.run()
            res.wait_until_finish()

            pass

    except DefaultCredentialsError as e:
        print(f"Error: {e}")
        print("Ensure that the GOOGLE_APPLICATION_CREDENTIALS environment variable is set correctly.")
        print("Refer to https://cloud.google.com/docs/authentication/external/set-up-adc for more information.")


if __name__ == "__main__":
    run_pipeline()