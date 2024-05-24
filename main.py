import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


#sets up the pipeline options 
option = PipelineOptions()
gcp_options = option.view_as(GoogleCloudOptions)
gcp_options.project = 'datavisprojects'
gcp_options.region = 'us-central1'
gcp_options.job_name = 'pubsub-to-bigquery'
gcp_options.staging_location = 'gs://buchet_number_1/staging'
gcp_options.temp_location = 'gs://buchet_number_1/temp'
option.view_as(PipelineOptions).streaming = True



