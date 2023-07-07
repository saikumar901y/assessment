import pandas as pd
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def calculate_values(data):
    # Define the join logic
    joined_data = data[0].merge(data[1], on=['legal_entity', 'counterparty'], how='left')

    # Calculate tier, max rating, and sum of values based on status
    joined_data['tier'] = joined_data['tier_x']
    joined_data['max_rating'] = joined_data.groupby('counterparty')['rating'].transform('max')
    joined_data['sum_arap'] = joined_data.loc[joined_data['status'] == 'ARAP', 'value'].sum()
    joined_data['sum_accr'] = joined_data.loc[joined_data['status'] == 'ACCR', 'value'].sum()

    # Create new records for total values
    total_record = joined_data.groupby('legal_entity').sum().reset_index()
    total_record['counterparty'] = 'Total'
    total_record['tier'] = 'Total'
    total_record['max_rating'] = total_record['rating']
    total_record = total_record[['legal_entity', 'counterparty', 'tier', 'max_rating', 'sum_arap', 'sum_accr']]

    # Concatenate the original data with the total records
    final_data = pd.concat([joined_data, total_record], ignore_index=True)

    return final_data


class JoinTransform(beam.DoFn):
    def process(self, element):
        dataset1, dataset2 = element
        joined_data = calculate_values([dataset1, dataset2])
        yield joined_data.values.tolist()

def run_pipeline(dataset1_path, dataset2_path, output_path):
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        dataset1 = (
            pipeline
            | 'Read Dataset1' >> ReadFromText(dataset1_path)
            | 'Split Dataset1' >> beam.Map(lambda x: x.split(','))
        )
        dataset2 = (
            pipeline
            | 'Read Dataset2' >> ReadFromText(dataset2_path)
            | 'Split Dataset2' >> beam.Map(lambda x: x.split(','))
        )
        joined_data = (
            (dataset1, dataset2)
            | 'Join Datasets' >> beam.ParDo(JoinTransform())
        )
        joined_data | 'Write Output' >> WriteToText(output_path, num_shards=1)


if __name__ == '__main__':
    dataset1_path = 'path/to/dataset1.csv'
    dataset2_path = 'path/to/dataset2.csv'
    output_path = 'path/to/output.csv'
    run_pipeline(dataset1_path, dataset2_path, output_path)
