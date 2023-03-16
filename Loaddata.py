import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# ParDo Class for Parallel processing by applying user defined transformations
class scrip_val(beam.DoFn):
    def process(self, element):
        try:
            line = element.split('"')
            if line[9] == 'BUY':
                tp = line[3] + ',' + line[11].replace(',', '')
            else:
                tp = line[3] + ',-' + line[11].replace(',', '')
            tp = tp.split()
            return tp
        except:
            logging.info('Some error occured')

# Entry run method for triggering pipeline @property
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://gcp-demobucket/bulktest.csv',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args()


  # Function to SUM grouped elements
    def sum_groups(word_ones):
        (word, ones) = word_ones
        return word + ',' + str(sum(ones))

    def parse_method(string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('SYMBOL', 'BUY_SELL_QTY'),
                values))
        return row

    # Main Pipeline

    with beam.Pipeline(options = PipelineOptions(pipeline_args)) as p:
        lines = p | 'read' >> ReadFromText(known_args.input, skip_header_lines=1)
        counts = (
                lines
                | 'Get required tuple' >> beam.ParDo(scrip_val())
                | 'PairWithValue' >> beam.Map(lambda x: (x.split(',')[0], int(x.split(',')[1])))
                | 'Group by key' >> beam.GroupByKey()
                | 'Sum Group' >> beam.Map(sum_groups)
                | 'To String' >> beam.Map(lambda s: str(s))
                | 'String To BigQueryRow' >> beam.Map(lambda s: parse_method(s))
        )

        # Write to BigQuery
        counts | 'Write to BigQuery' >> beam.io.Write(
                                                  beam.io.WriteToBigQuery(
                                                                            'batach_data',
                                                                            dataset='dataflow_demo',
                                                                            project='GCPProject',
                                                                            schema='SYMBOL:STRING,BUY_SELL_QTY:INTEGER',
                                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                                        )
                                                     )

# Trigger entry function here
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()