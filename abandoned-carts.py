"""A abandoned-carts workflow."""

from __future__ import absolute_import

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class JsonCoder(object):
    "Parsing JSON objects."
    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)

def run(argv=None):
  """pipeline abandoned-carts"""
  """Definicao argumentos de entrada do pipeline"""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Arquivo de entrada para o processamento.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Arquivo de saida com o resultado do processamento.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  """Le arquivo"""
  lines = p | 'le_arquivo' >> ReadFromText(known_args.input, coder=JsonCoder())

  """Filtra todos os clientes que fizeram checkout, e traz o customer como chave"""
  filtros_checkout = (lines
                | 'filtra_pagina_checkout' >> beam.Filter(lambda x: (x['page'] == 'checkout'))
                | 'gera_chave_customer' >> beam.Map(lambda x: (x['customer'],x))
      )
  
  """Traz o customer como chave para todos os registros"""
  filtros_todos = (lines
                | 'gera_chave_customer_geral' >> beam.Map(lambda x: (x['customer'],x))
      )

  """Faz o left join trazendo todos os registros que nao tem checkout"""
  results = ((filtros_todos, filtros_checkout)
         | 'left_join' >> beam.CoGroupByKey()
         | 'filtro_sem_checkout' >> beam.Filter(lambda x: not(x[1][1]))
         | 'formata_saida' >> beam.Map(lambda x: (x[1][0][-1]))
      )

  """Grava o resultado num arquivo json"""
  results | 'grava_resultado' >> WriteToText(known_args.output, coder=JsonCoder(), file_name_suffix='.json')
  
  result_run = p.run()
  result_run.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()