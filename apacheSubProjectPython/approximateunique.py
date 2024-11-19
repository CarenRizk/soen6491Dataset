



def approximateunique(test=None):
  import random

  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    data = list(range(1000))
    random.shuffle(data)
    result = (
        pipeline
        | 'create' >> beam.Create(data)
        | 'get_estimate' >> beam.ApproximateUnique.Globally(size=16)
        | beam.Map(print))
    if test:
      test(result)
