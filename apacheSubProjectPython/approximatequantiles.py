



def approximatequantiles(test=None):
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    quantiles = (
        pipeline
        | 'Create data' >> beam.Create(list(range(1001)))
        | 'Compute quantiles' >> beam.ApproximateQuantiles.Globally(5)
        | beam.Map(print))
    if test:
      test(quantiles)


if __name__ == '__main__':
  approximatequantiles()
