package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
   * A marker {@link DoFn} for writing the contents of a {@link PCollection} to a streaming {@link
   * PCollectionView} backend implementation.
   */
  public class StreamingPCollectionViewWriterFn<T> extends DoFn<Iterable<T>, T> {

    private final PCollectionView<?> view;
    private final Coder<T> dataCoder;

    public static <T> StreamingPCollectionViewWriterFn<T> create(
        PCollectionView<?> view, Coder<T> dataCoder) {
      return new StreamingPCollectionViewWriterFn<>(view, dataCoder);
    }

    private StreamingPCollectionViewWriterFn(PCollectionView<?> view, Coder<T> dataCoder) {
      this.view = view;
      this.dataCoder = dataCoder;
    }

    public PCollectionView<?> getView() {
      return view;
    }

    public Coder<T> getDataCoder() {
      return dataCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow w) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is a marker class only and should never be executed.", getClass().getName()));
    }
  }