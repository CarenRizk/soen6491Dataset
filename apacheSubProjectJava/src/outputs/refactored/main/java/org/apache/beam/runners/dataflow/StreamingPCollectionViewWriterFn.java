package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;

// Optimized by LLM: Removed unused import statements
// import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
// import org.apache.beam.sdk.values.PCollection;

/**
 * StreamingPCollectionViewWriterFn is a DoFn that serves as a marker class for writing
 * to a PCollectionView. It is not intended to be executed directly.
 *
 * @param <T> the type of elements processed by this function
 */
public class StreamingPCollectionViewWriterFn<T> extends DoFn<Iterable<T>, T> {

    private final PCollectionView<?> view; // Optimized by LLM: Made field final
    private final Coder<T> dataCoder; // Optimized by LLM: Made field final

    // Optimized by LLM: Changed constructor visibility to private
    private StreamingPCollectionViewWriterFn(PCollectionView<?> view, Coder<T> dataCoder) {
      this.view = view;
      this.dataCoder = dataCoder;
    }

    /**
     * Creates a new instance of StreamingPCollectionViewWriterFn.
     *
     * @param view the PCollectionView to write to
     * @param dataCoder the Coder for the data type
     * @param <T> the type of elements processed by this function
     * @return a new instance of StreamingPCollectionViewWriterFn
     */
    public static <T> StreamingPCollectionViewWriterFn<T> create(
        PCollectionView<?> view, Coder<T> dataCoder) {
      return new StreamingPCollectionViewWriterFn<>(view, dataCoder);
    }

    /**
     * Gets the PCollectionView associated with this function.
     *
     * @return the PCollectionView
     */
    public PCollectionView<?> getView() {
      return view;
    }

    /**
     * Gets the Coder associated with the data type.
     *
     * @return the Coder for the data type
     */
    public Coder<T> getDataCoder() {
      return dataCoder;
    }

    // Optimized by LLM: Improved exception message in processElement
    @ProcessElement
    public void processElement(ProcessContext c) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is a marker class only and should never be executed.", getClass().getName()));
    }
}