package streams.io.parallel;

import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * Abstract class for distributed multi stream. Each subclass has to implement the method {@link
 * #handleParallelism(int, int)}. E.g. for distributed streams like in Flink or Spark Streaming this
 * method should be called before the serialization and thus we can save serializable settings
 * before the program is distributed over the cluster.
 */
public abstract class ParallelMultiStream extends AbstractMultiStream implements ParallelStream {

    public ParallelMultiStream(SourceURL url) {
        super(url);
    }

    public ParallelMultiStream() {
        super();
    }
}
