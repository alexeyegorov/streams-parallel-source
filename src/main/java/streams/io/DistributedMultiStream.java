package streams.io;

import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * Abstract class for distributed multi stream. Each subclass has to implement the method {@link
 * #handleParallelism(int, int)}. E.g. for distributed streams like in Flink or Spark Streaming this
 * method should be called before the serialization and thus we can save serializable settings
 * before the program is distributed over the cluster.
 */
public abstract class DistributedMultiStream extends AbstractMultiStream implements DistributedStream {

    public DistributedMultiStream(SourceURL url) {
        super(url);
    }

    public DistributedMultiStream() {
        super();
    }
}
