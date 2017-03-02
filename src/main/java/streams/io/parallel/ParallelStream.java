package streams.io.parallel;

import stream.io.Stream;

/**
 * Abstract class for parallel multi stream. Each subclass has to implement the method {@link
 * #handleParallelism(int, int)}. E.g. for distributed streams like in Flink or Spark Streaming this
 * method should be called before the serialization and thus we can save serializable settings
 * before the program is distributed over the cluster.
 */
public interface ParallelStream extends Stream {

    /**
     * Abstract method that should help handle parallelism especially in a distributed environment.
     *
     * @param instanceNumber number of this instance
     * @param copiesNumber   number of all instantiated instances
     */
    void handleParallelism(int instanceNumber, int copiesNumber);
}
