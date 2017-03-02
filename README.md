# streams-parallel-source [![Build Status](https://travis-ci.org/alexeyegorov/streams-parallel-source.svg?branch=master)](https://travis-ci.org/alexeyegorov/streams-parallel-source)

[streams](https://sfb876.de/streams/) framework aims at providing a clean and easy-to-use Java-based platform to process streaming data. With its abstraction level, it can be used to define various processing jobs decoupled from the underlying execution engine. Packages [streams-storm](https://bitbucket.org/cbockermann/streams-storm/), [streams-spark](https://github.com/alexeyegorov/streams-spark) and [streams-flink](https://github.com/alexeyegorov/streams-flink) allow executing these jobs on top of Storm, Spark and Flink respectively.

This packages extends the ``streams`` framework with the ability to run stream sources in parallel on distributed processing frameworks mentioned above.
