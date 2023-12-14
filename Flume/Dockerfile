FROM openjdk:8-jre-alpine

# Set environment variables
ENV FLUME_HOME /opt/flume
ENV PATH $FLUME_HOME/bin:$PATH

# Install dependencies
RUN apk add --no-cache bash

# Download and install Flume
RUN mkdir -p $FLUME_HOME \
    && wget -qO- http://apache.mirrors.pair.com/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz \
    | tar zxvf - -C $FLUME_HOME --strip-components=1

# Add Flume configuration file
COPY flume-conf.properties $FLUME_HOME/conf/flume-conf.properties

# Default command
CMD ["flume-ng", "agent", "-n", "agent1", "-c", "/opt/flume/conf", "-f", "/opt/flume/conf/flume-conf.properties", "-Dflume.root.logger=INFO,console"]
