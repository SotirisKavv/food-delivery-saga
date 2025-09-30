FROM confluentinc/cp-kafka:7.5.0

# Copy configuration files
COPY topics.conf /usr/local/etc/topics.conf
COPY --chmod=755 setup-topics.sh /usr/local/bin/setup-topics.sh

ENTRYPOINT ["/usr/local/bin/setup-topics.sh"]