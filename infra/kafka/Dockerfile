FROM confluentinc/cp-kafka:7.6.1

COPY init-topics.sh /usr/local/bin/init-topics.sh

USER root
RUN chmod +x /usr/local/bin/init-topics.sh
USER appuser

ENTRYPOINT ["/usr/local/bin/init-topics.sh"]