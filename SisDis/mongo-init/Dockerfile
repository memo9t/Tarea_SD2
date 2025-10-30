FROM mongo:6.0

COPY bdd.json /bdd.json
COPY init.sh /init.sh
RUN chmod +x /init.sh

CMD ["/init.sh"]
