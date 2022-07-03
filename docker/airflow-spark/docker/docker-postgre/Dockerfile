FROM postgres:13
COPY ./init-user-db.sh /docker-entrypoint-initdb.d/
CMD ["docker-entrypoint.sh", "postgres"]
