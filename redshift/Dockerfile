FROM debian:latest

RUN apt update
RUN apt install postgresql-client gettext -y

COPY docker_script.sh /docker_script.sh
COPY schema.sql /schema_template.sql
RUN chmod +x docker_script.sh

ENTRYPOINT ["/docker_script.sh"]
