FROM mysql:8.0.30

ADD sqls /sqls

ENV MYSQL_USR_NAME=root
ENV MYSQL_USR_PASSWD=nopasswd
ENV MYSQL_SERVER_URL=mysql
ENV MYSQL_SERVER_PORT=3306

ADD entrypoint.sh /

ENTRYPOINT [ "bash", "/entrypoint.sh"]
