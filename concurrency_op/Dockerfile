FROM golang:1.18

ADD codes /go/src/github.com/jingtaozhang18/codes
ADD entrypoint.sh /go

ENV MYSQL_USR_NAME=root
ENV MYSQL_USR_PASSWD=nopasswd
ENV MYSQL_SERVER_URL=mysql
ENV MYSQL_SERVER_PORT=3306
ENV MYSQL_DATABASE_NAME=db_world

ENTRYPOINT [ "bash", "/go/entrypoint.sh"]
