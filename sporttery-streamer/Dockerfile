FROM g0gb7qq6rcc7mh.xuanyuan.run/library/alpine:3.20
WORKDIR /app

ENV TZ=Asia/Shanghai

RUN sed -i 's#https://dl-cdn.alpinelinux.org/alpine#https://mirrors.aliyun.com/alpine#g' /etc/apk/repositories \
 && apk add --no-cache ca-certificates tzdata

COPY dist/sporttery-streamer /app/app

RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S nonroot -G nonroot

USER nonroot:nonroot

ENTRYPOINT ["/app/app"]
