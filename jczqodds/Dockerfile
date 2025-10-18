# ========= Build stage =========
FROM g0gb7qq6rcc7mh.xuanyuan.run/library/golang:1.22-alpine AS build
WORKDIR /app

# 国内源 + 证书
RUN sed -i 's#https://dl-cdn.alpinelinux.org/alpine#https://mirrors.aliyun.com/alpine#g' /etc/apk/repositories \
 && apk add --no-cache ca-certificates tzdata

ENV GOPROXY=https://goproxy.cn,direct
ENV GOTOOLCHAIN=auto

# 1) 先只拷 go.mod/go.sum 以便缓存依赖层
COPY go.mod go.sum ./
RUN go mod download

# 2) 再拷全部源码，并根据 import 自动补依赖
COPY . .
RUN go mod tidy   # <<< 关键：这里会把 chromedp 写入 go.mod/go.sum

# 3) 编译
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -trimpath -ldflags="-s -w" -o /out/app ./main.go

# ========= Runtime stage =========
FROM g0gb7qq6rcc7mh.xuanyuan.run/library/alpine:3.20
WORKDIR /app
ENV TZ=Asia/Shanghai
RUN sed -i 's#https://dl-cdn.alpinelinux.org/alpine#https://mirrors.aliyun.com/alpine#g' /etc/apk/repositories \
 && apk add --no-cache ca-certificates tzdata
COPY --from=build /out/app /app/app
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S nonroot -G nonroot
USER nonroot:nonroot
ENTRYPOINT ["/app/app"]
