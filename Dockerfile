# 构建阶段
FROM golang:1.26-alpine AS builder

WORKDIR /app

# 配置 Go 代理, 加速国内下载
ENV GOPROXY=https://goproxy.cn,direct

# 复制 go.mod 和 go.sum (如果有的话)
COPY go.mod go.sum* ./
RUN go mod download

# 复制所有源码
COPY . .

# 编译成名为 nanojob 的可执行文件
RUN go build -o /nanojob ./cmd/nanojob

# 运行阶段 (极其轻量)
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /nanojob .
# 前端静态页面
COPY --from=builder /app/ui ./ui
# 配置文件 (docker-compose 用环境变量覆盖 localhost 默认值, 指向 mysql/redis 服务名)
COPY --from=builder /app/conf.json ./conf.json

EXPOSE 8080

CMD ["./nanojob", "-c", "conf.json"]
