# 使用官方 Go 镜像作为构建环境
FROM golang:1.20-alpine AS builder

# 设置工作目录
WORKDIR /app

# 配置 Go 代理，加速国内下载
ENV GOPROXY=https://goproxy.cn,direct

# 复制 go.mod 和 go.sum (如果有的话)
COPY go.mod go.sum* ./
RUN go mod download

# 复制所有源码
COPY . .

# 编译成名为 nanojob 的可执行文件
RUN go build -o /nanojob ./cmd/nanojob

# 使用极其轻量级的 alpine 作为最终运行镜像
FROM alpine:latest

WORKDIR /root/

# 从 builder 阶段把编译好的执行文件拷贝过来
COPY --from=builder /nanojob .
# 把前端静态页面拷贝过来
COPY --from=builder /app/ui ./ui

# 暴露 8080 端口
EXPOSE 8080

# 启动命令，默认连接名为 nanojob-etcd 的容器
CMD ["./nanojob", "-etcd=nanojob-etcd:2379", "-port=8080"]
