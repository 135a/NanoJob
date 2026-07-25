# 编译阶段：利用官方 Go 镜像进行源码编译
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
# 禁用 CGO 并编译为完全静态链接的 Linux 二进制文件
RUN CGO_ENABLED=0 GOOS=linux go build -o nanojob ./cmd/nanojob/main.go

# 运行阶段：采用极简的 alpine 镜像，剥离编译环境，使得最终镜像体积通常小于 20MB
FROM alpine:latest
WORKDIR /app

# 从 builder 阶段复制编译好的二进制文件
COPY --from=builder /app/nanojob .

# 赋予可执行权限
RUN chmod +x ./nanojob

# 声明对外暴露 8080 端口
EXPOSE 8080

# 设置容器启动的默认指令，后续可通过 K8s args 覆盖或补充参数
ENTRYPOINT ["./nanojob"]
