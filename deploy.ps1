Write-Host "🚀 开始一键构建并部署 NanoJob K8s 集群..." -ForegroundColor Green

Write-Host "`n[1/3] 构建 Go 引擎控制面镜像..." -ForegroundColor Cyan
docker build -t nanojob/engine:v1.0 .

Write-Host "`n[2/3] 构建 Java 兵团数据面镜像..." -ForegroundColor Cyan
Push-Location examples/java-executor
docker build -t nanojob/java-executor:v1.0 .
Pop-Location

Write-Host "`n[3/3] 向 K8s 下达全量部署指令..." -ForegroundColor Cyan
# 这一行是灵魂！只要指定目录，K8s 会一次性部署里面的所有 yaml
kubectl apply -f deploy/k8s/

Write-Host "`n✅ 所有基础设施与微服务部署指令已发送！" -ForegroundColor Green
Write-Host "--------------------------------------------------------"
Write-Host "👉 观察运行状态: kubectl get pods -w"
Write-Host "👉 部署完成后，请手动执行以下两行命令注入种子测试数据："
Write-Host "   1. 另开终端建立隧道: kubectl port-forward svc/etcd-service 2379:2379"
Write-Host "   2. 执行注入脚本:     go run ./cmd/seed/main.go"
Write-Host "--------------------------------------------------------"
