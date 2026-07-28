package uid

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
)

var sfNode *snowflake.Node

// Init 根据指定的 machineID 初始化全局发号器
func Init(machineID int64) error {
	node, err := snowflake.NewNode(machineID)
	if err != nil {
		return err
	}
	sfNode = node
	fmt.Printf("[UID] 雪花算法初始化成功，当前机器 Worker ID: %d\n", machineID)
	return nil
}

// Generate 生成一个全局唯一 ID
func Generate() string {
	if sfNode == nil {
		panic("UID 生成器尚未初始化！")
	}
	return sfNode.Generate().String()
}
