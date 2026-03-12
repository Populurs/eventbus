package eventbus

import (
	"fmt"
	"strconv"
)

// EventMetadata 事件元数据，结果入库用
type EventMetadata struct {
	//EventID      string  `json:"event_id"` // UUID
	EventType   string `json:"event_type"`
	TenantID    uint32 `json:"tenant_id"`
	UserID      uint32 `json:"user_id"`
	TaskID      uint32 `json:"task_id"` // 执行任务WorkTaskID
	WorkTaskID  uint32 `json:"work_task_id"`
	CompanyID   uint32 `json:"company_id"`
	CompanyName string `json:"company_name"`

	//JobID        uint32  `json:"job_id"`          // 子任务ID——唯一标识
	//ShardID      uint32  `json:"shard_id"`        // job任务目标分片ID
	//JobType      string  `json:"job_type"`        // 任务类型
	//DepdByJobIds []int32 `json:"depd_by_job_ids"` // 被依赖JobID
}

// TaskPayload 任务信息，启动任务用
type TaskPayload struct {
	OssPath string      `json:"oss_path"`
	Options interface{} `json:"options,omitempty"`
}

func MapToEventMetadata(m map[string]string) (*EventMetadata, error) {
	em := &EventMetadata{}

	// 字符串字段直接赋值
	em.EventType = m["event_type"]
	em.CompanyName = m["company_name"]

	// uint32 字段需要转换
	if val, ok := m["tenant_id"]; ok && val != "" {
		if u, err := strconv.ParseUint(val, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid tenant_id: %w", err)
		} else {
			em.TenantID = uint32(u)
		}
	}

	if val, ok := m["user_id"]; ok && val != "" {
		if u, err := strconv.ParseUint(val, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid user_id: %w", err)
		} else {
			em.UserID = uint32(u)
		}
	}

	if val, ok := m["task_id"]; ok && val != "" {
		if u, err := strconv.ParseUint(val, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid task_id: %w", err)
		} else {
			em.TaskID = uint32(u)
		}
	}

	if val, ok := m["work_task_id"]; ok && val != "" {
		if u, err := strconv.ParseUint(val, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid work_task_id: %w", err)
		} else {
			em.WorkTaskID = uint32(u)
		}
	}

	if val, ok := m["company_id"]; ok && val != "" {
		if u, err := strconv.ParseUint(val, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid company_id: %w", err)
		} else {
			em.CompanyID = uint32(u)
		}
	}

	return em, nil
}

// EventMetadataToMap 将 EventMetadata 转换为 map[string]string
func EventMetadataToMap(em *EventMetadata) map[string]string {
	if em == nil {
		return make(map[string]string)
	}

	return map[string]string{
		"event_type":   em.EventType,
		"tenant_id":    strconv.FormatUint(uint64(em.TenantID), 10),
		"user_id":      strconv.FormatUint(uint64(em.UserID), 10),
		"task_id":      strconv.FormatUint(uint64(em.TaskID), 10),
		"work_task_id": strconv.FormatUint(uint64(em.WorkTaskID), 10),
		"company_id":   strconv.FormatUint(uint64(em.CompanyID), 10),
		"company_name": em.CompanyName,
	}
}
