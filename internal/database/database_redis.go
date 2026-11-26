// Copyright (c) 2022-2023 https://rasbora.openseawave.com
//
// This file is part of Rasbora Distributed Video Transcoding
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"openseawave.com/rasbora/internal/config"
	"openseawave.com/rasbora/internal/data"
	"openseawave.com/rasbora/internal/logger"
)

// Create a new context based on the Background context
var ctx = context.Background()

// RedisDatabaseManager holds an instance
type RedisDatabaseManager struct {
	Redis  *redis.Client
	Config *config.Config
	Logger *logger.Logger
}

// SendLogsToDatabase save rasbora logs at database.
func (rdm *RedisDatabaseManager) SendLogsToDatabase(log map[string]interface{}) error {
	res := rdm.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: rdm.Config.GetString("Database.Redis.Structure.Logger"),
		Values: log,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// SendHeartbeat send heartbeat to update cluster status.
func (rdm *RedisDatabaseManager) SendHeartbeat(workerId, workerType string) error {
	clusterHeartbeatList := rdm.Config.GetString("Database.Redis.Structure.Cluster.Heartbeat")

	workerWithType := fmt.Sprintf("%s:%s", workerType, workerId)

	if zAddResult := rdm.Redis.ZAdd(
		ctx,
		clusterHeartbeatList,
		redis.Z{
			Score:  float64(time.Now().UnixMilli()),
			Member: workerWithType,
		},
	); zAddResult.Err() != nil {
		return zAddResult.Err()
	}

	return nil
}

// Enqueue add item to waiting queue.
//
// Enqueue is idempotent for a given item.ID: repeated submissions or
// redeliveries never create more than one live entry in the waiting sorted set
// and never reset or inflate the retry counter. The retry hash field is
// initialised to zero on first sight and is only advanced by Failed (one
// increment per recorded failure). Re-enqueueing a failed/retriable item
// therefore preserves its retry count instead of clearing or doubling it.
func (rdm *RedisDatabaseManager) Enqueue(queueName string, item data.Queueable) error {
	scoreWithID := fmt.Sprintf("%d:%s", time.Now().UnixMilli(), item.ID)
	waiting, status, worker, _, retry, items, _ := rdm._queueStructures(queueName)

	// Remove any waiting members that already refer to the same item ID so a
	// duplicate enqueue cannot leave parallel entries for one task/callback.
	oldMembers := rdm._waitingMembersForID(waiting, item.ID)

	tx := rdm.Redis.TxPipeline()
	for _, member := range oldMembers {
		tx.ZRem(ctx, waiting, member)
	}
	tx.ZAdd(ctx, waiting, redis.Z{Score: item.Priority, Member: scoreWithID})
	tx.HSet(ctx, items, item.ID, item)
	tx.HSet(ctx, status, item.ID, "waiting")
	// Clear any previous worker ownership. HDel (instead of HSet with a nil
	// value) avoids leaving an empty owner field in the worker hash.
	tx.HDel(ctx, worker, item.ID)
	// Initialise the retry counter exactly once. Pre-existing values are left
	// untouched so re-enqueue after a failure neither resets nor doubles them.
	tx.HSetNX(ctx, retry, item.ID, 0)

	if _, err := tx.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// Dequeue fetch item from waiting queue.
//
// Before blocking on the waiting set it recovers items that were left in the
// "working" state with a worker owner. A worker processes one item at a time
// and only calls Dequeue while idle, so every ownership field still present at
// this point belongs to a crashed/restarted/redelivered execution (including
// tasks left under an old worker ID). Those items are returned to the waiting
// queue with their retry count preserved and are redistributed instead of
// being permanently stuck in "working".
func (rdm *RedisDatabaseManager) Dequeue(queueName string, workerId string) (item data.Queueable, err error) {
	waiting, status, worker, _, _, items, _ := rdm._queueStructures(queueName)

	rdm._recoverInterruptedItems(queueName)

	ids, zPopMinError := rdm.Redis.ZPopMin(ctx, waiting, 1).Result()
	if zPopMinError != nil {
		return item, zPopMinError
	}

	if len(ids) <= 0 {
		return item, errors.New("there is no items in waiting queue")
	}

	var itemID = queueMemberItemID(ids[0].Member)
	itemAsJsonString, hGetError := rdm.Redis.HGet(ctx, items, itemID).Result()
	if hGetError != nil {
		return item, hGetError
	}

	jsonParserError := json.Unmarshal([]byte(itemAsJsonString), &item)
	if jsonParserError != nil {
		return item, jsonParserError
	}

	rdm.Redis.HSet(ctx, status, item.ID, "working")
	rdm.Redis.HSet(ctx, worker, item.ID, workerId)

	return item, nil
}

// _recoverInterruptedItems return "working" items still owned by a worker back
// to the waiting queue. Terminal items with a stale owner field only get their
// ownership cleared. Retry counters are intentionally not touched: a restart
// or redelivery is not a processing failure.
func (rdm *RedisDatabaseManager) _recoverInterruptedItems(queueName string) {
	waiting, statusName, worker, _, _, items, _ := rdm._queueStructures(queueName)

	workerList, err := rdm.Redis.HGetAll(ctx, worker).Result()
	if err != nil {
		return
	}

	for itemID := range workerList {
		currentStatus, _ := rdm.Redis.HGet(ctx, statusName, itemID).Result()

		if currentStatus == "failed" || currentStatus == "finished" {
			// Terminal state with a leftover owner: drop the ownership field.
			_, _ = rdm.Redis.HDel(ctx, worker, itemID).Result()
			continue
		}

		if currentStatus != "working" {
			// A waiting/missing item should not carry an owner. Clear the stale
			// field (this also removes legacy empty-string owners) without
			// creating a duplicate waiting entry.
			_, _ = rdm.Redis.HDel(ctx, worker, itemID).Result()
			continue
		}

		itemAsJsonString, hGetError := rdm.Redis.HGet(ctx, items, itemID).Result()
		if hGetError != nil {
			// Only drop ownership when the item record is genuinely gone. For
			// transient Redis errors keep the field so the entry is not lost.
			if errors.Is(hGetError, redis.Nil) {
				_, _ = rdm.Redis.HDel(ctx, worker, itemID).Result()
			}
			continue
		}

		var item data.Queueable
		if jsonParserError := json.Unmarshal([]byte(itemAsJsonString), &item); jsonParserError != nil {
			continue
		}

		// Requeue the interrupted item exactly once. Removing prior same-ID
		// members first keeps the waiting set idempotent.
		oldMembers := rdm._waitingMembersForID(waiting, itemID)
		scoreWithID := fmt.Sprintf("%d:%s", time.Now().UnixMilli(), itemID)

		tx := rdm.Redis.TxPipeline()
		for _, member := range oldMembers {
			tx.ZRem(ctx, waiting, member)
		}
		tx.ZAdd(ctx, waiting, redis.Z{Score: item.Priority, Member: scoreWithID})
		tx.HSet(ctx, statusName, itemID, "waiting")
		tx.HDel(ctx, worker, itemID)
		if _, err := tx.Exec(ctx); err != nil {
			continue
		}
	}
}

// _waitingMembersForID return all members of the waiting sorted set that refer
// to the given queue item ID.
func (rdm *RedisDatabaseManager) _waitingMembersForID(waiting, itemID string) []string {
	members, err := rdm.Redis.ZRange(ctx, waiting, 0, -1).Result()
	if err != nil {
		return nil
	}

	var result []string
	for _, member := range members {
		if queueMemberItemID(member) == itemID {
			result = append(result, member)
		}
	}

	return result
}

// queueMemberItemID extract the item ID from a waiting set member. Members use
// the "unix-milli:item-id" convention; everything after the first colon is the
// ID (IDs without a colon are returned unchanged).
func queueMemberItemID(member interface{}) string {
	memberAsString, ok := member.(string)
	if !ok {
		return fmt.Sprintf("%v", member)
	}

	if separator := strings.Index(memberAsString, ":"); separator >= 0 && separator+1 < len(memberAsString) {
		return memberAsString[separator+1:]
	}

	return memberAsString
}

// Failed change item status to failed.
//
// Recording a failure advances the per-item retry counter exactly once. The
// counter lives in the existing retry hash and is preserved by Enqueue, so a
// retriable item goes through Failed (+1) then Enqueue (no change) and never
// jumps by more than one per real failed attempt.
func (rdm *RedisDatabaseManager) Failed(queueName string, item data.Queueable, err error) error {
	_, status, worker, processing, retry, items, logs := rdm._queueStructures(queueName)

	errorMessage := ""
	if err != nil {
		errorMessage = err.Error()
	}

	tx := rdm.Redis.TxPipeline()

	tx.Del(ctx, fmt.Sprintf("%v:%v", processing, item.ID))
	tx.HSet(ctx, items, item.ID, item)
	tx.HSet(ctx, status, item.ID, "failed")
	tx.HSet(ctx, logs, item.ID, errorMessage)
	tx.HDel(ctx, worker, item.ID)
	tx.HIncrBy(ctx, retry, item.ID, 1)

	if _, err := tx.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// Finished change item status to finished.
func (rdm *RedisDatabaseManager) Finished(queueName string, item data.Queueable) error {
	_, status, worker, processing, _, items, _ := rdm._queueStructures(queueName)

	tx := rdm.Redis.TxPipeline()

	// Remove the per-task processing stream as well so no processing flow is
	// left behind after a successful completion.
	tx.Del(ctx, fmt.Sprintf("%v:%v", processing, item.ID))
	tx.HSet(ctx, items, item.ID, item)
	tx.HSet(ctx, status, item.ID, "finished")
	tx.HDel(ctx, worker, item.ID)

	if _, err := tx.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// Processing send single content a processing status.
func (rdm *RedisDatabaseManager) Processing(queueName string, data map[string]interface{}) error {
	_, _, _, processing, _, _, _ := rdm._queueStructures(queueName)

	res := rdm.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: fmt.Sprintf("%v:%v", processing, data["task_id"]),
		Values: data,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// TotalRetry get total failed retry.
func (rdm *RedisDatabaseManager) TotalRetry(queueName string, item data.Queueable) int {
	_, _, _, _, retry, _, _ := rdm._queueStructures(queueName)

	resH, hGetError := rdm.Redis.HGet(ctx, retry, item.ID).Result()
	if hGetError != nil {
		return -1
	}

	resP, err := strconv.ParseInt(resH, 10, 64)
	if err != nil {
		return -1
	}

	return int(resP)
}

// SendSystemRadarScannerData send system radar scanning data content full information about running node.
func (rdm *RedisDatabaseManager) SendSystemRadarScannerData(scanner map[string]interface{}) error {
	res := rdm.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: rdm.Config.GetString("Database.Redis.Structure.Cluster.Radar"),
		Values: scanner,
	})

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

// _queueStructures shortcut to fetch all key names
func (rdm *RedisDatabaseManager) _queueStructures(queueName string) (waiting, status, worker, processing, retry, items, logs string) {
	waiting = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Waiting"), "{{name}}", queueName, 1)
	status = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Status"), "{{name}}", queueName, 1)
	worker = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Worker"), "{{name}}", queueName, 1)
	processing = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Processing"), "{{name}}", queueName, 1)
	retry = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Retry"), "{{name}}", queueName, 1)
	items = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Items"), "{{name}}", queueName, 1)
	logs = strings.Replace(rdm.Config.GetString("Database.Redis.Structure.Queue.Logs"), "{{name}}", queueName, 1)
	return
}

// _outcomeDeliveredKey returns the hash key used to durably record that an
// external outcome (an HTTP callback) has already been delivered for an item.
// It is derived from the existing status key so no new configuration or key
// convention is required.
func (rdm *RedisDatabaseManager) _outcomeDeliveredKey(queueName string) string {
	_, status, _, _, _, _, _ := rdm._queueStructures(queueName)
	return status + ":delivered"
}

// MarkOutcomeDelivered durably record that the item's external outcome has
// been delivered successfully. This is the idempotency guard that makes every
// duplicate delivery after the first success a no-op.
func (rdm *RedisDatabaseManager) MarkOutcomeDelivered(queueName string, itemID string) error {
	return rdm.Redis.HSet(ctx, rdm._outcomeDeliveredKey(queueName), itemID, time.Now().UnixMilli()).Err()
}

// IsOutcomeDelivered report whether the item's external outcome was already
// delivered successfully.
func (rdm *RedisDatabaseManager) IsOutcomeDelivered(queueName string, itemID string) (bool, error) {
	return rdm.Redis.HExists(ctx, rdm._outcomeDeliveredKey(queueName), itemID).Result()
}

// _deliveryLockKey returns the short-lived lease key used to serialise
// delivery attempts for one item across competing workers.
func (rdm *RedisDatabaseManager) _deliveryLockKey(queueName, itemID string) string {
	_, status, _, _, _, _, _ := rdm._queueStructures(queueName)
	return status + ":lock:" + itemID
}

// AcquireDeliveryLock try to obtain the per-item delivery lease. It returns
// the opaque token that must be presented to ReleaseDeliveryLock. The lease
// auto-expires after ttl so a crashed worker can never block redelivery.
func (rdm *RedisDatabaseManager) AcquireDeliveryLock(queueName, itemID, token string, ttl time.Duration) (bool, error) {
	return rdm.Redis.SetNX(ctx, rdm._deliveryLockKey(queueName, itemID), token, ttl).Result()
}

// ReleaseDeliveryLock release the lease only when it still belongs to token.
// The compare-and-delete avoids removing another worker's renewed lease.
func (rdm *RedisDatabaseManager) ReleaseDeliveryLock(queueName, itemID, token string) error {
	script := redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	return script.Run(ctx, rdm.Redis, []string{rdm._deliveryLockKey(queueName, itemID)}, token).Err()
}
