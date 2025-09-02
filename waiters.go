package waiters

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

type UnifiedArgs map[string]any
type UnifiedResult struct {
	Result any
	Id     string
	Error  string
}
type TaskFn func(args UnifiedArgs) (any, error)

type Task struct {
	Fn   TaskFn
	Id   string
	Args UnifiedArgs
}

type App struct {
	registry    map[string]TaskFn
	rc          *redis.Client
	workerCount int // 每个Worker Process里goroutine worker的数量。producer可以随便填写
}

func (app *App) Registry() map[string]TaskFn {
	return app.registry
}

func GetTaskFnName(fn TaskFn) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func (app *App) Register(fn TaskFn) {
	name := GetTaskFnName(fn)
	app.registry[name] = fn
}

func (app *App) DispatchAsync(fn TaskFn, arg UnifiedArgs) (string, error) {
	name := GetTaskFnName(fn)
	argData, err := json.Marshal(arg)
	if err != nil {
		return "", err
	}

	taskId, err := app.rc.XAdd(ctx, &redis.XAddArgs{
		ID:     "*",
		Stream: "waiters-tasks",
		Values: map[string]interface{}{
			name: argData,
		},
	}).Result()
	if err != nil {
		return "", err
	}
	return taskId, nil
}

func NewApp(redisAddr string, workerCount int) (*App, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	return &App{
		registry:    make(map[string]TaskFn),
		rc:          rdb,
		workerCount: workerCount,
	}, nil
}

func Work(workerName string, index int, taskChan <-chan *Task, resultChan chan<- *UnifiedResult) {
	log.Infof("Worker process %s goroutine worker %d started", workerName, index)
	for task := range taskChan {
		result, err := task.Fn(task.Args)
		errStr := ""
		if err != nil {
			errStr = err.Error()
			log.Errorf("Worker %s task %s failed: %v", workerName, task.Id, err)
		} else {
			log.Infof("Worker %s task %s succeeded: %v", workerName, task.Id, result)
		}
		ur := &UnifiedResult{
			Result: result,
			Id:     task.Id,
			Error:  errStr,
		}
		resultChan <- ur
	}
}

func (app *App) FromStreamElementToTask(reply []redis.XStream) (*Task, error) {
	theOnlyData := reply[0]
	message := theOnlyData.Messages[0]

	taskId := message.ID
	Values := message.Values
	var fnName string
	var argData string
	for k, v := range Values {
		fnName = k
		_argData, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("failed to assert arg data to string. %v %T", v, v)
		}
		argData = _argData
	}
	arg := make(UnifiedArgs)
	err := json.Unmarshal([]byte(argData), &arg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal args: %v", err)
	}
	return &Task{
		Id:   taskId,
		Fn:   app.registry[fnName],
		Args: arg,
	}, nil
}

var ctx = context.Background()

func (app *App) Serve(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	workerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	_, err = app.rc.XGroupCreateMkStream(ctx, "waiters-tasks", "DEFAULT", "$").Result()
	if err != nil {
		errStr := err.Error()
		if errStr != "BUSYGROUP Consumer Group name already exists" {
			return err
		}
	}

	log.Infof("DEFAULT group available")

	taskChan := make(chan *Task)
	resultChan := make(chan *UnifiedResult)
	wg := sync.WaitGroup{}
	for index := range app.workerCount {
		wg.Go(func() { Work(workerName, index, taskChan, resultChan) })
	}
	go func() {
		for result := range resultChan {
			resultData, err := json.Marshal(result)
			if err != nil {
				log.Errorf("Failed to marshal result: %s, error %s", result.Id, err)
			}

			_, err = app.rc.Set(ctx, fmt.Sprintf("waiters-results:%s", result.Id), resultData, 0).Result()
			if err != nil {
				log.Errorf("Failed to store result: %s, error %s", result.Id, err)
			} else {
				log.Infof("Result stored: %s", result.Id)
			}
		}
	}()

	for {
		shutdown := false
		select {
		case <-ctx.Done():
			shutdown = true
		default:
			//
		}
		if shutdown {
			wg.Wait()
			log.Infof("Worker %s shutdown", workerName)
			return nil
		}
		reply, err := app.rc.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "DEFAULT",
			Consumer: workerName,
			Block:    6000 * time.Millisecond,
			Count:    1,
			Streams:  []string{"waiters-tasks", ">"},
		}).Result()
		if err != nil {
			if err.Error() == "redis: nil" {
				log.Info("No tasks in queue")
			} else {
				log.Errorf("Worker %s failed to read task: %v", workerName, err.Error())
			}
			continue
		}
		task, err := app.FromStreamElementToTask(reply)
		if err != nil {
			log.Errorf("Worker %s failed to parse task: %v", workerName, err)
			continue
		}
		taskChan <- task
	}
}
