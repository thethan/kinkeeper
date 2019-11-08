package consumers

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"log/syslog"
	"testing"
	"time"
)

type MockStoreInput struct {
	PullContexts []context.Context
}

type MockStoreOutput struct {
	MapConsumerData         []map[string]ConsumerData
	PullConsumerGroupErrors []error
}

type MockStore struct {
	Output MockStoreOutput
	Input  MockStoreInput
}

func MakeMockStore() MockStore {
	return MockStore{
		Input: MockStoreInput{
			PullContexts: make([]context.Context, 0),
		},
		Output: MockStoreOutput{
			MapConsumerData:         make([]map[string]ConsumerData, 0),
			PullConsumerGroupErrors: make([]error, 0),
		},
	}
}

func (m *MockStore) Store(ctx context.Context) {
	m.Input.PullContexts = append(m.Input.PullContexts, ctx)
}

func (m *MockStore) PullConsumerGroup(ctx context.Context) (map[string]ConsumerData, error) {
	i := len(m.Input.PullContexts)
	m.Input.PullContexts = append(m.Input.PullContexts, ctx)

	return m.Output.MapConsumerData[i], m.Output.PullConsumerGroupErrors[i]
}

func TestConsumerGroup_Register(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	mockStore := MakeMockStore()
	mockStore.Output.MapConsumerData = []map[string]ConsumerData{map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}}}
	mockStore.Output.PullConsumerGroupErrors = []error{nil}

	cg := NewConsumerGroup(context.Background(), logger, &mockStore, streamName, shardID)
	assert.IsType(t, ConsumerGroup{}, cg)

	timeBeforeRegister := time.Now()
	data, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data)

	assert.True(t, timeBeforeRegister.Before(data.timeOfRegister), "registered time should be after test starts now")
	timeAfterRegister := time.Now()
	assert.True(t, timeAfterRegister.After(data.timeOfRegister))

	data2, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data2)
	assert.Equal(t, data, data2)
}

func TestConsumerGroup_Register_With_AlreadyRegistered(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	mockStore := MakeMockStore()
	cg := NewConsumerGroup(context.Background(), logger, &mockStore, streamName, shardID)
	assert.IsType(t, ConsumerGroup{}, cg)

	data, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data)

	data2, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data2)
	assert.Equal(t, data, data2)
}

func TestConsumerGroup_CallingPullFromAtLeastOnce(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	mockStore := MakeMockStore()
	mockStore.Output.MapConsumerData = []map[string]ConsumerData{
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},

		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}}}
	mockStore.Output.PullConsumerGroupErrors = []error{nil, nil, nil, nil, nil, nil, nil, nil}

	ctx := context.Background()
	cg := NewConsumerGroup(ctx, logger, &mockStore, streamName, shardID)
	assert.IsType(t, ConsumerGroup{}, cg)

	// Read from gets called every 5 milliseconds
	time.Sleep(time.Millisecond)
	assert.Greater(t, 1, len(mockStore.Input.PullContexts), "Pull from store was not called once")

	ctx.Done()
}

func Test_readAndLoadConsumers(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	mockStore := MakeMockStore()
	mockStore.Output.MapConsumerData = []map[string]ConsumerData{
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
	}
	mockStore.Output.PullConsumerGroupErrors = []error{nil, nil, nil, nil, nil, nil, nil, nil}
	ctx := context.Background()

	cg := NewConsumerGroup(ctx, logger, &mockStore, streamName, shardID)

	errChan := make(chan error)
	cg.readAndLoadConsumers(ctx, errChan)

	assert.Equal(t, 0, len(errChan))
	lengthOfConsumers := 0
	cg.consumers.Range(func(key, value interface{}) bool {
		lengthOfConsumers++
		return true
	})
	assert.Equal(t, 1, lengthOfConsumers, "did not have the right number of consumers")
}



func Test_readAndLoadConsumers_multiple(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	mockStore := MakeMockStore()
	mockStore.Output.MapConsumerData = []map[string]ConsumerData{
		map[string]ConsumerData{"ethan": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}, "not": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}, "not2": {partitionKey: aws.String("test"), lastRead: aws.Time(time.Now())}},
		}
	mockStore.Output.PullConsumerGroupErrors = []error{nil, nil, nil, nil, nil, nil, nil, nil}
	ctx := context.Background()

	cg := NewConsumerGroup(ctx, logger, &mockStore, streamName, shardID)

	errChan := make(chan error)
	cg.readAndLoadConsumers(ctx, errChan)

	assert.Equal(t, 0, len(errChan))
	lengthOfConsumers := 0
	cg.consumers.Range(func(key, value interface{}) bool {
		lengthOfConsumers++
		return true
	})
	assert.Equal(t, 3, lengthOfConsumers, "did not have the right number of consumers")
}