package consumers

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log/syslog"
	"testing"
)

type MockStore struct {
	
}

func (MockStore) Store(ctx context.Context, ) {
	panic("implement me")
}

func TestConsumerGroup_Register(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	cg := NewConsumerGroup(logger, &MockStore{}, streamName, shardID)
	assert.IsType(t, ConsumerGroup{}, cg)

	data, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data)

	data2, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data2)
	assert.Equal(t, data, data2)
}


func TestConsumerGroup_Register_With_AlreadyRegistered(t *testing.T) {
	logger, _ := syslog.NewLogger(syslog.LOG_INFO, 1)
	shardID := "0000"
	streamName := "streamName"
	cg := NewConsumerGroup(logger,&MockStore{},  streamName, shardID)
	assert.IsType(t, ConsumerGroup{}, cg)

	data, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data)

	data2, err := cg.Register(context.Background(), "consumer1")
	assert.Nil(t, err)
	assert.IsType(t, &ConsumerData{}, data2)
	assert.Equal(t, data, data2)
}

