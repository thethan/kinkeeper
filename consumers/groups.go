package consumers

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	"sync"
	"time"
)

// Consumer is part of a group.
// Each one can equally read from the same stream
type Consumer interface {
	Scan(func(*kinesis.Record) error)
}

// Store saves the iteration into the database in order to pull the next record from the next consumer
type Store interface {
	Store(ctx context.Context, )
	PullConsumerGroup(ctx context.Context) (map[string]ConsumerData, error)
}

// namespace is where the shardID and stream names are located.
type namespace struct {
	streamName string
	shardID    string
}

// Data Transfer Object is what gets sent to the stores in order to pull the information
type DataTransferObject struct {
	StreamName   string
	Namespace    namespace
	Consumers    []string // unique identifiers for each consumer
	PartitionKey string
	LastUpdated  time.Time
}

// ConsumerGroup is what keeps track of all the consumers
type ConsumerGroup struct {
	log       *log.Logger
	consumers sync.Map
	namespace namespace
	store     Store
}

// NewConsumerGroup creates a consumer group
func NewConsumerGroup(ctx context.Context, log *log.Logger, store Store, streamName string, shardID string) ConsumerGroup {
	cg := ConsumerGroup{
		log: log,
		namespace: namespace{
			shardID:    shardID,
			streamName: streamName,
		},
		store: store,
	}

	cg.consumers = sync.Map{}
	go cg.start(ctx)
	return cg
}

// consumerData
type ConsumerData struct {
	partitionKey   *string
	lastRead       *time.Time
	timeOfRegister time.Time
}

// start listening for other consumers
func (cg *ConsumerGroup) start(ctx context.Context) error {

	readErrorChan := make(chan error)
	// pull from the data store

	ticker := time.NewTicker(5 * time.Millisecond)

	//// read on load
	//cg.readAndLoadConsumers(ctx, readErrorChan)

	for {
		select {
		case <-ctx.Done():
			cg.log.Print("context closed ")
			close(readErrorChan)
			return nil
		case err := <-readErrorChan:
			cg.log.Print("error in error channel. Closing channel ", "err ", err)
			close(readErrorChan)
			return nil
		case <-ticker.C:
			cg.readAndLoadConsumers(ctx, readErrorChan)

		}
	}
}

func (cg *ConsumerGroup) readAndLoadConsumers(ctx context.Context, errorChan chan error) {
	consumers, err := cg.store.PullConsumerGroup(ctx)
	if err != nil {
		errorChan <- err
	}
	err = loadConsumers(ctx, cg, consumers)
	if err != nil {
		errorChan <- err
	}
}

func loadConsumers(ctx context.Context, consumerGroup *ConsumerGroup, consumersDataObject map[string]ConsumerData) error {
	for key, data := range consumersDataObject {
		consumerGroup.consumers.Store(key, data)
	}
	return nil
}

// Register a consumer.
// A consumer will be added to the loop in order to read from the checkpoint store.
func (cg *ConsumerGroup) Register(ctx context.Context, identifier string) (*ConsumerData, error) {
	data, alreadyExists := cg.consumers.LoadOrStore(identifier, &ConsumerData{timeOfRegister:time.Now()})

	cg.log.Printf("ConsumerData already existed if true: %v, else it was stored for identifier: %s\n", alreadyExists, identifier)

	cData, isType := data.(*ConsumerData)
	if isType == false {
		return nil, errors.New("could not convert consumer data to")
	}

	return cData, nil
}

// CheckIfConsumerIsNextInSequence if the
func (cg *ConsumerGroup) CheckIfConsumerIsNextInSequence(ctx context.Context, identifier string) {

}
