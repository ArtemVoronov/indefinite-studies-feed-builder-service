package feed

import (
	"fmt"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
)

const NewPostsTopic = "new_posts"
const UpdatedPostsTopic = "updated_posts"
const DeletedPostsTopic = "deleted_posts"
const AssignedTagsToPostsTopic = "assigned_tags_to_posts"
const DeletededTagsToPostsTopic = "deleted_tags_to_posts"

type MongoFeedService struct {
	mongoService         *mongo.MongoService
	kafkaConsumerService *kafkaService.KafkaConsumerService
	quit                 chan struct{}
}

func CreateMongoFeedService(mongoService *mongo.MongoService, kafkaConsumerService *kafkaService.KafkaConsumerService) *MongoFeedService {
	return &MongoFeedService{
		mongoService:         mongoService,
		kafkaConsumerService: kafkaConsumerService,
	}
}

func (s *MongoFeedService) Shutdown() error {
	defer close(s.quit)
	result := []error{}
	err := s.mongoService.ShutDown()
	if err != nil {
		result = append(result, err)
	}
	err = s.kafkaConsumerService.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	if len(result) > 0 {
		return fmt.Errorf("errors during mongo feed service shutdown: %v", result)
	}
	return nil
}

func (s *MongoFeedService) StartSync() error {
	// TODO: start reading queue from kafka and enable sync process

	fmt.Println("-------------START FEED SYNC-------------")
	s.quit = make(chan struct{})
	msgChannel, errChannel := s.kafkaConsumerService.SubscribeTopics(s.quit, []string{NewPostsTopic, DeletedPostsTopic}, 5*time.Second)

	go func() {
		for {
			select {
			case <-s.quit:
				fmt.Printf("kafka consumer quit\n")
				return
			case msg := <-msgChannel:
				fmt.Printf("----------------NEW MESSAGE: %v\n", msg.Value)
				//TODO: add to the appropruate feed
			}
		}
	}()

	go func() {
		for {
			select {
			case <-s.quit:
				fmt.Printf("kafka consumer quit\n")
				return
			case msg := <-errChannel:
				log.Error("kafka consume error", msg.Error())
			}
		}
	}()

	return nil
}

func (s *MongoFeedService) StopSync() error {
	fmt.Println("-------------STOP FEED SYNC-------------") // TODO: clean
	s.quit <- struct{}{}
	return nil
}
