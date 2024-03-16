package feed

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const NewPostsTopic = "new_posts"
const UpdatedPostsTopic = "updated_posts"
const DeletedPostsTopic = "deleted_posts"
const AssignedTagsToPostsTopic = "assigned_tags_to_posts"
const DeletededTagsToPostsTopic = "deleted_tags_to_posts"

const FeedCommonCollectionName = "feed_all_posts"
const FeedByTagCollectionPrefix = "feed_by_tag_"

type PostWithTagsFromQueue struct {
	PostUuid string
	TagIds   []int
}

type MongoFeedService struct {
	mongoService         *mongo.MongoService
	kafkaConsumerService *kafkaService.KafkaConsumerService
	quit                 chan struct{}
	mongoDbName          string
}

func CreateMongoFeedService(mongoService *mongo.MongoService, kafkaConsumerService *kafkaService.KafkaConsumerService) *MongoFeedService {
	quit := make(chan struct{})
	mongoDbName := utils.EnvVar("MONGO_DB_NAME")

	return &MongoFeedService{
		mongoService:         mongoService,
		kafkaConsumerService: kafkaConsumerService,
		quit:                 quit,
		mongoDbName:          mongoDbName,
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
	log.Debug("START FEED SYNC")

	//TODO: process error when topics are missed:
	//{"@timestamp":"2024-03-16T07:27:47Z","cause":"consumer error: Subscribed topic not available: deleted_posts: Broker: Unknown topic or partition","level":"error","message":"kafka consume error"}
	//{"@timestamp":"2024-03-16T07:27:47Z","cause":"consumer error: Subscribed topic not available: new_posts: Broker: Unknown topic or partition","level":"error","message":"kafka consume error"}
	msgChannel, errChannel := s.kafkaConsumerService.SubscribeTopics(s.quit, []string{NewPostsTopic, DeletedPostsTopic}, 5*time.Second)

	go func() {
		for {
			select {
			case <-s.quit:
				log.Debug("kafka consumer quit")
				return
			case msg := <-msgChannel:
				s.processMessageByTopic(msg)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-s.quit:
				log.Debug("kafka consumer quit")
				return
			case msg := <-errChannel:
				log.Error("kafka consume error", msg.Error())
			}
		}
	}()

	return nil
}

func (s *MongoFeedService) StopSync() error {
	log.Debug("STOP FEED SYNC")
	s.quit <- struct{}{}
	return nil
}

func (s *MongoFeedService) processMessageByTopic(msg *kafka.Message) error {
	if msg == nil {
		return fmt.Errorf("nil message")
	}

	topic := *msg.TopicPartition.Topic

	switch topic {
	case NewPostsTopic:
		var postWithTagsFromQueue *PostWithTagsFromQueue
		err := json.Unmarshal([]byte(msg.Value), &postWithTagsFromQueue)
		if err != nil {
			log.Error(fmt.Sprintf("Error during processing topic %v. Unable to convert json %v to post", topic, msg.Value), err.Error())
		} else {
			// TODO: save as array
			s.mongoService.Insert(s.mongoDbName, FeedCommonCollectionName, postWithTagsFromQueue.PostUuid)
		}
		fmt.Printf("----------------NEW MESSAGE: %v\n", postWithTagsFromQueue) // todo clean
	case UpdatedPostsTopic:
		// TODO
	case DeletedPostsTopic:
		// TODO
	case AssignedTagsToPostsTopic:
		// TODO
	case DeletededTagsToPostsTopic:
		// TODO
	default:
		return fmt.Errorf("unknown message Topic: %v", topic)
	}

	return nil
}
