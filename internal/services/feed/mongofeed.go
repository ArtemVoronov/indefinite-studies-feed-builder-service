package feed

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
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
				fmt.Printf("kafka consumer quit\n")
				return
			case msg := <-msgChannel:
				// TODO: process different cases base on message topic (new post, deleted post, updated tags)
				var postWithTagsFromQueue *PostWithTagsFromQueue
				err := json.Unmarshal([]byte(msg.Value), &postWithTagsFromQueue)
				if err != nil {
					log.Error(fmt.Sprintf("Unable to convert json %v to psot", msg.Value), err.Error())
				} else {
					// TODO: if post with the UUID is missed -> insert
					// TODO: if post exists -> add tag
					s.mongoService.Insert(s.mongoDbName, FeedCommonCollectionName, postWithTagsFromQueue)
				}
				fmt.Printf("----------------NEW MESSAGE: %v\n", postWithTagsFromQueue) // todo clean
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
	log.Debug("STOP FEED SYNC")
	s.quit <- struct{}{}
	return nil
}
