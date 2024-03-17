package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	mongoUtils "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

type PostInMongoDB struct {
	ID uuid.UUID `bson:"_id" json:"id"`
}

type MongoFeedService struct {
	mongoService         *mongoUtils.MongoService
	kafkaConsumerService *kafkaService.KafkaConsumerService
	quit                 chan struct{}
	mongoDbName          string
}

func CreateMongoFeedService(mongoService *mongoUtils.MongoService, kafkaConsumerService *kafkaService.KafkaConsumerService) *MongoFeedService {
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
	msgChannel, errChannel, err := s.kafkaConsumerService.SubscribeTopics(s.quit, []string{NewPostsTopic, DeletedPostsTopic}, 5*time.Second)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-s.quit:
				log.Debug("kafka consumer quit")
				return
			case msg := <-msgChannel:
				err := s.processMessageByTopic(msg)
				if err != nil {
					log.Error(fmt.Sprintf("unable ot process message with topic %v", msg.TopicPartition.Topic), err.Error())
				}
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
			return fmt.Errorf("error during processing topic %v. Unable to convert json %v to post", topic, msg.Value)
		} else {
			parsedUUID, err := uuid.Parse(postWithTagsFromQueue.PostUuid)
			if err != nil {
				return fmt.Errorf("unable to parsk uuid: %v", postWithTagsFromQueue.PostUuid)
			}
			return s.StorePostAtFeed(parsedUUID, postWithTagsFromQueue.TagIds...)
		}
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

func (s *MongoFeedService) StorePostAtFeed(uuid uuid.UUID, tagIds ...int) error {
	err := s.storePostAtFeed(uuid, FeedCommonCollectionName)
	if err != nil && mongo.IsDuplicateKeyError(errors.Cause(err)) {
		log.Error(fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", uuid, FeedCommonCollectionName), err.Error())
		return err
	}
	for _, tagId := range tagIds {
		collectionName := s.GetCollectionNameByTag(tagId)
		err := s.storePostAtFeed(uuid, collectionName)
		if err != nil && mongo.IsDuplicateKeyError(errors.Cause(err)) {
			log.Error(fmt.Sprintf("unable to insert document '%v' to collection '%v'", uuid, collectionName), err.Error())
			return err
		}
	}
	return nil
}

func (s *MongoFeedService) GetCollectionNameByTag(tagId int) string {
	return fmt.Sprintf("%v%v", FeedByTagCollectionPrefix, tagId)
}

func (s *MongoFeedService) GetFeed(limit int, offset int) ([]string, error) {
	return s.getFeed(FeedCommonCollectionName, limit, offset)
}

func (s *MongoFeedService) GetFeedByTag(tagId int, limit int, offset int) ([]string, error) {
	return s.getFeed(s.GetCollectionNameByTag(tagId), limit, offset)
}

func (s *MongoFeedService) getFeed(collectionName string, limit int, offset int) ([]string, error) {
	limit64 := int64(limit)
	offset64 := int64(offset)
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	filter := bson.D{}
	opts := options.Find().SetSkip(offset64).SetLimit(limit64).SetSort(bson.D{{"_id", -1}})
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}

	var posts []PostInMongoDB

	err = cursor.All(ctx, &posts)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(posts))

	for _, post := range posts {
		result = append(result, post.ID.String())
	}
	return result, nil
}

func (s *MongoFeedService) storePostAtFeed(uuid uuid.UUID, collectionName string) error {
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	document := bson.D{{"_id", uuid}}
	_, err := collection.InsertOne(ctx, document)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", uuid, FeedCommonCollectionName))
	}

	return nil
}
