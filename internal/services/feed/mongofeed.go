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
const UpdatedPostsStatesTopic = "updated_posts_states"
const UpdatedPostsTagsTopic = "updated_posts_tags"
const DeletedPostsTopic = "deleted_posts"

var AllTopics []string = []string{NewPostsTopic, UpdatedPostsStatesTopic, UpdatedPostsTagsTopic, DeletedPostsTopic}

const FeedCommonCollectionName = "feed_all_posts"
const FeedByTagCollectionPrefix = "feed_by_tag_"
const FeedByUserCollectionPrefix = "feed_by_user_"

type PostWithTagsFromQueue struct {
	PostUuid   uuid.UUID
	AuthorUuid string
	CreateDate time.Time
	State      string
	TagIds     []int
}

type PostInMongoDB struct {
	ID         uuid.UUID `bson:"_id" json:"id"`
	CreateDate time.Time `bson:"createDate" json:"createDate"`
	State      string    `bson:"state" json:"state"`
}

type MongoFeedService struct {
	mongoService            *mongoUtils.MongoService
	kafkaConsumerService    *kafkaService.KafkaConsumerService
	kafkaAdminService       *kafkaService.KafkaAdminService
	quit                    chan struct{}
	msgChannel              chan *kafka.Message
	errChannel              chan error
	mongoDbName             string
	kafkaReadMessageTimeout time.Duration
}

func CreateMongoFeedService(mongoService *mongoUtils.MongoService, kafkaConsumerService *kafkaService.KafkaConsumerService, kafkaAdminService *kafkaService.KafkaAdminService, kafkaReadMessageTimeout time.Duration) *MongoFeedService {
	quit := make(chan struct{})
	msgChannel := make(chan *kafka.Message)
	errChannel := make(chan error)
	mongoDbName := utils.EnvVar("MONGO_DB_NAME")

	return &MongoFeedService{
		mongoService:            mongoService,
		kafkaConsumerService:    kafkaConsumerService,
		kafkaAdminService:       kafkaAdminService,
		quit:                    quit,
		msgChannel:              msgChannel,
		errChannel:              errChannel,
		mongoDbName:             mongoDbName,
		kafkaReadMessageTimeout: kafkaReadMessageTimeout,
	}
}

func (s *MongoFeedService) Shutdown() error {
	defer close(s.quit)
	defer close(s.msgChannel)
	defer close(s.errChannel)
	result := []error{}
	err := s.mongoService.ShutDown()
	if err != nil {
		result = append(result, err)
	}
	err = s.kafkaConsumerService.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.kafkaAdminService.Shutdown()
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

	err := s.kafkaConsumerService.SubscribeTopics(AllTopics)
	if err != nil {
		// trying to create topics
		errOfTopicsCreating := s.kafkaAdminService.CreateTopics(AllTopics, 1)
		if errOfTopicsCreating != nil {
			return errOfTopicsCreating
		}
	}

	s.kafkaConsumerService.StartReadingMessages(s.quit, s.msgChannel, s.errChannel, s.kafkaReadMessageTimeout)

	s.startConsumingKafkaMessages()

	s.startConsumingKafkaError()

	return nil
}

func (s *MongoFeedService) StopSync() error {
	log.Debug("STOP FEED SYNC")
	s.kafkaConsumerService.Unsubscribe()
	s.quit <- struct{}{}
	return nil
}

func (s *MongoFeedService) startConsumingKafkaMessages() {
	go func() {
		for {
			select {
			case <-s.quit:
				log.Debug("kafka consumer quit")
				return
			case msg := <-s.msgChannel:
				err := s.processMessageByTopic(msg)
				if err != nil {
					log.Error(fmt.Sprintf("unable ot process message with topic %v", msg.TopicPartition.Topic), err.Error())
				}
			}
		}
	}()
}
func (s *MongoFeedService) startConsumingKafkaError() {
	go func() {
		for {
			select {
			case <-s.quit:
				log.Debug("kafka consumer quit")
				return
			case msg := <-s.errChannel:
				log.Error("kafka consume error", msg.Error())
			}
		}
	}()
}

func (s *MongoFeedService) processMessageByTopic(msg *kafka.Message) error {
	if msg == nil {
		return fmt.Errorf("nil message")
	}

	topic := *msg.TopicPartition.Topic

	switch topic {
	case NewPostsTopic:
		post, err := parseMessageToPost(msg)
		if err != nil {
			return err
		}
		return s.StorePostAtFeeds(post)

	case UpdatedPostsStatesTopic:
		post, err := parseMessageToPost(msg)
		if err != nil {
			return err
		}
		return s.UpdatePostStateAtFeeds(post)
	case UpdatedPostsTagsTopic:
		post, err := parseMessageToPost(msg)
		if err != nil {
			return err
		}
		return s.UpdatePostTagsAtFeeds(post)
	case DeletedPostsTopic:
		postUuid, err := parseMessageToUUID(msg)
		if err != nil {
			return err
		}
		return s.DeletePostAtFeeds(postUuid)
	default:
		return fmt.Errorf("unknown message Topic: %v", topic)
	}
}

func (s *MongoFeedService) StorePostAtFeeds(post *PostWithTagsFromQueue) error {
	err := s.storePostAtFeed(FeedCommonCollectionName, post.PostUuid, post.CreateDate, post.State)
	if err != nil && !mongo.IsDuplicateKeyError(errors.Cause(err)) {
		log.Error(fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", post.PostUuid, FeedCommonCollectionName), err.Error())
		return err
	}

	userFeedCollectionName := s.GetCollectionNameByUser(post.AuthorUuid)
	err = s.storePostAtFeed(userFeedCollectionName, post.PostUuid, post.CreateDate, post.State)
	if err != nil && !mongo.IsDuplicateKeyError(errors.Cause(err)) {
		log.Error(fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", post.PostUuid, userFeedCollectionName), err.Error())
		return err
	}

	for _, tagId := range post.TagIds {
		tagFeedCollectionName := s.GetCollectionNameByTag(tagId)
		err := s.storePostAtFeed(tagFeedCollectionName, post.PostUuid, post.CreateDate, post.State)
		if err != nil && !mongo.IsDuplicateKeyError(errors.Cause(err)) {
			log.Error(fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", post.PostUuid, tagFeedCollectionName), err.Error())
			return err
		}
	}
	return nil
}

func (s *MongoFeedService) UpdatePostStateAtFeeds(post *PostWithTagsFromQueue) error {
	// TODO: iterate though all find post by UUID in all collections and update the state
	collectionNames, err := s.mongoService.GetCollectionNames(s.mongoDbName)
	if err != nil {
		return err
	}

	for _, collectionName := range collectionNames {
		filter := bson.D{{"_id", post.PostUuid}}
		update := bson.D{{"$set", bson.D{{"state", post.State}}}}
		s.mongoService.Update(s.mongoDbName, collectionName, filter, update)

	}
	return nil
}

func (s *MongoFeedService) UpdatePostTagsAtFeeds(post *PostWithTagsFromQueue) error {
	// TODO: find post by UUID in all collections:
	// TODO: if post exists in collection_by_tag but it is missed in new array of tags -> drop the post UUID from this collection
	// TODO: if post exists in collection_by_tag but it is exists in new array of tags -> just set state
	// TODO: if post missed in collection_by_tag but it is exists in new array of tags -> add post UUID to this collection
	return nil
}
func (s *MongoFeedService) DeletePostAtFeeds(postUuid uuid.UUID) error {
	// TODO: find post by UUID in all collections and update the state to "DELETED"
	return nil
}

func (s *MongoFeedService) GetCollectionNameByTag(tagId int) string {
	return fmt.Sprintf("%v%v", FeedByTagCollectionPrefix, tagId)
}

func (s *MongoFeedService) GetCollectionNameByUser(userUuid string) string {
	return fmt.Sprintf("%v%v", FeedByUserCollectionPrefix, userUuid)
}

func (s *MongoFeedService) GetFeed(state string, limit int, offset int) ([]string, error) {
	filter := initFilter(state)
	return s.getFeed(FeedCommonCollectionName, filter, limit, offset)
}

func (s *MongoFeedService) GetFeedByTag(tagId int, state string, limit int, offset int) ([]string, error) {
	collectionName := s.GetCollectionNameByTag(tagId)
	filter := initFilter(state)
	return s.getFeed(collectionName, filter, limit, offset)
}

func (s *MongoFeedService) GetFeedByUser(userUuid string, state string, limit int, offset int) ([]string, error) {
	collectionName := s.GetCollectionNameByUser(userUuid)
	filter := initFilter(state)
	return s.getFeed(collectionName, filter, limit, offset)
}

func (s *MongoFeedService) getFeed(collectionName string, filter any, limit int, offset int) ([]string, error) {
	limit64 := int64(limit)
	offset64 := int64(offset)
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	opts := options.Find().SetSkip(offset64).SetLimit(limit64).SetSort(bson.D{{"createDate", -1}})
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

func (s *MongoFeedService) storePostAtFeed(collectionName string, uuid uuid.UUID, createDate time.Time, state string) error {
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	document := bson.D{{"_id", uuid}, {"createDate", createDate}, {"state", state}}
	_, err := collection.InsertOne(ctx, document)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to store post with UUID '%v' to collection '%v'", uuid, FeedCommonCollectionName))
	}

	return nil
}

func parseMessageToPost(msg *kafka.Message) (*PostWithTagsFromQueue, error) {
	var post *PostWithTagsFromQueue
	err := json.Unmarshal([]byte(msg.Value), &post)
	if err != nil {
		return nil, fmt.Errorf("unable to parse message '%v' to post", msg)
	}
	return post, nil
}

func parseMessageToUUID(msg *kafka.Message) (uuid.UUID, error) {
	var result uuid.UUID
	err := json.Unmarshal([]byte(msg.Value), &result)
	if err != nil {
		return result, fmt.Errorf("unable to parse message '%v' to uuid.UUID", msg)
	}
	return result, nil
}

func emptyFilter() any {
	return bson.D{}
}

func filterByState(state string) any {
	return bson.D{{"state", state}}
}

func initFilter(state string) any {
	if len(state) > 0 {
		return filterByState(state)
	}
	return emptyFilter()
}
