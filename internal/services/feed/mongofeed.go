package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/db/entities"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	mongoUtils "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//TODO: add indexes to posts feeds

const NewPostsTopic = "new_posts"
const UpdatedPostsStatesTopic = "updated_posts_states"
const UpdatedPostsTagsTopic = "updated_posts_tags"
const DeletedPostsTopic = "deleted_posts"
const NewCommentsTopic = "new_comments"
const UpdatedCommentsStatesTopic = "updated_comments_states"
const DeletedCommentsTopic = "deleted_comments"

var AllTopics []string = []string{NewPostsTopic, UpdatedPostsStatesTopic, UpdatedPostsTagsTopic, DeletedPostsTopic, NewCommentsTopic, UpdatedCommentsStatesTopic, DeletedCommentsTopic}

const FeedCommonCollectionName = "feed_all_posts"
const FeedByTagCollectionPrefix = "feed_by_tag_"
const FeedByUserCollectionPrefix = "feed_by_user_"
const CommentsCollectionPrefix = "comments_by_post_"
const PostsCollectionsPrefix = "feed_"
const CommentsCollectionsPrefix = "comments_"

type PostWithTagsFromQueue struct {
	PostUuid   uuid.UUID
	AuthorUuid string
	CreateDate time.Time
	State      string
	TagIds     []int
}

type CommentFromQueue struct {
	PostUuid   uuid.UUID
	CommentId  int
	CreateDate time.Time
	State      string
}

type DeletedCommentForQueue struct {
	PostUuid  uuid.UUID
	CommentId int
}

type PostInMongoDB struct {
	ID         uuid.UUID `bson:"_id" json:"id"`
	CreateDate time.Time `bson:"createDate" json:"createDate"`
	State      string    `bson:"state" json:"state"`
}

type CommentInMongoDB struct {
	ID         int       `bson:"_id" json:"Id"`
	CreateDate time.Time `bson:"createDate" json:"CreateDate"`
	State      string    `bson:"state" json:"State"`
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
		return errors.Join(result...)
	}
	return nil
}

func (s *MongoFeedService) CreateRequiredTopics() error {
	return s.kafkaAdminService.CreateTopics(AllTopics, 1)
}

func (s *MongoFeedService) StartSync() error {
	log.Debug("START FEED SYNC")
	err := s.kafkaConsumerService.SubscribeTopics(AllTopics)
	if err != nil {
		return err
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
			case errMsg := <-s.errChannel:
				log.Error("kafka consume error", errMsg.Error())
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
	case NewCommentsTopic:
		comment, err := parseMessageToComment(msg)
		if err != nil {
			return err
		}
		return s.StoreCommentAtFeeds(comment)
	case UpdatedCommentsStatesTopic:
		comment, err := parseMessageToComment(msg)
		if err != nil {
			return err
		}
		return s.UpdateCommentStateAtFeeds(comment)
	case DeletedCommentsTopic:
		comment, err := parseMessageToDeletedComment(msg)
		if err != nil {
			return err
		}
		return s.DeleteCommentAtFeeds(comment)
	default:
		return fmt.Errorf("unknown message Topic: %v", topic)
	}
}

func (s *MongoFeedService) StorePostAtFeeds(post *PostWithTagsFromQueue) error {
	err := s.storePostAtFeed(FeedCommonCollectionName, post.PostUuid, post.CreateDate, post.State)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("unable to store post with UUID '%v' to collection '%v', error: %w", post.PostUuid, FeedCommonCollectionName, err)
	}

	userFeedCollectionName := s.GetPostsCollectionNameByUser(post.AuthorUuid)
	err = s.storePostAtFeed(userFeedCollectionName, post.PostUuid, post.CreateDate, post.State)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("unable to store post with UUID '%v' to collection '%v', error: %w", post.PostUuid, userFeedCollectionName, err)
	}

	for _, tagId := range post.TagIds {
		tagFeedCollectionName := s.GetPostsCollectionNameByTag(tagId)
		err := s.storePostAtFeed(tagFeedCollectionName, post.PostUuid, post.CreateDate, post.State)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("unable to store post with UUID '%v' to collection '%v', error: %w", post.PostUuid, tagFeedCollectionName, err)
		}
	}
	return nil
}

func (s *MongoFeedService) UpdatePostStateAtFeeds(post *PostWithTagsFromQueue) error {
	filterForCollections := bson.D{{"name", bson.D{{"$regex", "^" + PostsCollectionsPrefix}}}}
	collectionNames, err := s.mongoService.GetCollectionNames(s.mongoDbName, filterForCollections)
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
	filterForCollections := bson.D{{"name", bson.D{{"$regex", "^" + FeedByTagCollectionPrefix}}}}
	actualCollectionNames, err := s.mongoService.GetCollectionNames(s.mongoDbName, filterForCollections)
	if err != nil {
		return fmt.Errorf("unable to get collections: %w", err)
	}

	expectedCollectionNames := make([]string, len(post.TagIds))
	for i, tagId := range post.TagIds {
		expectedCollectionNames[i] = s.GetPostsCollectionNameByTag(tagId)
	}

	//if post exists in collection_by_tag but it is missed in new array of tags -> drop the post UUID from this collection
	for _, collectionName := range actualCollectionNames {
		if !slices.Contains(expectedCollectionNames, collectionName) {
			filter := bson.D{{"_id", post.PostUuid}}
			update := bson.D{{"$set", bson.D{{"state", entities.POST_STATE_DELETED}}}}
			err := s.mongoService.Update(s.mongoDbName, collectionName, filter, update)
			if err != nil {
				return fmt.Errorf("unable to drop post %v in collection %v, error: %w", post, collectionName, err)
			}
		}
	}

	// if post exists in collection_by_tag but it is exists in new array of tags -> just set state
	// if post missed in collection_by_tag but it is exists in new array of tags -> add post UUID to this collection
	for _, collectionName := range expectedCollectionNames {
		filter := bson.D{{"_id", post.PostUuid}}
		update := bson.D{{"$set", bson.D{{"_id", post.PostUuid}, {"createDate", post.CreateDate}, {"state", post.State}}}}
		s.mongoService.Upsert(s.mongoDbName, collectionName, filter, update)
		if err != nil {
			return fmt.Errorf("unable to upsert post %v in collection %v, error: %w", post, collectionName, err)
		}
	}
	return nil
}

func (s *MongoFeedService) DeletePostAtFeeds(postUuid uuid.UUID) error {
	filter := bson.D{{"name", bson.D{{"$regex", "^" + PostsCollectionsPrefix}}}}
	collectionNames, err := s.mongoService.GetCollectionNames(s.mongoDbName, filter)
	if err != nil {
		return fmt.Errorf("unable to get collections: %w", err)
	}
	for _, collectionName := range collectionNames {
		filter := bson.D{{"_id", postUuid}}
		update := bson.D{{"$set", bson.D{{"state", entities.POST_STATE_DELETED}}}}
		err := s.mongoService.Update(s.mongoDbName, collectionName, filter, update)
		if err != nil {
			return fmt.Errorf("unable to delete post with UUID %v in collection %v, error: %w", postUuid, collectionName, err)
		}

	}
	return nil
}

func (s *MongoFeedService) StoreCommentAtFeeds(comment *CommentFromQueue) error {
	commentsCollectionName := s.GetCommentsCollectionNameByPost(comment.PostUuid.String())
	err := s.storeCommentAtFeed(commentsCollectionName, comment.CommentId, comment.CreateDate, comment.State)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("unable to store comment '%v' to collection '%v', error: %w", comment, commentsCollectionName, err)
	}
	return nil
}

func (s *MongoFeedService) UpdateCommentStateAtFeeds(comment *CommentFromQueue) error {
	collectionName := s.GetCommentsCollectionNameByPost(comment.PostUuid.String())

	filter := bson.D{{"_id", comment.CommentId}}
	update := bson.D{{"$set", bson.D{{"state", comment.State}}}}
	s.mongoService.Update(s.mongoDbName, collectionName, filter, update)

	return nil
}

func (s *MongoFeedService) DeleteCommentAtFeeds(comment *DeletedCommentForQueue) error {
	collectionName := s.GetCommentsCollectionNameByPost(comment.PostUuid.String())

	filter := bson.D{{"_id", comment.CommentId}}
	update := bson.D{{"$set", bson.D{{"state", entities.COMMENT_STATE_DELETED}}}}
	err := s.mongoService.Update(s.mongoDbName, collectionName, filter, update)
	if err != nil {
		return fmt.Errorf("unable to delete comment with post UUID %v and ID %v in collection %v, error: %w", comment.PostUuid, comment.CommentId, collectionName, err)
	}

	return nil
}

func (s *MongoFeedService) GetPostsCollectionNameByTag(tagId int) string {
	return fmt.Sprintf("%v%v", FeedByTagCollectionPrefix, tagId)
}

func (s *MongoFeedService) GetPostsCollectionNameByUser(userUuid string) string {
	return fmt.Sprintf("%v%v", FeedByUserCollectionPrefix, userUuid)
}

func (s *MongoFeedService) GetCommentsCollectionNameByPost(postUuid string) string {
	return fmt.Sprintf("%v%v", CommentsCollectionPrefix, postUuid)
}

func (s *MongoFeedService) GetPostsFeed(state string, limit int, offset int) ([]string, error) {
	filter := initFilter(state)
	return s.getPostsFeed(FeedCommonCollectionName, filter, limit, offset)
}

func (s *MongoFeedService) GetPostsFeedByTag(tagId int, state string, limit int, offset int) ([]string, error) {
	collectionName := s.GetPostsCollectionNameByTag(tagId)
	filter := initFilter(state)
	return s.getPostsFeed(collectionName, filter, limit, offset)
}

func (s *MongoFeedService) GetPostsFeedByUser(userUuid string, state string, limit int, offset int) ([]string, error) {
	collectionName := s.GetPostsCollectionNameByUser(userUuid)
	filter := initFilter(state)
	return s.getPostsFeed(collectionName, filter, limit, offset)
}

func (s *MongoFeedService) GetCommentsFeedByPost(postUuid string, limit int, offset int) ([]CommentInMongoDB, error) {
	collectionName := s.GetCommentsCollectionNameByPost(postUuid)
	filter := emptyFilter()
	return s.getCommentsFeed(collectionName, filter, limit, offset)
}

func (s *MongoFeedService) getPostsFeed(collectionName string, filter any, limit int, offset int) ([]string, error) {
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

func (s *MongoFeedService) getCommentsFeed(collectionName string, filter any, limit int, offset int) ([]CommentInMongoDB, error) {
	limit64 := int64(limit)
	offset64 := int64(offset)
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	opts := options.Find().SetSkip(offset64).SetLimit(limit64).SetSort(bson.D{{"createDate", 1}})
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}

	var result []CommentInMongoDB

	err = cursor.All(ctx, &result)
	if err != nil {
		return nil, err
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
		return fmt.Errorf("unable to store post with UUID '%v' to collection '%v', error: %w", uuid, collectionName, err)
	}

	return nil
}

func (s *MongoFeedService) storeCommentAtFeed(collectionName string, commentId int, createDate time.Time, state string) error {
	collection := s.mongoService.GetCollection(s.mongoDbName, collectionName)

	ctx, cancel := context.WithTimeout(context.Background(), s.mongoService.QueryTimeout)
	defer cancel()

	document := bson.D{{"_id", commentId}, {"createDate", createDate}, {"state", state}}
	_, err := collection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("unable to store comment with ID '%v' to collection '%v', error: %w", commentId, collectionName, err)
	}

	return nil
}

func parseMessageToPost(msg *kafka.Message) (*PostWithTagsFromQueue, error) {
	var result *PostWithTagsFromQueue
	err := json.Unmarshal([]byte(msg.Value), &result)
	if err != nil {
		return nil, fmt.Errorf("unable to parse message '%v' to *PostWithTagsFromQueue, error: %w", msg, err)
	}
	return result, nil
}

func parseMessageToUUID(msg *kafka.Message) (uuid.UUID, error) {
	var result uuid.UUID
	err := json.Unmarshal([]byte(msg.Value), &result)
	if err != nil {
		return result, fmt.Errorf("unable to parse message '%v' to uuid.UUID, error: %w", msg, err)
	}
	return result, nil
}

func parseMessageToComment(msg *kafka.Message) (*CommentFromQueue, error) {
	var result *CommentFromQueue
	err := json.Unmarshal([]byte(msg.Value), &result)
	if err != nil {
		return nil, fmt.Errorf("unable to parse message '%v' to *CommentFromQueue, error: %w", msg, err)
	}
	return result, nil
}

func parseMessageToDeletedComment(msg *kafka.Message) (*DeletedCommentForQueue, error) {
	var result *DeletedCommentForQueue
	err := json.Unmarshal([]byte(msg.Value), &result)
	if err != nil {
		return nil, fmt.Errorf("unable to parse message '%v' to *DeletedCommentForQueue, error: %w", msg, err)
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
