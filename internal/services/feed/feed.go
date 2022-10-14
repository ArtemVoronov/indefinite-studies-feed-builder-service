package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/posts"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	redisService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/redis"
	"github.com/go-redis/redis/v8"
)

/*
Basic implementation of feed:
HASHMAP1 (REDIS_POSTS_KEY): post_uuid -> {full json post info}
SORTEDSET1 (REDIS_FEED_KEY): [(create_date, post_uuid) ... (create_date, post_uuid)]

HASHMAP2 (REDIS_COMMENTS_KEY): (comment_uuid) -> {full json comment info}
SORTEDSET2 (post_uuid_comments): [(create_date, comment_uuid) ... (create_date, comment_uuid)]

HASHMAP3 (REDIS_USERS_KEY): (user_uuid) -> {full json user info}

Use-cases:
1. create post -> add to HASHMAP1 and SORTEDSET1
2. update post -> update key in HASHMAP1
3. delete post -> delete key from HASHMAP1 and SORTEDSET1
4. create comment -> add to HASHMAP2 and SORTEDSET2
5. update comment -> update key in HASHMAP2
6. delete comment -> delete key from HASHMAP2 and SORTEDSET2

7. get feed ->
	- get pairs from REDIS_FEED_POSTS_KEY (e.g. first ten, it is sorted by create date in desc order)
	- get post by post_uuid for each pair from HASHMAP1
	- get post comments count for each part from SORTEDSET2
	- return feed object (as array of posts with comments counter)
8. get post ->
	- get post by uuid from HASHMAP1
	- get all comments by id from SORTEDSET2
9. sync feed:
	- clear all collections (post_uuid_comments...), REDIS_POSTS_KEY, REDIS_FEED_KEY, REDIS_COMMENTS_KEY
	- load all posts and comments from posts service via gGRPC

10. update user -> update key in HASHMAP3 and iterate through all posts and comments, if AuthroUuid == user.uuid, then update the post and its comments
*/

const (
	REDIS_POSTS_KEY    = "posts"
	REDIS_FEED_KEY     = "feed"
	REDIS_COMMENTS_KEY = "comments"
	REDIS_USERS_KEY    = "users"
)

var (
	ErrorRedisNotFound = errors.New("REDIS_ERROR_NOT_FOUND")
)

type FeedBlock struct {
	PostUuid        string
	PostPreviewText string
	PostTopic       string
	AuthorUuid      string
	AuthorName      string
	CreateDate      time.Time
	CommentsCount   int64
	Tags            []string
}

type FullPostInfo struct {
	Post        entities.FeedPost
	Comments    []entities.FeedComment
	CommentsMap map[string]FeedCommentWithIndex
}

type FeedService struct {
	redisService    *redisService.RedisService
	postsService    *posts.PostsGRPCService
	profilesService *profiles.ProfilesGRPCService
	SyncGuard       sync.RWMutex
}

type FeedCommentWithIndex struct {
	Index int
	entities.FeedComment
}

func CreateFeedService(postsService *posts.PostsGRPCService, profilesService *profiles.ProfilesGRPCService) *FeedService {
	return &FeedService{
		redisService:    redisService.CreateRedisService(),
		postsService:    postsService,
		profilesService: profilesService,
	}
}

func (s *FeedService) Shutdown() error {
	result := []error{}
	err := s.redisService.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.postsService.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.profilesService.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	if len(result) > 0 {
		return fmt.Errorf("errors during feed service shutdown: %v", result)
	}
	return nil
}

func (s *FeedService) CreatePost(post *entities.FeedPost) error {
	postKey := PostKey(post.PostUuid)
	postVal, err := ToJsonString(post)
	if err != nil {
		return err
	}
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_POSTS_KEY, postKey, postVal).Err()
		if err != nil {
			return err
		}
		err = cli.ZAdd(ctx, REDIS_FEED_KEY, &redis.Z{
			Score:  float64(post.CreateDate.Unix() * -1),
			Member: post.PostUuid,
		}).Err()
		return err
	})()
}

func (s *FeedService) UpdatePost(post *entities.FeedPost) error {
	postKey := PostKey(post.PostUuid)
	postVal, err := ToJsonString(post)
	if err != nil {
		return err
	}
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_POSTS_KEY, postKey, postVal).Err()
		if err != nil {
			return err
		}
		return err
	})()
}

func (s *FeedService) DeletePost(postUuid string) error {
	postKey := PostKey(postUuid)
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, REDIS_FEED_KEY, postUuid).Err()
		if err != nil {
			return err
		}
		err = cli.HDel(ctx, REDIS_POSTS_KEY, postKey).Err()
		if err != nil {
			return err
		}
		return err
	})()
}

func (s *FeedService) CreateComment(comment *entities.FeedComment) error {
	postCommentsKey := PostCommentsKey(comment.PostUuid)
	commentKey := CommentKey(comment.CommentUuid)
	commentVal, err := ToJsonString(comment)
	if err != nil {
		return err
	}
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_COMMENTS_KEY, commentKey, commentVal).Err()
		if err != nil {
			return err
		}
		err = cli.ZAdd(ctx, postCommentsKey, &redis.Z{
			Score:  float64(comment.CreateDate.Unix()),
			Member: comment.CommentUuid,
		}).Err()
		return err
	})()
}

func (s *FeedService) UpdateComment(comment *entities.FeedComment) error {
	commentKey := CommentKey(comment.CommentUuid)
	commentVal, err := ToJsonString(comment)
	if err != nil {
		return err
	}
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_COMMENTS_KEY, commentKey, commentVal).Err()
		if err != nil {
			return err
		}
		return err
	})()
}

func (s *FeedService) DeleteComment(postUuid string, commentUuid string) error {
	commentKey := CommentKey(commentUuid)
	postCommentsKey := PostCommentsKey(postUuid)
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, postCommentsKey, commentUuid).Err()
		if err != nil {
			return err
		}
		err = cli.HDel(ctx, REDIS_COMMENTS_KEY, commentKey).Err()
		if err != nil {
			return err
		}
		return err
	})()
}

func (s *FeedService) GetFeed(offset int, limit int) ([]FeedBlock, error) {
	s.SyncGuard.RLock()
	defer s.SyncGuard.RUnlock()
	data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		postUuids, err := getPostUuids(offset, limit, cli, ctx)
		if err != nil {
			return nil, err
		}
		result := make([]FeedBlock, 0, limit)
		for _, postUuid := range postUuids {
			post, err := getPost(PostKey(postUuid), cli, ctx)
			if err != nil {
				return nil, err
			}
			commentsCount, err := getCommentsCount(PostCommentsKey(postUuid), cli, ctx)
			if err != nil {
				return nil, err
			}
			feedBlock := toFeedBlock(&post, commentsCount)
			result = append(result, feedBlock)
		}

		return result, err
	})()

	result, ok := data.([]FeedBlock)
	if !ok {
		return nil, fmt.Errorf("unable cast to []FeedBlock")
	}
	return result, err
}

func (s *FeedService) GetPost(postUuid string) (*FullPostInfo, error) {
	s.SyncGuard.RLock()
	defer s.SyncGuard.RUnlock()
	data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		resultPost, err := getPost(PostKey(postUuid), cli, ctx)
		if err != nil {
			return nil, err
		}
		commentUuids, err := getCommentsUuids(PostCommentsKey(postUuid), cli, ctx)
		if err != nil {
			return nil, err
		}
		resultComments, resultCommentsMap, err := getComments(toCommentKeys(commentUuids), cli, ctx)
		if err != nil {
			return nil, err
		}

		return &FullPostInfo{Post: resultPost, Comments: resultComments, CommentsMap: resultCommentsMap}, err
	})()
	if err != nil {
		return nil, err
	}
	result, ok := data.(*FullPostInfo)
	if !ok {
		return nil, fmt.Errorf("unable cast to FullPostInfo")
	}
	return result, err
}

func (s *FeedService) UpsertUser(user *profiles.GetUserResult) error {
	userKey := UserKey(user.Uuid)
	userVal, err := ToJsonString(user)
	if err != nil {
		return err
	}
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		return cli.HSet(ctx, REDIS_USERS_KEY, userKey, userVal).Err()
	})()
}

func (s *FeedService) GetUser(userUuid string) (*profiles.GetUserResult, error) {
	data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		return getUser(UserKey(userUuid), cli, ctx)
	})()
	if err != nil {
		return nil, err
	}
	result, ok := data.(profiles.GetUserResult)
	if !ok {
		return nil, fmt.Errorf("unable cast to profiles.GetUserResult")
	}
	return &result, err
}

func (s *FeedService) SyncUserDataInFeed(user *profiles.GetUserResult) error {
	return s.syncUserDataInPosts(user)
}

func (s *FeedService) syncUserDataInPosts(updatedUser *profiles.GetUserResult) error {
	var offset int = 0
	var limit int = 50

	for {
		data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
			postUuids, err := getPostUuids(offset, limit, cli, ctx)
			if err != nil {
				return nil, err
			}
			for _, postUuid := range postUuids {
				post, err := getPost(PostKey(postUuid), cli, ctx)
				if err != nil {
					return nil, err
				}
				if post.AuthorUuid == updatedUser.Uuid {
					post.AuthorName = updatedUser.Login
					s.UpdatePost(&post)
				}
				commentUuids, err := getCommentsUuids(PostCommentsKey(post.PostUuid), cli, ctx)
				if err != nil {
					return nil, err
				}
				comments, _, err := getComments(toCommentKeys(commentUuids), cli, ctx)
				if err != nil {
					return nil, err
				}
				for _, comment := range comments {
					if comment.AuthorUuid == updatedUser.Uuid {
						comment.AuthorName = updatedUser.Login
						s.UpdateComment(&comment)
					}
				}
			}

			return postUuids, err
		})()
		if err != nil {
			return fmt.Errorf("unable to SyncUserDataInFeed: %v", err)
		}
		postUuids, ok := data.([]string)
		if !ok {
			return fmt.Errorf("unable to SyncUserDataInFeed: %v", "unable to cast to data to []string (as post ids)")
		}
		if len(postUuids) <= 0 {
			return nil
		}

		if len(postUuids) < limit {
			break
		}

		offset += limit
	}

	return nil
}

func (s *FeedService) Sync() error {
	s.SyncGuard.Lock()
	defer s.SyncGuard.Unlock()
	err := s.syncUsers()
	if err != nil {
		return err
	}
	return s.syncPosts()
}

func (s *FeedService) Clear() error {
	s.SyncGuard.Lock()
	defer s.SyncGuard.Unlock()
	return s.clear()
}

func (s *FeedService) syncUsers() error {
	var shardCount int32 = -1
	var shard int32 = 0
	for {
		var offset int32 = 0
		var limit int32 = 50
		for {
			reply, err := s.profilesService.GetUsers(offset, limit, shard)
			if err != nil {
				return fmt.Errorf("unable to syncUsers: %v", err)
			}
			if shardCount < 0 {
				shardCount = reply.GetShardsCount()
				log.Info(fmt.Sprintf("users shard count: %v", shardCount))
			}
			users := profiles.ToGetGetUserResultSlice(reply.GetUsers())

			if len(users) <= 0 {
				break
			}

			for _, user := range users {
				err := s.UpsertUser(&user)
				if err != nil {
					return fmt.Errorf("unable to syncUsers: %v", err)
				}
			}

			if len(users) < int(limit) {
				break
			}

			offset += limit
		}

		shard += 1

		if int(shard) >= int(shardCount) {
			break
		}
	}
	return nil
}

func (s *FeedService) syncPosts() error {
	var shardCount int32 = -1
	var shard int32 = 0
	for {
		var offset int32 = 0
		var limit int32 = 50
		for {
			reply, err := s.postsService.GetPosts(offset, limit, shard)
			if err != nil {
				return fmt.Errorf("unable to syncPosts: %v", err)
			}
			if shardCount < 0 {
				shardCount = reply.GetShardsCount()
				log.Info(fmt.Sprintf("posts shard count: %v", shardCount))
			}
			posts := posts.ToGetPostsResultSlice(reply.GetPosts())

			if len(posts) <= 0 {
				break
			}
			for _, post := range posts {
				user, err := s.GetUser(post.AuthorUuid)
				if err != nil {
					return fmt.Errorf("unable to syncPosts due to problem of getting user from cache: %v", err)
				}
				feedPost, convertErr := ToFeedPost(post, user.Login)
				if convertErr != nil {
					return fmt.Errorf("unable to syncPosts: %v", convertErr)
				}

				createFeedPostErr := s.CreatePost(feedPost)
				if createFeedPostErr != nil {
					return fmt.Errorf("unable to syncPosts, unable save to store the feed post with Uuid: %v", feedPost.PostUuid)
				}
				s.syncComments(post.Uuid)
			}

			if len(posts) < int(limit) {
				break
			}

			offset += limit
		}

		shard += 1

		if int(shard) >= int(shardCount) {
			break
		}
	}
	return nil
}

func (s *FeedService) syncComments(postUuid string) error {
	var offset int32 = 0
	var limit int32 = 50
	for {
		commentReplies, err := s.postsService.GetComments(postUuid, offset, limit)
		if err != nil {
			return fmt.Errorf("unable to syncComments: %v", err)
		}
		if len(commentReplies) <= 0 {
			return nil
		}
		for _, commentReply := range commentReplies {
			user, err := s.GetUser(commentReply.AuthorUuid)
			if err != nil {
				return fmt.Errorf("unable to syncComments due to problem of getting user from cache: %v", err)
			}
			feedComment, convertErr := ToFeedComment(commentReply, user.Login)
			if convertErr != nil {
				return fmt.Errorf("unable to syncComments: %v", convertErr)
			}

			createFeedCommentErr := s.CreateComment(feedComment)
			if createFeedCommentErr != nil {
				return fmt.Errorf("unable to syncComments, unable save to store the feed comment with UUID: %v. Post UUID: %v", feedComment.CommentUuid, feedComment.PostUuid)
			}
		}

		if len(commentReplies) < int(limit) {
			break
		}

		offset += limit
	}

	return nil
}

func (s *FeedService) clear() error {
	var offset int = 0
	var limit int = 50
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {

		for {
			postUuids, err := getPostUuids(offset, limit, cli, ctx)
			if err != nil {
				return fmt.Errorf("unable to clear feed: %v", err)
			}
			if len(postUuids) <= 0 {
				return nil
			}

			for _, postUuid := range postUuids {
				postCommentsKey := PostCommentsKey(postUuid)
				if err := cli.Del(ctx, postCommentsKey).Err(); err != nil {
					return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", postCommentsKey, err)
				}
			}

			if len(postUuids) < int(limit) {
				break
			}

			offset += limit
		}

		if err := cli.Del(ctx, REDIS_COMMENTS_KEY).Err(); err != nil {
			return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", REDIS_COMMENTS_KEY, err)
		}

		if err := cli.Del(ctx, REDIS_FEED_KEY).Err(); err != nil {
			return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", REDIS_FEED_KEY, err)
		}

		if err := cli.Del(ctx, REDIS_POSTS_KEY).Err(); err != nil {
			return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", REDIS_POSTS_KEY, err)
		}

		if err := cli.Del(ctx, REDIS_USERS_KEY).Err(); err != nil {
			return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", REDIS_USERS_KEY, err)
		}

		return nil
	})()
}

func UserKey(userUuid string) string {
	return "user_" + userUuid
}

func PostKey(postUuid string) string {
	return "post_" + postUuid
}

func CommentKey(commentUuid string) string {
	return "comment_" + commentUuid
}

func PostCommentsKey(postUuid string) string {
	return "post_" + postUuid + "_comments"
}

func ToJsonString(obj any) (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func getPostUuids(offset int, limit int, cli *redis.Client, ctx context.Context) ([]string, error) {
	return cli.ZRangeByScore(ctx, REDIS_FEED_KEY, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: int64(offset),
		Count:  int64(limit),
	}).Result()
}

func getCommentsUuids(postCommentsKey string, cli *redis.Client, ctx context.Context) ([]string, error) {
	return cli.ZRangeByScore(ctx, postCommentsKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
}

func getPost(postKey string, cli *redis.Client, ctx context.Context) (entities.FeedPost, error) {
	var post entities.FeedPost
	postStr, err := cli.HGet(ctx, REDIS_POSTS_KEY, postKey).Result()
	if postStr == "" {
		return post, ErrorRedisNotFound
	}
	if err != nil {
		return post, fmt.Errorf("unable to get feed post: %v", err)
	}
	err = json.Unmarshal([]byte(postStr), &post)
	if err != nil {
		return post, fmt.Errorf("unable to get unmarshal feed post: %v", err)
	}
	return post, nil
}

func getUser(userKey string, cli *redis.Client, ctx context.Context) (profiles.GetUserResult, error) {
	var user profiles.GetUserResult
	userStr, err := cli.HGet(ctx, REDIS_USERS_KEY, userKey).Result()
	if userStr == "" {
		return user, ErrorRedisNotFound
	}
	if err != nil {
		return user, fmt.Errorf("unable to get feed user: %v", err)
	}
	err = json.Unmarshal([]byte(userStr), &user)
	if err != nil {
		return user, fmt.Errorf("unable to unmarshal feed user: %v. userStr: %v", err, userStr)
	}
	return user, nil
}

func getCommentsCount(postCommentsKey string, cli *redis.Client, ctx context.Context) (int64, error) {
	commentsCount, err := cli.ZCount(ctx, postCommentsKey, "-inf", "+inf").Result()
	if err != nil {
		return 0, fmt.Errorf("unable to get feed post comments count: %v", err)
	}
	return commentsCount, nil
}

func getComments(commentKeys []string, cli *redis.Client, ctx context.Context) ([]entities.FeedComment, map[string]FeedCommentWithIndex, error) {
	resultComments := make([]entities.FeedComment, 0)
	resultCommentsMap := make(map[string]FeedCommentWithIndex)

	if len(commentKeys) <= 0 {
		return resultComments, resultCommentsMap, nil
	}

	commentVals, err := cli.HMGet(ctx, REDIS_COMMENTS_KEY, commentKeys...).Result()
	if err != nil {
		return nil, nil, err
	}

	for commentIndex, commentVal := range commentVals {
		commentJsonStr, ok := commentVal.(string)
		if !ok {
			return nil, nil, fmt.Errorf("unable cast comment json to string")
		}
		var comment entities.FeedComment
		err = json.Unmarshal([]byte(commentJsonStr), &comment)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get unmarshal feed comment: %v", err)
		}
		resultComments = append(resultComments, comment)
		resultCommentsMap[comment.CommentUuid] = FeedCommentWithIndex{Index: commentIndex, FeedComment: comment}
	}
	return resultComments, resultCommentsMap, nil
}

func toFeedBlock(post *entities.FeedPost, commentsCount int64) FeedBlock {
	return FeedBlock{
		PostUuid:        post.PostUuid,
		PostPreviewText: post.PostPreviewText,
		PostTopic:       post.PostTopic,
		AuthorUuid:      post.AuthorUuid,
		AuthorName:      post.AuthorName,
		CreateDate:      post.CreateDate,
		CommentsCount:   commentsCount,
		Tags:            post.Tags,
	}
}

func toCommentKeys(commentUuids []string) []string {
	commentKeys := make([]string, 0, len(commentUuids))
	for _, commentUuid := range commentUuids {
		commentKeys = append(commentKeys, CommentKey(commentUuid))
	}
	return commentKeys
}

func ToFeedPost(post any, authorName string) (*entities.FeedPost, error) {
	switch t := post.(type) {
	case *feed.CreatePostRequest:
		return &entities.FeedPost{
			AuthorUuid:      t.AuthorUuid,
			AuthorName:      authorName,
			PostUuid:        t.Uuid,
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
			Tags:            convertTags(t.Tags),
		}, nil
	case *feed.UpdatePostRequest:
		return &entities.FeedPost{
			AuthorUuid:      t.AuthorUuid,
			AuthorName:      authorName,
			PostUuid:        t.Uuid,
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
			Tags:            convertTags(t.Tags),
		}, nil
	case posts.GetPostResult:
		return &entities.FeedPost{
			AuthorUuid:      t.AuthorUuid,
			AuthorName:      authorName,
			PostUuid:        t.Uuid,
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate,
			LastUpdateDate:  t.LastUpdateDate,
			Tags:            convertTags(t.Tags),
		}, nil
	default:
		return nil, fmt.Errorf("unknown type of post: %T", post)
	}
}

func ToFeedComment(comment any, authorName string) (*entities.FeedComment, error) {
	switch t := comment.(type) {
	case *feed.CreateCommentRequest:
		return &entities.FeedComment{
			AuthorUuid:        t.AuthorUuid,
			AuthorName:        authorName,
			PostUuid:          t.PostUuid,
			LinkedCommentUuid: t.LinkedCommentUuid,
			CommentId:         int(t.Id),
			CommentUuid:       t.Uuid,
			CommentText:       t.Text,
			CommentState:      t.State,
			CreateDate:        t.CreateDate.AsTime(),
			LastUpdateDate:    t.LastUpdateDate.AsTime(),
		}, nil
	case *feed.UpdateCommentRequest:
		return &entities.FeedComment{
			AuthorUuid:        t.AuthorUuid,
			AuthorName:        authorName,
			PostUuid:          t.PostUuid,
			LinkedCommentUuid: t.LinkedCommentUuid,
			CommentId:         int(t.Id),
			CommentUuid:       t.Uuid,
			CommentText:       t.Text,
			CommentState:      t.State,
			CreateDate:        t.CreateDate.AsTime(),
			LastUpdateDate:    t.LastUpdateDate.AsTime(),
		}, nil
	case posts.GetCommentResult:
		return &entities.FeedComment{
			AuthorUuid:        t.AuthorUuid,
			AuthorName:        authorName,
			PostUuid:          t.PostUuid,
			LinkedCommentUuid: t.LinkedCommentUuid,
			CommentId:         t.Id,
			CommentUuid:       t.Uuid,
			CommentText:       t.Text,
			CommentState:      t.State,
			CreateDate:        t.CreateDate,
			LastUpdateDate:    t.LastUpdateDate,
		}, nil
	default:
		return nil, fmt.Errorf("unknown type of comment: %T", comment)
	}
}

func convertTags(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	return in
}
