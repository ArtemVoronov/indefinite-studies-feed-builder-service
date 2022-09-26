package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/posts"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	redisService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/redis"
	"github.com/go-redis/redis/v8"
)

/*
Basic implementation of feed:
HASHMAP1 (REDIS_POSTS_KEY): post_id -> {full json post info}
SORTEDSET1 (REDIS_FEED_KEY): [(create_date, post_id) ... (create_date, post_id)]

HASHMAP2 (REDIS_COMMENTS_KEY): (comment_id) -> {full json comment info}
SORTEDSET2 (post_id_comments): [(create_date, comment_id) ... (create_date, comment_id)]

Use-cases:
1. create post -> add to HASHMAP1 and SORTEDSET1
2. update post -> update key in HASHMAP1
3. delete post -> delete key from HASHMAP1 and SORTEDSET1
4. create comment -> add to HASHMAP2 and SORTEDSET2
5. update comment -> update key in HASHMAP2
6. delete comment -> delete key from HASHMAP2 and SORTEDSET2

7. get feed ->
	- get pairs from REDIS_FEED_POSTS_KEY (e.g. first ten, it is sorted by create date in desc order)
	- get post by post_id for each pair from HASHMAP1
	- get post comments count for each part from SORTEDSET2
	- return feed object (as array of posts with comments counter)
8. get post ->
	- get post by id from HASHMAP1
	- get all comments by id from SORTEDSET2
9. sync feed:
	- clear all collections (post_id_comments...), REDIS_POSTS_KEY, REDIS_FEED_KEY, REDIS_COMMENTS_KEY
	- load all posts and comments from posts service via gGRPC
*/

const (
	REDIS_POSTS_KEY    = "posts"
	REDIS_FEED_KEY     = "feed"
	REDIS_COMMENTS_KEY = "comments"
)

type FeedBlock struct {
	PostId          int
	PostPreviewText string
	PostTopic       string
	AuthorId        int
	AuthorName      string
	CreateDate      time.Time
	CommentsCount   int64
}

type FullPostInfo struct {
	Post        entities.FeedPost
	Comments    []entities.FeedComment
	CommentsMap map[int]entities.FeedComment
}

type FeedService struct {
	redisService    *redisService.RedisService
	postsService    *posts.PostsGRPCService
	profilesService *profiles.ProfilesGRPCService
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
	postKey := PostKey(post.PostId)
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
			Member: post.PostId,
		}).Err()
		return err
	})()
}

func (s *FeedService) UpdatePost(post *entities.FeedPost) error {
	postKey := PostKey(post.PostId)
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

func (s *FeedService) DeletePost(postId int) error {
	postKey := PostKey(postId)
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, REDIS_FEED_KEY, postId).Err()
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
	postCommentsKey := PostCommentsKey(comment.PostId)
	commentKey := CommentKey(comment.CommentId)
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
			Member: comment.CommentId,
		}).Err()
		return err
	})()
}

func (s *FeedService) UpdateComment(comment *entities.FeedComment) error {
	commentKey := CommentKey(comment.CommentId)
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

func (s *FeedService) DeleteComment(postId int, commentId int) error {
	commentKey := CommentKey(commentId)
	postCommentsKey := PostCommentsKey(postId)
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, postCommentsKey, commentId).Err()
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
	data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		postIds, err := getPostIds(offset, limit, cli, ctx)
		if err != nil {
			return nil, err
		}
		result := make([]FeedBlock, 0, limit)
		for _, postId := range postIds {
			post, err := getPost(PostKeyStr(postId), cli, ctx)
			if err != nil {
				return nil, err
			}
			commentsCount, err := getCommentsCount(PostCommentsKeyStr(postId), cli, ctx)
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

func (s *FeedService) GetPost(postId int) (*FullPostInfo, error) {
	data, err := s.redisService.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		resultPost, err := getPost(PostKey(postId), cli, ctx)
		if err != nil {
			return nil, err
		}
		commentIds, err := getCommentsIds(PostCommentsKey(postId), cli, ctx)
		if err != nil {
			return nil, err
		}
		resultComments, resultCommentsMap, err := getComments(toCommentKeys(commentIds), cli, ctx)
		if err != nil {
			return nil, err
		}

		return &FullPostInfo{Post: resultPost, Comments: resultComments, CommentsMap: resultCommentsMap}, err
	})()
	result, ok := data.(*FullPostInfo)
	if !ok {
		return nil, fmt.Errorf("unable cast to FullPostInfo")
	}
	return result, err
}

func (s *FeedService) Sync() error {
	// TODO: check concurrent create/update/delete events during syncing
	// TODO: add appropriate locks or op time comparings
	s.ClearFeed()
	return s.syncPosts()
}

func (s *FeedService) syncPosts() error {
	var offset int32 = 0
	var limit int32 = 50
	for {
		postReplies, err := s.postsService.GetPosts(offset, limit, nil)
		if err != nil {
			return fmt.Errorf("unable to syncPosts: %v", err)
		}
		if len(postReplies) <= 0 {
			return nil
		}

		userIds, err := toUserIds(postReplies)
		if err != nil {
			return fmt.Errorf("unable to syncPosts: %v", err)
		}
		userReplies, err := s.getUsers(userIds)
		if err != nil {
			return fmt.Errorf("unable to syncPosts: %v", err)
		}
		usersById := make(map[int]string)
		for _, userReply := range userReplies {
			usersById[userReply.Id] = userReply.Login
		}
		for _, postReply := range postReplies {
			authorName, ok := usersById[postReply.AuthorId]
			if !ok {
				return fmt.Errorf("unable to syncPosts, unable to find author with id: %v", postReply.AuthorId)
			}
			feedPost, convertErr := ToFeedPost(postReply, authorName)
			if convertErr != nil {
				return fmt.Errorf("unable to syncPosts: %v", convertErr)
			}

			createFeedPostErr := s.CreatePost(feedPost)
			if createFeedPostErr != nil {
				return fmt.Errorf("unable to syncPosts, unable save to store the feed post with ID: %v", feedPost.PostId)
			}
			s.syncComments(int32(postReply.Id))
		}

		if len(postReplies) < int(limit) {
			break
		}

		offset += limit
	}

	return nil
}
func (s *FeedService) syncComments(postId int32) error {
	var offset int32 = 0
	var limit int32 = 50
	for {
		commentReplies, err := s.postsService.GetComments(postId, offset, limit)
		if err != nil {
			return fmt.Errorf("unable to syncComments: %v", err)
		}
		if len(commentReplies) <= 0 {
			return nil
		}

		userIds, err := toUserIds(commentReplies)
		if err != nil {
			return fmt.Errorf("unable to syncComments: %v", err)
		}
		userReplies, err := s.getUsers(userIds)
		if err != nil {
			return fmt.Errorf("unable to syncComments: %v", err)
		}
		usersById := make(map[int]string)
		for _, userReply := range userReplies {
			usersById[userReply.Id] = userReply.Login
		}
		for _, commentReply := range commentReplies {
			authorName, ok := usersById[commentReply.AuthorId]
			if !ok {
				return fmt.Errorf("unable to syncComments, unable to find author with id: %v", commentReply.AuthorId)
			}
			feedComment, convertErr := ToFeedComment(commentReply, authorName)
			if convertErr != nil {
				return fmt.Errorf("unable to syncComments: %v", convertErr)
			}

			createFeedCommentErr := s.CreateComment(feedComment)
			if createFeedCommentErr != nil {
				return fmt.Errorf("unable to syncComments, unable save to store the feed comment with ID: %v. Post ID: %v", feedComment.CommentId, feedComment.PostId)
			}
		}

		if len(commentReplies) < int(limit) {
			break
		}

		offset += limit
	}

	return nil
}

func (s *FeedService) ClearFeed() error {
	var offset int = 0
	var limit int = 50
	return s.redisService.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {

		for {
			postIds, err := getPostIds(offset, limit, cli, ctx)
			if err != nil {
				return fmt.Errorf("unable to clear feed: %v", err)
			}
			if len(postIds) <= 0 {
				return nil
			}

			for _, postId := range postIds {
				postCommentsKey := PostCommentsKeyStr(postId)
				if err := cli.Del(ctx, postCommentsKey).Err(); err != nil {
					return fmt.Errorf("unable to clear feed, error during deleting '%v': %v", postCommentsKey, err)
				}
			}

			if len(postIds) < int(limit) {
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

		return nil
	})()
}

func (s *FeedService) getUsers(userIds []int32) ([]profiles.GetUserResult, error) {
	var offset int32 = 0
	var limit int32 = 50
	result := []profiles.GetUserResult{}
	for {
		userReplies, err := s.profilesService.GetUsers(offset, limit, userIds)
		if err != nil {
			return result, fmt.Errorf("unable to getUsers via gRPC: %v", err)
		}
		if len(userReplies) <= 0 {
			return result, nil
		}

		result = append(result, userReplies...)

		offset += limit
	}
}

func toUserIds(input any) ([]int32, error) {
	switch t := input.(type) {
	case []posts.GetPostResult:
		result := make([]int32, len(t))
		for i, p := range t {
			result[i] = int32(p.AuthorId)
		}
		return result, nil
	case []posts.GetCommentResult:
		result := make([]int32, len(t))
		for i, c := range t {
			result[i] = int32(c.AuthorId)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown type of input for 'toUserIds' func: %T", t)
	}
}

func PostKey(postId int) string {
	return PostKeyStr(strconv.Itoa(postId))
}

func PostKeyStr(postId string) string {
	return "post_" + postId
}

func CommentKey(commentId int) string {
	return CommentKeyStr(strconv.Itoa(commentId))
}

func CommentKeyStr(commentId string) string {
	return "comment_" + commentId
}

func PostCommentsKey(postId int) string {
	return PostCommentsKeyStr(strconv.Itoa(postId))
}

func PostCommentsKeyStr(postId string) string {
	return "post_" + postId + "_comments"
}

func ToJsonString(obj any) (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func getPostIds(offset int, limit int, cli *redis.Client, ctx context.Context) ([]string, error) {
	return cli.ZRangeByScore(ctx, REDIS_FEED_KEY, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: int64(offset),
		Count:  int64(limit),
	}).Result()
}

func getCommentsIds(postCommentsKey string, cli *redis.Client, ctx context.Context) ([]string, error) {
	return cli.ZRangeByScore(ctx, postCommentsKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
}

func getPost(postKey string, cli *redis.Client, ctx context.Context) (entities.FeedPost, error) {
	var post entities.FeedPost
	postStr, err := cli.HGet(ctx, REDIS_POSTS_KEY, postKey).Result()
	if err != nil {
		return post, fmt.Errorf("unable to get feed post: %v", err)
	}
	err = json.Unmarshal([]byte(postStr), &post)
	if err != nil {
		return post, fmt.Errorf("unable to get unmarshal feed post: %v", err)
	}
	return post, nil
}

func getCommentsCount(postCommentsKey string, cli *redis.Client, ctx context.Context) (int64, error) {
	commentsCount, err := cli.ZCount(ctx, postCommentsKey, "-inf", "+inf").Result()
	if err != nil {
		return 0, fmt.Errorf("unable to get feed post comments count: %v", err)
	}
	return commentsCount, nil
}

func getComments(commentKeys []string, cli *redis.Client, ctx context.Context) ([]entities.FeedComment, map[int]entities.FeedComment, error) {
	resultComments := make([]entities.FeedComment, 0)
	resultCommentsMap := make(map[int]entities.FeedComment)

	if len(commentKeys) <= 0 {
		return resultComments, resultCommentsMap, nil
	}

	commentVals, err := cli.HMGet(ctx, REDIS_COMMENTS_KEY, commentKeys...).Result()
	if err != nil {
		return nil, nil, err
	}

	for _, commentVal := range commentVals {
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
		resultCommentsMap[comment.CommentId] = comment
	}
	return resultComments, resultCommentsMap, nil
}

func toFeedBlock(post *entities.FeedPost, commentsCount int64) FeedBlock {
	return FeedBlock{
		PostId:          post.PostId,
		PostPreviewText: post.PostPreviewText,
		PostTopic:       post.PostTopic,
		AuthorId:        post.AuthorId,
		AuthorName:      post.AuthorName,
		CreateDate:      post.CreateDate,
		CommentsCount:   commentsCount,
	}
}

func toCommentKeys(commentIds []string) []string {
	commentKeys := make([]string, 0, len(commentIds))
	for _, commentId := range commentIds {
		commentKeys = append(commentKeys, CommentKeyStr(commentId))
	}
	return commentKeys
}

func ToFeedPost(post any, authorName string) (*entities.FeedPost, error) {
	switch t := post.(type) {
	case *feed.CreatePostRequest:
		return &entities.FeedPost{
			AuthorId:        int(t.AuthorId),
			AuthorName:      authorName,
			PostId:          int(t.Id),
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
		}, nil
	case *feed.UpdatePostRequest:
		return &entities.FeedPost{
			AuthorId:        int(t.AuthorId),
			AuthorName:      authorName,
			PostId:          int(t.Id),
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
		}, nil
	case posts.GetPostResult:
		return &entities.FeedPost{
			AuthorId:        t.AuthorId,
			AuthorName:      authorName,
			PostId:          t.Id,
			PostText:        t.Text,
			PostPreviewText: t.PreviewText,
			PostTopic:       t.Topic,
			PostState:       t.State,
			CreateDate:      t.CreateDate,
			LastUpdateDate:  t.LastUpdateDate,
		}, nil
	default:
		return nil, fmt.Errorf("unknown type of post: %T", post)
	}
}

func ToFeedComment(comment any, authorName string) (*entities.FeedComment, error) {
	switch t := comment.(type) {
	case *feed.CreateCommentRequest:
		return &entities.FeedComment{
			AuthorId:        int(t.AuthorId),
			AuthorName:      authorName,
			PostId:          int(t.PostId),
			LinkedCommentId: toLinkedCommentIdPrt(t.LinkedCommentId),
			CommentId:       int(t.Id),
			CommentText:     t.Text,
			CommentState:    t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
		}, nil
	case *feed.UpdateCommentRequest:
		return &entities.FeedComment{
			AuthorId:        int(t.AuthorId),
			AuthorName:      authorName,
			PostId:          int(t.PostId),
			LinkedCommentId: toLinkedCommentIdPrt(t.LinkedCommentId),
			CommentId:       int(t.Id),
			CommentText:     t.Text,
			CommentState:    t.State,
			CreateDate:      t.CreateDate.AsTime(),
			LastUpdateDate:  t.LastUpdateDate.AsTime(),
		}, nil
	case posts.GetCommentResult:
		return &entities.FeedComment{
			AuthorId:        t.AuthorId,
			AuthorName:      authorName,
			PostId:          t.PostId,
			LinkedCommentId: t.LinkedCommentId,
			CommentId:       t.Id,
			CommentText:     t.Text,
			CommentState:    t.State,
			CreateDate:      t.CreateDate,
			LastUpdateDate:  t.LastUpdateDate,
		}, nil
	default:
		return nil, fmt.Errorf("unknown type of comment: %T", comment)
	}
}

func toLinkedCommentIdPrt(val int32) *int {
	if val == 0 {
		return nil
	}
	result := int(val)
	return &result
}
