package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	redisService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/redis"
	"github.com/go-redis/redis/v8"
)

/*
TODO:
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

*/

const (
	REDIS_POSTS_KEY    = "posts"
	REDIS_FEED_KEY     = "feed"
	REDIS_COMMENTS_KEY = "comments"
)

type FeedService struct {
	redis *redisService.RedisService
}

func CreateFeedService() *FeedService {
	return &FeedService{
		redis: redisService.CreateRedisService(),
	}
}

func (s *FeedService) Shutdown() error {
	return s.redis.Shutdown()
}

func (s *FeedService) CreatePost(post *entities.FeedPost) error {
	postKey := PostKey(post.PostId)
	postVal, err := ToJsonString(post)
	if err != nil {
		return err
	}
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_POSTS_KEY, postKey, postVal).Err()
		if err != nil {
			return err
		}
		return err
	})()
}

func (s *FeedService) DeletePost(postId int) error {
	postKey := PostKey(postId)
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	return s.redis.WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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

type FeedBlock struct {
	PostId          int
	PostPreviewText string
	PostTopic       string
	AuthorId        int
	AuthorName      string
	CreateDate      time.Time
	CommentsCount   int64
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

func (s *FeedService) GetFeed(offset int, limit int) ([]FeedBlock, error) {
	data, err := s.redis.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		postIds, err := cli.ZRangeByScore(ctx, REDIS_FEED_KEY, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    "+inf",
			Offset: int64(offset),
			Count:  int64(limit),
		}).Result()
		if err != nil {
			return nil, err
		}
		result := make([]FeedBlock, 0, limit)
		for _, postId := range postIds {
			postStr, err := cli.HGet(ctx, REDIS_POSTS_KEY, PostKeyStr(postId)).Result()
			if err != nil {
				return nil, fmt.Errorf("unable to get feed post: %v", err)
			}
			var post entities.FeedPost
			err = json.Unmarshal([]byte(postStr), &post)
			if err != nil {
				return nil, fmt.Errorf("unable to get unmarshal feed post: %v", err)
			}

			postCommentsKey := PostCommentsKeyStr(postId)
			commentsCount, err := cli.ZCount(ctx, postCommentsKey, "-inf", "+inf").Result()
			if err != nil {
				return nil, fmt.Errorf("unable to get feed post comments count: %v", err)
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

type FullPostInfo struct {
	Post     entities.FeedPost
	Comments []entities.FeedComment
}

func (s *FeedService) GetPost(postId int) (*FullPostInfo, error) {
	data, err := s.redis.WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		postJsonStr, err := cli.HGet(ctx, REDIS_POSTS_KEY, PostKey(postId)).Result()
		if err != nil {
			return nil, fmt.Errorf("unable to get feed post: %v", err)
		}
		var resultPost entities.FeedPost
		err = json.Unmarshal([]byte(postJsonStr), &resultPost)
		if err != nil {
			return nil, fmt.Errorf("unable to get unmarshal feed post: %v", err)
		}

		postCommentsKey := PostCommentsKey(postId)
		commentIds, err := cli.ZRangeByScore(ctx, postCommentsKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()

		if err != nil {
			return nil, err
		}
		resultComments := make([]entities.FeedComment, 0)
		for _, commentId := range commentIds {
			commentJsonStr, err := cli.HGet(ctx, REDIS_COMMENTS_KEY, CommentKeyStr(commentId)).Result()
			if err != nil {
				return nil, fmt.Errorf("unable to get feed comment: %v", err)
			}
			var comment entities.FeedComment
			err = json.Unmarshal([]byte(commentJsonStr), &comment)
			if err != nil {
				return nil, fmt.Errorf("unable to get unmarshal feed comment: %v", err)
			}
			resultComments = append(resultComments, comment)
		}

		return &FullPostInfo{Post: resultPost, Comments: resultComments}, err
	})()
	result, ok := data.(*FullPostInfo)
	if !ok {
		return nil, fmt.Errorf("unable cast to FullPostInfo")
	}
	return result, err
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
