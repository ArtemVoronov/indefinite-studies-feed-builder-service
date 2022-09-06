package feed

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
)

// TODO: implement update and delete user ops

type FeedBuilderServiceServer struct {
	feed.UnimplementedFeedBuilderServiceServer
}

func RegisterServiceServer(s *grpc.Server) {
	feed.RegisterFeedBuilderServiceServer(s, &FeedBuilderServiceServer{})
}

const (
	REDIS_POSTS_KEY    = "posts"
	REDIS_FEED_KEY     = "feed"
	REDIS_COMMENTS_KEY = "comments"
)

type PostRedis struct {
	PostId     int
	CreateDate time.Time
}

type CommentRedis struct {
	CommentId  int
	PostId     int
	CreateDate time.Time
}

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

func PostKey(id int32) string {
	return "post_" + strconv.Itoa(int(id))
}
func PostKey2(id string) string {
	return "post_" + id
}
func CommentKey(id int32) string {
	return "comment_" + strconv.Itoa(int(id))
}
func PostCommentsKey(id int32) string {
	return "post_" + strconv.Itoa(int(id)) + "_comments"
}

func (s *FeedBuilderServiceServer) CreatePost(ctx context.Context, in *feed.CreatePostRequest) (*feed.CreatePostReply, error) {
	// TODO: use local cache for getting user name
	user, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}
	postKey := PostKey(in.GetId())
	post := toFeedPostCreate(in, user.Login)
	if err != nil {
		return nil, err
	}
	postVal, err := toJsonString(post)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	if err != nil {
		return nil, err
	}
	return &feed.CreatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdatePost(ctx context.Context, in *feed.UpdatePostRequest) (*feed.UpdatePostReply, error) {
	// TODO: use local cache for getting user name
	user, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}
	postKey := PostKey(in.GetId())
	post := toFeedPostUpdate(in, user.Login)
	if err != nil {
		return nil, err
	}
	postVal, err := toJsonString(post)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_POSTS_KEY, postKey, postVal).Err()
		if err != nil {
			return err
		}
		return err
	})()
	if err != nil {
		return nil, err
	}
	return &feed.UpdatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) DeletePost(ctx context.Context, in *feed.DeletePostRequest) (*feed.DeletePostReply, error) {
	postKey := PostKey(in.GetId())
	err := services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, REDIS_FEED_KEY, int(in.GetId())).Err()
		if err != nil {
			return err
		}
		err = cli.HDel(ctx, REDIS_POSTS_KEY, postKey).Err()
		if err != nil {
			return err
		}
		return err
	})()
	if err != nil {
		return nil, err
	}

	return &feed.DeletePostReply{}, nil
}

func (s *FeedBuilderServiceServer) CreateComment(ctx context.Context, in *feed.CreateCommentRequest) (*feed.CreateCommentReply, error) {
	// TODO: use local cache for getting user name
	user, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}
	postCommentsKey := PostCommentsKey(in.GetPostId())
	commentKey := CommentKey(in.GetId())
	comment := toFeedCommentCreate(in, user.Login)
	if err != nil {
		return nil, err
	}
	commentVal, err := toJsonString(comment)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
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
	if err != nil {
		return nil, err
	}

	return &feed.CreateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateComment(ctx context.Context, in *feed.UpdateCommentRequest) (*feed.UpdateCommentReply, error) {
	// TODO: use local cache for getting user name
	user, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}
	commentKey := CommentKey(in.GetId())
	comment := toFeedCommentUpdate(in, user.Login)
	if err != nil {
		return nil, err
	}
	commentVal, err := toJsonString(comment)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.HSet(ctx, REDIS_COMMENTS_KEY, commentKey, commentVal).Err()
		if err != nil {
			return err
		}
		return err
	})()
	if err != nil {
		return nil, err
	}
	return &feed.UpdateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) DeleteComment(ctx context.Context, in *feed.DeleteCommentRequest) (*feed.DeleteCommentReply, error) {
	commentKey := CommentKey(in.GetCommentId())
	postCommentsKey := PostCommentsKey(in.GetPostId())
	err := services.Instance().Redis().WithTimeoutVoid(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) error {
		err := cli.ZRem(ctx, postCommentsKey, int(in.GetCommentId())).Err()
		if err != nil {
			return err
		}
		err = cli.HDel(ctx, REDIS_COMMENTS_KEY, commentKey).Err()
		if err != nil {
			return err
		}
		return err
	})()
	if err != nil {
		return nil, err
	}
	return &feed.DeleteCommentReply{}, nil
}

// func toCreateFeedPostParams(post *feed.CreatePostRequest, authorName string) *queries.CreateFeedPostParams {
// 	return &queries.CreateFeedPostParams{
// 		AuthorId:        post.AuthorId,
// 		AuthorName:      authorName,
// 		PostId:          post.Id,
// 		PostText:        post.Text,
// 		PostPreviewText: post.PreviewText,
// 		PostTopic:       post.Topic,
// 		PostState:       post.State,
// 		CreateDate:      post.CreateDate.AsTime(),
// 		LastUpdateDate:  post.LastUpdateDate.AsTime(),
// 	}
// }

// func toUpdateFeedPostParams(post *feed.UpdatePostRequest, authorName string) *queries.UpdateFeedPostParams {
// 	return &queries.UpdateFeedPostParams{
// 		AuthorId:        post.AuthorId,
// 		AuthorName:      authorName,
// 		PostId:          post.Id,
// 		PostText:        post.Text,
// 		PostPreviewText: post.PreviewText,
// 		PostTopic:       post.Topic,
// 		PostState:       post.State,
// 		CreateDate:      post.CreateDate.AsTime(),
// 		LastUpdateDate:  post.LastUpdateDate.AsTime(),
// 	}
// }

// func toCreateFeedCommentParams(comment *feed.CreateCommentRequest, authorName string) *queries.CreateFeedCommentParams {
// 	return &queries.CreateFeedCommentParams{
// 		AuthorId:        comment.AuthorId,
// 		AuthorName:      authorName,
// 		PostId:          comment.PostId,
// 		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
// 		CommentId:       comment.Id,
// 		CommentText:     comment.Text,
// 		CommentState:    comment.State,
// 		CreateDate:      comment.CreateDate.AsTime(),
// 		LastUpdateDate:  comment.LastUpdateDate.AsTime(),
// 	}
// }

// func toUpdateFeedCommentParams(comment *feed.UpdateCommentRequest, authorName string) *queries.UpdateFeedCommentParams {
// 	return &queries.UpdateFeedCommentParams{
// 		AuthorId:        comment.AuthorId,
// 		AuthorName:      authorName,
// 		PostId:          comment.PostId,
// 		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
// 		CommentId:       comment.Id,
// 		CommentText:     comment.Text,
// 		CommentState:    comment.State,
// 		CreateDate:      comment.CreateDate.AsTime(),
// 		LastUpdateDate:  comment.LastUpdateDate.AsTime(),
// 	}
// }

func toFeedPostCreate(post *feed.CreatePostRequest, authorName string) *entities.FeedPost {
	return &entities.FeedPost{
		AuthorId:        int(post.AuthorId),
		AuthorName:      authorName,
		PostId:          int(post.Id),
		PostText:        post.Text,
		PostPreviewText: post.PreviewText,
		PostTopic:       post.Topic,
		PostState:       post.State,
		CreateDate:      post.CreateDate.AsTime(),
		LastUpdateDate:  post.LastUpdateDate.AsTime(),
	}
}

func toFeedPostUpdate(post *feed.UpdatePostRequest, authorName string) *entities.FeedPost {
	return &entities.FeedPost{
		AuthorId:        int(post.AuthorId),
		AuthorName:      authorName,
		PostId:          int(post.Id),
		PostText:        post.Text,
		PostPreviewText: post.PreviewText,
		PostTopic:       post.Topic,
		PostState:       post.State,
		CreateDate:      post.CreateDate.AsTime(),
		LastUpdateDate:  post.LastUpdateDate.AsTime(),
	}
}

func toFeedCommentCreate(comment *feed.CreateCommentRequest, authorName string) *entities.FeedComment {
	return &entities.FeedComment{
		AuthorId:        int(comment.AuthorId),
		AuthorName:      authorName,
		PostId:          int(comment.PostId),
		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
		CommentId:       int(comment.Id),
		CommentText:     comment.Text,
		CommentState:    comment.State,
		CreateDate:      comment.CreateDate.AsTime(),
		LastUpdateDate:  comment.LastUpdateDate.AsTime(),
	}
}

func toFeedCommentUpdate(comment *feed.UpdateCommentRequest, authorName string) *entities.FeedComment {
	return &entities.FeedComment{
		AuthorId:        int(comment.AuthorId),
		AuthorName:      authorName,
		PostId:          int(comment.PostId),
		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
		CommentId:       int(comment.Id),
		CommentText:     comment.Text,
		CommentState:    comment.State,
		CreateDate:      comment.CreateDate.AsTime(),
		LastUpdateDate:  comment.LastUpdateDate.AsTime(),
	}
}

func toLinkedCommentIdPrt(val int32) *int {
	if val == 0 {
		return nil
	}
	result := int(val)
	return &result
}

func toJsonString(obj any) (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
