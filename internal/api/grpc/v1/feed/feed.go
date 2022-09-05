package feed

import (
	"context"
	"database/sql"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/queries"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"google.golang.org/grpc"
)

// TODO: implement update and delete user ops

type FeedBuilderServiceServer struct {
	feed.UnimplementedFeedBuilderServiceServer
}

func RegisterServiceServer(s *grpc.Server) {
	feed.RegisterFeedBuilderServiceServer(s, &FeedBuilderServiceServer{})
}

func (s *FeedBuilderServiceServer) CreatePost(ctx context.Context, in *feed.CreatePostRequest) (*feed.CreatePostReply, error) {
	// TODO: use local cache for getting user name
	result, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}

	params := toCreateFeedPostParams(in, result.Login)

	err = services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.CreateFeedPost(tx, ctx, params)
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().AddPost(*toFeedPostCreate(in, result.Login))

	return &feed.CreatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdatePost(ctx context.Context, in *feed.UpdatePostRequest) (*feed.UpdatePostReply, error) {
	// TODO: use local cache for getting user name
	result, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}

	params := toUpdateFeedPostParams(in, result.Login)

	err = services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.UpdateFeedPost(tx, ctx, params)
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().UpdatePost(*toFeedPostUpdate(in, result.Login))

	return &feed.UpdatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) DeletePost(ctx context.Context, in *feed.DeletePostRequest) (*feed.DeletePostReply, error) {
	err := services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.DeleteFeedPost(tx, ctx, int(in.GetId()))
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().DeletePost(int(in.GetId()))

	return &feed.DeletePostReply{}, nil
}

func (s *FeedBuilderServiceServer) CreateComment(ctx context.Context, in *feed.CreateCommentRequest) (*feed.CreateCommentReply, error) {
	// TODO: use local cache for getting user name
	result, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}

	params := toCreateFeedCommentParams(in, result.Login)

	err = services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.CreateFeedComment(tx, ctx, params)
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().AddComment(*toFeedCommentCreate(in, result.Login))

	return &feed.CreateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateComment(ctx context.Context, in *feed.UpdateCommentRequest) (*feed.UpdateCommentReply, error) {
	// TODO: use local cache for getting user name
	result, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}

	params := toUpdateFeedCommentParams(in, result.Login)

	err = services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.UpdateFeedComment(tx, ctx, params)
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().UpdateComment(*toFeedCommentUpdate(in, result.Login))

	return &feed.UpdateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) DeleteComment(ctx context.Context, in *feed.DeleteCommentRequest) (*feed.DeleteCommentReply, error) {
	err := services.Instance().DB().TxVoid(func(tx *sql.Tx, ctx context.Context, cancel context.CancelFunc) error {
		err := queries.DeleteFeedComment(tx, ctx, int(in.GetCommentId()))
		return err
	})()
	if err != nil {
		return nil, err
	}

	services.Instance().FeedCache().DeleteComment(int(in.GetPostId()), int(in.GetCommentId()))

	return &feed.DeleteCommentReply{}, nil
}

func toCreateFeedPostParams(post *feed.CreatePostRequest, authorName string) *queries.CreateFeedPostParams {
	return &queries.CreateFeedPostParams{
		AuthorId:        post.AuthorId,
		AuthorName:      authorName,
		PostId:          post.Id,
		PostText:        post.Text,
		PostPreviewText: post.PreviewText,
		PostTopic:       post.Topic,
		PostState:       post.State,
		CreateDate:      post.CreateDate,
		LastUpdateDate:  post.LastUpdateDate,
	}
}

func toUpdateFeedPostParams(post *feed.UpdatePostRequest, authorName string) *queries.UpdateFeedPostParams {
	return &queries.UpdateFeedPostParams{
		AuthorId:        post.AuthorId,
		AuthorName:      authorName,
		PostId:          post.Id,
		PostText:        post.Text,
		PostPreviewText: post.PreviewText,
		PostTopic:       post.Topic,
		PostState:       post.State,
		CreateDate:      post.CreateDate,
		LastUpdateDate:  post.LastUpdateDate,
	}
}

func toCreateFeedCommentParams(comment *feed.CreateCommentRequest, authorName string) *queries.CreateFeedCommentParams {
	return &queries.CreateFeedCommentParams{
		AuthorId:        comment.AuthorId,
		AuthorName:      authorName,
		PostId:          comment.PostId,
		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
		CommentId:       comment.Id,
		CommentText:     comment.Text,
		CommentState:    comment.State,
	}
}

func toUpdateFeedCommentParams(comment *feed.UpdateCommentRequest, authorName string) *queries.UpdateFeedCommentParams {
	return &queries.UpdateFeedCommentParams{
		AuthorId:        comment.AuthorId,
		AuthorName:      authorName,
		PostId:          comment.PostId,
		LinkedCommentId: toLinkedCommentIdPrt(comment.LinkedCommentId),
		CommentId:       comment.Id,
		CommentText:     comment.Text,
		CommentState:    comment.State,
	}
}

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
