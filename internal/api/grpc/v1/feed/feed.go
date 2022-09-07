package feed

import (
	"context"
	"fmt"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"google.golang.org/grpc"
)

type FeedBuilderServiceServer struct {
	feed.UnimplementedFeedBuilderServiceServer
}

func RegisterServiceServer(s *grpc.Server) {
	feed.RegisterFeedBuilderServiceServer(s, &FeedBuilderServiceServer{})
}

func (s *FeedBuilderServiceServer) CreatePost(ctx context.Context, in *feed.CreatePostRequest) (*feed.CreatePostReply, error) {
	// TODO: use local cache for getting user name
	user, err := services.Instance().Profiles().GetUser(int32(in.AuthorId))
	if err != nil {
		return nil, err
	}
	post, err := toFeedPost(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Feed().CreatePost(post)
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
	post, err := toFeedPost(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Feed().UpdatePost(post)
	if err != nil {
		return nil, err
	}
	return &feed.UpdatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) DeletePost(ctx context.Context, in *feed.DeletePostRequest) (*feed.DeletePostReply, error) {
	err := services.Instance().Feed().DeletePost(int(in.GetId()))
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
	comment, err := toFeedComment(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Feed().CreateComment(comment)
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
	comment, err := toFeedComment(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Feed().UpdateComment(comment)
	if err != nil {
		return nil, err
	}
	return &feed.UpdateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) DeleteComment(ctx context.Context, in *feed.DeleteCommentRequest) (*feed.DeleteCommentReply, error) {
	err := services.Instance().Feed().DeleteComment(int(in.GetPostId()), int(in.GetCommentId()))
	if err != nil {
		return nil, err
	}
	return &feed.DeleteCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateUser(ctx context.Context, in *feed.UpdateUserRequest) (*feed.UpdateUserReply, error) {
	// TODO:
	return nil, fmt.Errorf("NOT IMPLEMENTED")
}

func (s *FeedBuilderServiceServer) DeleteUser(ctx context.Context, in *feed.DeleteUserRequest) (*feed.DeleteUserReply, error) {
	// TODO:
	return nil, fmt.Errorf("NOT IMPLEMENTED")
}

func toFeedPost(post any, authorName string) (*entities.FeedPost, error) {
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
	default:
		return nil, fmt.Errorf("unknown type of post: %T", post)
	}
}

func toFeedComment(comment any, authorName string) (*entities.FeedComment, error) {
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
