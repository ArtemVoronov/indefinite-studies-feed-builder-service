package feed

import (
	"context"
	"fmt"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	feedService "github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
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
	post, err := feedService.ToFeedPost(in, user.Login)
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
	post, err := feedService.ToFeedPost(in, user.Login)
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
	comment, err := feedService.ToFeedComment(in, user.Login)
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
	comment, err := feedService.ToFeedComment(in, user.Login)
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
