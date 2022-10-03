package feed

import (
	"context"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	feedService "github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	"google.golang.org/grpc"
)

type FeedBuilderServiceServer struct {
	feed.UnimplementedFeedBuilderServiceServer
}

func RegisterServiceServer(s *grpc.Server) {
	feed.RegisterFeedBuilderServiceServer(s, &FeedBuilderServiceServer{})
}

func (s *FeedBuilderServiceServer) CreatePost(ctx context.Context, in *feed.CreatePostRequest) (*feed.CreatePostReply, error) {
	user, err := s.getAndCacheUser(in.AuthorId)
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
	user, err := s.getAndCacheUser(in.AuthorId)
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
	user, err := s.getAndCacheUser(in.AuthorId)
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
	user, err := s.getAndCacheUser(in.AuthorId)
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
	user := &profiles.GetUserResult{
		Id:    int(in.GetId()),
		Login: in.GetLogin(),
		Email: in.GetEmail(),
		Role:  in.GetRole(),
		State: in.GetState(),
	}
	err := services.Instance().Feed().UpsertUser(user)
	if err != nil {
		return nil, err
	}
	err = services.Instance().Feed().SyncUserDataInFeed(user)
	if err != nil {
		return nil, err
	}
	return &feed.UpdateUserReply{}, nil
}

func (s *FeedBuilderServiceServer) getAndCacheUser(authorId int32) (*profiles.GetUserResult, error) {
	userId := int(authorId)

	user, err := services.Instance().Feed().GetUser(userId)
	if err != nil && err == feedService.ErrorRedisNotFound {
		user, err = services.Instance().Profiles().GetUser(authorId)
	}
	if err != nil || user == nil {
		return nil, err
	}
	err = services.Instance().Feed().UpsertUser(user)
	if err != nil {
		return nil, err
	}
	return user, nil
}
