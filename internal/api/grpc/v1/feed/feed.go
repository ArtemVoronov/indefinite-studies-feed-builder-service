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
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
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
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
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
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	err := services.Instance().Feed().DeletePost(in.GetUuid())
	if err != nil {
		return nil, err
	}
	return &feed.DeletePostReply{}, nil
}

func (s *FeedBuilderServiceServer) CreateComment(ctx context.Context, in *feed.CreateCommentRequest) (*feed.CreateCommentReply, error) {
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
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
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
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
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	err := services.Instance().Feed().DeleteComment(in.GetPostUuid(), in.GetUuid())
	if err != nil {
		return nil, err
	}
	return &feed.DeleteCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateUser(ctx context.Context, in *feed.UpdateUserRequest) (*feed.UpdateUserReply, error) {
	services.Instance().Feed().SyncGuard.RLock()
	defer services.Instance().Feed().SyncGuard.RUnlock()
	user := &profiles.GetUserResult{
		Uuid:  in.GetUuid(),
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

func (s *FeedBuilderServiceServer) getAndCacheUser(authorUuid string) (*profiles.GetUserResult, error) {
	user, err := services.Instance().Feed().GetUser(authorUuid)
	if err != nil && err == feedService.ErrorRedisNotFound {
		user, err = services.Instance().Profiles().GetUser(authorUuid)
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
