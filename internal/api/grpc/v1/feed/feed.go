package feed

import (
	"context"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	feedService "github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
	"google.golang.org/grpc"
)

type FeedBuilderServiceServer struct {
	feed.UnimplementedFeedBuilderServiceServer
}

func RegisterServiceServer(s *grpc.Server) {
	feed.RegisterFeedBuilderServiceServer(s, &FeedBuilderServiceServer{})
}

func (s *FeedBuilderServiceServer) CreatePost(ctx context.Context, in *feed.CreatePostRequest) (*feed.CreatePostReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
	if err != nil {
		return nil, err
	}
	tags, err := services.Instance().RedisFeed().GetAndCacheTags(utils.ToInt(in.GetTagIds()))
	if err != nil {
		return nil, err
	}
	post, err := services.Instance().RedisFeed().ToFeedPost(in, user.Login, tags)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().CreatePost(post)
	if err != nil {
		return nil, err
	}
	return &feed.CreatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdatePost(ctx context.Context, in *feed.UpdatePostRequest) (*feed.UpdatePostReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
	if err != nil {
		return nil, err
	}
	tags, err := services.Instance().RedisFeed().GetAndCacheTags(utils.ToInt(in.GetTagIds()))
	if err != nil {
		return nil, err
	}
	post, err := services.Instance().RedisFeed().ToFeedPost(in, user.Login, tags)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().UpdatePost(post)
	if err != nil {
		return nil, err
	}
	return &feed.UpdatePostReply{}, nil
}

func (s *FeedBuilderServiceServer) DeletePost(ctx context.Context, in *feed.DeletePostRequest) (*feed.DeletePostReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	err := services.Instance().RedisFeed().DeletePost(in.GetUuid())
	if err != nil {
		return nil, err
	}
	return &feed.DeletePostReply{}, nil
}

func (s *FeedBuilderServiceServer) CreateComment(ctx context.Context, in *feed.CreateCommentRequest) (*feed.CreateCommentReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
	if err != nil {
		return nil, err
	}
	comment, err := services.Instance().RedisFeed().ToFeedComment(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().CreateComment(comment)
	if err != nil {
		return nil, err
	}
	return &feed.CreateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateComment(ctx context.Context, in *feed.UpdateCommentRequest) (*feed.UpdateCommentReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	user, err := s.getAndCacheUser(in.AuthorUuid)
	if err != nil {
		return nil, err
	}
	comment, err := services.Instance().RedisFeed().ToFeedComment(in, user.Login)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().UpdateComment(comment)
	if err != nil {
		return nil, err
	}
	return &feed.UpdateCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) DeleteComment(ctx context.Context, in *feed.DeleteCommentRequest) (*feed.DeleteCommentReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	err := services.Instance().RedisFeed().DeleteComment(in.GetPostUuid(), in.GetUuid())
	if err != nil {
		return nil, err
	}
	return &feed.DeleteCommentReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateUser(ctx context.Context, in *feed.UpdateUserRequest) (*feed.UpdateUserReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()
	user := &profiles.GetUserResult{
		Uuid:  in.GetUuid(),
		Login: in.GetLogin(),
		Email: in.GetEmail(),
		Role:  in.GetRole(),
		State: in.GetState(),
	}
	err := services.Instance().RedisFeed().UpsertUser(user)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().SyncUserDataInFeed(user)
	if err != nil {
		return nil, err
	}
	return &feed.UpdateUserReply{}, nil
}

func (s *FeedBuilderServiceServer) CreateTag(ctx context.Context, in *feed.CreateTagRequest) (*feed.CreateTagReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()

	tag, err := feedService.ToFeedTag(in)
	if err != nil {
		return nil, err
	}

	err = services.Instance().RedisFeed().UpsertTag(tag)
	if err != nil {
		return nil, err
	}
	return &feed.CreateTagReply{}, nil
}

func (s *FeedBuilderServiceServer) UpdateTag(ctx context.Context, in *feed.UpdateTagRequest) (*feed.UpdateTagReply, error) {
	services.Instance().RedisFeed().SyncGuard.RLock()
	defer services.Instance().RedisFeed().SyncGuard.RUnlock()

	tag, err := feedService.ToFeedTag(in)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().UpsertTag(tag)
	if err != nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().SyncTagDataInFeed(tag)
	if err != nil {
		return nil, err
	}
	return &feed.UpdateTagReply{}, nil
}

func (s *FeedBuilderServiceServer) getAndCacheUser(authorUuid string) (*profiles.GetUserResult, error) {
	user, err := services.Instance().RedisFeed().GetUser(authorUuid)
	if err != nil && err == feedService.ErrorRedisNotFound {
		user, err = services.Instance().Profiles().GetUser(authorUuid)
	}
	if err != nil || user == nil {
		return nil, err
	}
	err = services.Instance().RedisFeed().UpsertUser(user)
	if err != nil {
		return nil, err
	}
	return user, nil
}
