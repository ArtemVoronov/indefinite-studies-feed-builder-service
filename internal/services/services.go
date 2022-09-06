package services

import (
	"log"
	"sync"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/cache"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/app"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/auth"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/db"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/redis"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
)

// TODO: delete postgres
type Services struct {
	profiles  *profiles.ProfilesGRPCService
	auth      *auth.AuthGRPCService
	db        *db.PostgreSQLService
	redis     *redis.RedisService
	feedCache *cache.FeedCache
}

var once sync.Once
var instance *Services

func Instance() *Services {
	once.Do(func() {
		if instance == nil {
			instance = createServices()
		}
	})
	return instance
}

func createServices() *Services {
	authcreds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("AUTH_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials")
	}
	profilescreds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("PROFILES_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials")
	}

	return &Services{
		profiles:  profiles.CreateProfilesGRPCService(utils.EnvVar("PROFILES_SERVICE_GRPC_HOST")+":"+utils.EnvVar("PROFILES_SERVICE_GRPC_PORT"), &profilescreds),
		auth:      auth.CreateAuthGRPCService(utils.EnvVar("AUTH_SERVICE_GRPC_HOST")+":"+utils.EnvVar("AUTH_SERVICE_GRPC_PORT"), &authcreds),
		db:        db.CreatePostgreSQLService(),
		redis:     redis.CreateRedisService(),
		feedCache: cache.CreateFeedCache(),
	}
}

func (s *Services) Shutdown() {
	s.profiles.Shutdown()
	s.auth.Shutdown()
	s.db.Shutdown()
	s.feedCache.Shutdown()
	s.redis.Shutdown()
}

func (s *Services) DB() *db.PostgreSQLService {
	return s.db
}

func (s *Services) Auth() *auth.AuthGRPCService {
	return s.auth
}

func (s *Services) Profiles() *profiles.ProfilesGRPCService {
	return s.profiles
}

func (s *Services) FeedCache() *cache.FeedCache {
	return s.feedCache
}

func (s *Services) Redis() *redis.RedisService {
	return s.redis
}
