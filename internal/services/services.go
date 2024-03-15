package services

import (
	"fmt"
	"sync"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/app"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/auth"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/posts"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
)

// TODO: clean redis feed service
type Services struct {
	profiles  *profiles.ProfilesGRPCService
	auth      *auth.AuthGRPCService
	redisfeed *feed.RedisFeedService
	mongofeed *feed.MongoFeedService
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
	authCreds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("AUTH_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials: %s", err)
	}
	profilesCreds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("PROFILES_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials: %s", err)
	}
	postsCreds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("POSTS_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials: %s", err)
	}
	postsService := posts.CreatePostsGRPCService(utils.EnvVar("POSTS_SERVICE_GRPC_HOST")+":"+utils.EnvVar("POSTS_SERVICE_GRPC_PORT"), &postsCreds)
	profilesService := profiles.CreateProfilesGRPCService(utils.EnvVar("PROFILES_SERVICE_GRPC_HOST")+":"+utils.EnvVar("PROFILES_SERVICE_GRPC_PORT"), &profilesCreds)

	kafkaConsumerService, err := kafkaService.CreateKafkaConsumerService(utils.EnvVar("KAFKA_HOST")+":"+utils.EnvVar("KAFKA_PORT"), utils.EnvVar("KAFKA_GROUP_ID"))
	if err != nil {
		log.Fatalf("unable to create kafka consumer: %s", err)
	}

	mongoService := mongo.CreateMongoService()

	mongofeed := feed.CreateMongoFeedService(mongoService, kafkaConsumerService)
	if err != nil {
		log.Fatalf("unable to create mongo feed service: %s", err)
	}

	mongofeed.StartSync()

	return &Services{
		profiles:  profilesService,
		auth:      auth.CreateAuthGRPCService(utils.EnvVar("AUTH_SERVICE_GRPC_HOST")+":"+utils.EnvVar("AUTH_SERVICE_GRPC_PORT"), &authCreds),
		redisfeed: feed.CreateFeedService(postsService, profilesService),
		mongofeed: mongofeed,
	}
}

func (s *Services) Shutdown() error {
	result := []error{}
	err := s.profiles.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.auth.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.redisfeed.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.mongofeed.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	if len(result) > 0 {
		return fmt.Errorf("errors during shutdown: %v", result)
	}
	return nil
}

func (s *Services) Auth() *auth.AuthGRPCService {
	return s.auth
}

func (s *Services) Profiles() *profiles.ProfilesGRPCService {
	return s.profiles
}

func (s *Services) Feed() *feed.RedisFeedService {
	return s.redisfeed
}
