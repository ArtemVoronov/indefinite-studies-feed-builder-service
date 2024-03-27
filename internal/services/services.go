package services

import (
	"errors"
	"sync"
	"time"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/app"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/auth"
	kafkaService "github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/kafka"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/mongo"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
)

// TODO: add service for synchronization of post's and feed's services queues

type Services struct {
	auth      *auth.AuthGRPCService
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

	authService := auth.CreateAuthGRPCService(utils.EnvVar("AUTH_SERVICE_GRPC_HOST")+":"+utils.EnvVar("AUTH_SERVICE_GRPC_PORT"), &authCreds)

	kafkaAdminQueryTimeout := utils.EnvVarDurationDefault("KAFKA_ADMIN_QUERY_TIMEOUT_IN_SECONDS", time.Second, 30*time.Second)
	kafkaReadMessageTimeout := utils.EnvVarDurationDefault("KAFKA_READ_MESSAGE_TIMEOUT_IN_SECONDS", time.Second, 5*time.Second)
	kafkaConsumerService, err := kafkaService.CreateKafkaConsumerService(utils.EnvVar("KAFKA_HOST")+":"+utils.EnvVar("KAFKA_PORT"), utils.EnvVar("KAFKA_GROUP_ID"))
	if err != nil {
		log.Fatalf("unable to create kafka consumer: %s", err)
	}

	kafkaAdminService, err := kafkaService.CreateKafkaAdminService(utils.EnvVar("KAFKA_HOST")+":"+utils.EnvVar("KAFKA_PORT"), kafkaAdminQueryTimeout)
	if err != nil {
		log.Fatalf("unable to create kafka consumer: %s", err)
	}

	mongoService, err := mongo.CreateMongoService()
	if err != nil {
		log.Fatalf("unable to create mongo service: %s", err)
	}

	mongoFeedService := feed.CreateMongoFeedService(mongoService, kafkaConsumerService, kafkaAdminService, kafkaReadMessageTimeout)
	err = mongoFeedService.CreateRequiredTopics()
	if err != nil {
		log.Fatalf("unable to create init kafka topics: %s", err)
	}
	err = mongoFeedService.StartSync()
	if err != nil {
		log.Fatalf("unable to create start feed sync: %s", err)
	}
	return &Services{
		auth:      authService,
		mongofeed: mongoFeedService,
	}
}

func (s *Services) Shutdown() error {
	result := []error{}
	err := s.auth.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	err = s.mongofeed.Shutdown()
	if err != nil {
		result = append(result, err)
	}
	if len(result) > 0 {
		return errors.Join(result...)
	}
	return nil
}

func (s *Services) Auth() *auth.AuthGRPCService {
	return s.auth
}

func (s *Services) MongoFeed() *feed.MongoFeedService {
	return s.mongofeed
}
