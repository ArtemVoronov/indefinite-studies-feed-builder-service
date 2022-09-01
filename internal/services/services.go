package services

import (
	"log"
	"sync"

	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/app"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/auth"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/db"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
)

type Services struct {
	auth *auth.AuthGRPCService
	db   *db.PostgreSQLService
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
	creds, err := app.LoadTLSCredentialsForClient(utils.EnvVar("AUTH_SERVICE_CLIENT_TLS_CERT_PATH"))
	if err != nil {
		log.Fatalf("unable to load TLS credentials")
	}

	return &Services{
		auth: auth.CreateAuthGRPCService(utils.EnvVar("AUTH_SERVICE_GRPC_HOST")+":"+utils.EnvVar("AUTH_SERVICE_GRPC_PORT"), &creds),
		db:   db.CreatePostgreSQLService(),
	}
}

func (s *Services) Shutdown() {
	s.auth.Shutdown()
	s.db.Shutdown()
}

func (s *Services) DB() *db.PostgreSQLService {
	return s.db
}

func (s *Services) Auth() *auth.AuthGRPCService {
	return s.auth
}
