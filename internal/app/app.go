package app

import (
	"fmt"
	"log"
	"net/http"
	"os"

	feedGrpcApi "github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/api/grpc/v1/feed"
	feedRestApi "github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/api/rest/v1/feed"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/api/rest/v1/ping"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/app"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/auth"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/utils"
	"github.com/gin-contrib/expvar"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func Start() {
	app.LoadEnv()
	logger := app.NewLogrusLogger()
	logpath := utils.EnvVarDefault("APP_LOGS_PATH", "stdout")
	if logpath != "stdout" {
		file, err := os.OpenFile(logpath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("unable init logging: %v", err)
		}
		logger.SetOutput(file)
		defer file.Close()
	}
	creds := app.TLSCredentials()
	go func() {
		app.StartGRPC(setup, shutdown, app.HostGRPC(), createGrpcApi, &creds, logger)
	}()
	app.StartHTTP(setup, shutdown, app.HostHTTP(), createRestApi(logger))
}

func setup() {
	services.Instance()
	if err := services.Instance().Feed().Sync(); err != nil {
		log.Fatalf("unable to sync feed: %v", err)
	}
}

func shutdown() {
	err := services.Instance().Shutdown()
	log.Printf("error during app shutdown: %v", err)
}

func createRestApi(logger *logrus.Logger) *gin.Engine {
	router := gin.Default()
	gin.SetMode(app.Mode())
	router.Use(app.Cors())
	router.Use(app.JSONLogMiddleware(logger))
	router.Use(gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		if err, ok := recovered.(string); ok {
			c.String(http.StatusInternalServerError, fmt.Sprintf("error: %s", err))
		}
		c.AbortWithStatus(http.StatusInternalServerError)
	}))

	// TODO: add permission controller by user role and user state
	v1 := router.Group("/api/v1")

	v1.GET("/feed/ping", ping.Ping)
	v1.GET("/feed", feedRestApi.GetFeed)
	v1.GET("/feed/:id", feedRestApi.GetPost)

	authorized := router.Group("/api/v1")
	authorized.Use(app.AuthReqired(authenicate))
	{
		authorized.GET("/feed/debug/vars", expvar.Handler())
		authorized.GET("/feed/safe-ping", ping.SafePing)

		authorized.POST("/feed/sync", feedRestApi.Sync)
		authorized.POST("/feed/clear", feedRestApi.Clear)
	}

	return router
}

func createGrpcApi(s *grpc.Server) {
	feedGrpcApi.RegisterServiceServer(s)
}

func authenicate(token string) (*auth.VerificationResult, error) {
	return services.Instance().Auth().VerifyToken(token)
}
