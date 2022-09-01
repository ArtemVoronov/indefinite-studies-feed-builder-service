package ping

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func Ping(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, "Pong!")
}

func SafePing(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, "Pong!")
}
