package feed

import (
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/gin-gonic/gin"
)

func GetFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 50
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}
	from := offset
	to := offset + limit

	result := services.Instance().FeedCache().Get(from, to)

	c.JSON(http.StatusOK, result)
}
