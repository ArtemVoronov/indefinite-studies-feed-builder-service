package feed

import (
	"log"
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/api"
	"github.com/gin-gonic/gin"
)

func GetFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 10
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}

	result, err := services.Instance().Feed().GetFeed(offset, limit)

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get feed")
		log.Printf("Unable to get feed: %s", err)
		return
	}

	c.JSON(http.StatusOK, result)
}

func GetPost(c *gin.Context) {
	postIdStr := c.Param("id")

	if postIdStr == "" {
		c.JSON(http.StatusBadRequest, "Missed ID")
		return
	}

	var postId int
	var parseErr error
	if postId, parseErr = strconv.Atoi(postIdStr); parseErr != nil {
		c.JSON(http.StatusBadRequest, api.ERROR_ID_WRONG_FORMAT)
		return
	}

	result, err := services.Instance().Feed().GetPost(postId)

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get post")
		log.Printf("Unable to get post: %s", err)
		return
	}

	c.JSON(http.StatusOK, result)
}
