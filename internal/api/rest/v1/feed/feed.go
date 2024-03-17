package feed

import (
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/api"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/profiles"
	"github.com/gin-gonic/gin"
)

type FeedDTO struct {
	Count  int
	Offset int
	Limit  int
	Data   []string
}

type UsersListDTO struct {
	Count  int
	Offset int
	Limit  int
	Data   []profiles.GetUserResult
}

type CommentsListDto struct {
	Count  int
	Offset int
	Limit  int
	Data   []entities.FeedComment
}

func GetFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")
	tagIdStr := c.DefaultQuery("tagId", "")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 10
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}

	var feedBlocks []string
	if tagIdStr != "" {
		tagId, err := strconv.Atoi(tagIdStr)
		if err != nil {
			c.JSON(http.StatusInternalServerError, "Unable to parse tag id")
			return
		}
		feedBlocks, err = services.Instance().MongoFeed().GetFeedByTag(tagId, offset, limit)
	} else {
		feedBlocks, err = services.Instance().MongoFeed().GetFeed(offset, limit)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get feed")
		log.Error("Unable to get feed", err.Error())
		return
	}

	result := &FeedDTO{
		Data:   feedBlocks,
		Count:  len(feedBlocks),
		Offset: offset,
		Limit:  limit,
	}

	c.JSON(http.StatusOK, result)
}

func StartSync(c *gin.Context) {
	err := services.Instance().MongoFeed().StartSync()

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to start sync feed")
		log.Error("Unable to start sync feed", err.Error())
		return
	}

	c.JSON(http.StatusOK, api.DONE)
}

func StopSync(c *gin.Context) {
	err := services.Instance().MongoFeed().StopSync()

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to stop sync feed")
		log.Error("Unable to stop sync feed", err.Error())
		return
	}

	c.JSON(http.StatusOK, api.DONE)
}
