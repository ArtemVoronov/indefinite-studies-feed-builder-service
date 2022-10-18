package feed

import (
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/api"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/services/db/entities"
	"github.com/gin-gonic/gin"
)

type FeedDTO struct {
	Count  int
	Offset int
	Limit  int
	Data   []feed.FeedBlock
}

func GetFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")
	tagId := c.DefaultQuery("tagId", "")
	state := c.DefaultQuery("state", "")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 10
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}

	if state == "" {
		state = entities.POST_STATE_PUBLISHED
	}

	var feedBlocks []feed.FeedBlock
	if tagId != "" {
		feedBlocks, err = services.Instance().Feed().GetFeedByTagAndState(tagId, state, offset, limit)
	} else {
		feedBlocks, err = services.Instance().Feed().GetFeedByState(state, offset, limit)
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

func GetPost(c *gin.Context) {
	postUuid := c.Param("uuid")

	if postUuid == "" {
		c.JSON(http.StatusBadRequest, "Missed 'uuid' parameter")
		return
	}

	result, err := services.Instance().Feed().GetPost(postUuid)
	if err != nil {
		if err == feed.ErrorRedisNotFound {
			c.JSON(http.StatusNotFound, api.PAGE_NOT_FOUND)
			log.Error("Unable to get post "+postUuid, err.Error())
		} else {
			c.JSON(http.StatusInternalServerError, "Unable to get post")
			log.Error("Unable to get post", err.Error())
		}
		return
	}

	c.JSON(http.StatusOK, result)
}

func Sync(c *gin.Context) {
	err := services.Instance().Feed().Sync()

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to sync feed")
		log.Error("Unable to sync feed", err.Error())
		return
	}

	c.JSON(http.StatusOK, api.DONE)
}

func Clear(c *gin.Context) {
	err := services.Instance().Feed().Clear()

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to clear feed")
		log.Error("Unable to clear feed", err.Error())
		return
	}

	c.JSON(http.StatusOK, api.DONE)
}
