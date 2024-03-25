package feed

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/feed"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/api"
	"github.com/ArtemVoronov/indefinite-studies-utils/pkg/log"
	"github.com/gin-gonic/gin"
)

type PostsFeedDTO struct {
	Count  int
	Offset int
	Limit  int
	Data   []string
}

type CommentsFeedDTO struct {
	Count  int
	Offset int
	Limit  int
	Data   []feed.CommentInMongoDB
}

func GetPostsFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")
	tagIdStr := c.DefaultQuery("tagId", "")
	state := c.DefaultQuery("state", "")
	userUuid := c.DefaultQuery("userUuid", "")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 10
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}

	feedBlocks, err := getPostsFeed(userUuid, state, tagIdStr, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get feed")
		log.Error("Unable to get feed", err.Error())
		return
	}

	result := &PostsFeedDTO{
		Data:   feedBlocks,
		Count:  len(feedBlocks),
		Offset: offset,
		Limit:  limit,
	}

	c.JSON(http.StatusOK, result)
}

func GetCommentsFeed(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	offsetStr := c.DefaultQuery("offset", "0")
	postUuid := c.Param("uuid")

	if postUuid == "" {
		c.JSON(http.StatusBadRequest, "Missed 'uuid' param")
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 10
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}

	feedBlocks, err := services.Instance().MongoFeed().GetCommentsFeedByPost(postUuid, limit, offset)

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get feed")
		log.Error("Unable to get feed", err.Error())
		return
	}

	result := &CommentsFeedDTO{
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

func getPostsFeed(userUuid string, state string, tagIdStr string, limit int, offset int) ([]string, error) {
	if len(userUuid) > 0 {
		return services.Instance().MongoFeed().GetPostsFeedByUser(userUuid, state, limit, offset)
	} else if len(tagIdStr) > 0 {
		tagId, err := strconv.Atoi(tagIdStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse tag id: %v", tagIdStr)
		}
		return services.Instance().MongoFeed().GetPostsFeedByTag(tagId, state, limit, offset)
	} else {
		return services.Instance().MongoFeed().GetPostsFeed(state, limit, offset)
	}
}
