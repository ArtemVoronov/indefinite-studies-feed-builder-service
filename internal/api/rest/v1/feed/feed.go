package feed

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/api/grpc/v1/feed"
	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

// TODO: finish feed building, clean code

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

	result, err := services.Instance().Redis().WithTimeout(func(cli *redis.Client, ctx context.Context, cancel context.CancelFunc) (any, error) {
		scoreMemberPairs, err := cli.ZRangeByScoreWithScores(ctx, feed.REDIS_FEED_KEY, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    "+inf",
			Offset: int64(offset),
			Count:  int64(limit),
		}).Result()

		if err != nil {
			return nil, err
		}
		result := make([]string, 0, 100)
		for _, pair := range scoreMemberPairs {
			str, ok := pair.Member.(string)
			if !ok {
				return nil, fmt.Errorf("member is not a string type")
			}
			json, err := cli.HGet(ctx, feed.REDIS_POSTS_KEY, feed.PostKey2(str)).Result()
			if !ok {
				return nil, fmt.Errorf("unable to creat feed: %v", err)
			}
			result = append(result, json)
		}

		return result, err
	})()

	if err != nil {
		c.JSON(http.StatusInternalServerError, "Unable to get feed")
		log.Printf("Unable to get feed: %s", err)
		return
	}

	c.JSON(http.StatusOK, result)
}
