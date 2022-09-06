package cache

import (
	"sort"
	"sync"

	"github.com/ArtemVoronov/indefinite-studies-feed-builder-service/internal/services/db/entities"
)

// TODO: remove, use redis

type FeedEntry struct {
	Post     entities.FeedPost
	Comments []entities.FeedComment
}

type FeedCache struct {
	feed     []entities.FeedPost
	comments map[int][]entities.FeedComment // post id -> comments
	rwm      sync.RWMutex
}

func CreateFeedCache() *FeedCache {
	return &FeedCache{
		feed:     make([]entities.FeedPost, 0),
		comments: make(map[int][]entities.FeedComment),
	}
}

func (cache *FeedCache) Shutdown() {

}

func (cache *FeedCache) Get(from, to int) []FeedEntry {
	cache.rwm.RLock()
	defer cache.rwm.RUnlock()

	to = min(to, len(cache.feed))
	result := make([]FeedEntry, 0, abs(to-from))

	posts := cache.feed[from:to]
	for _, p := range posts {
		c := cache.comments[p.PostId]
		result = append(result, FeedEntry{Post: p, Comments: c})
	}
	return result
}

func (cache *FeedCache) AddPost(post entities.FeedPost) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	cache.feed = append(cache.feed, post)
	sort.Slice(cache.feed, func(i, j int) bool {
		return cache.feed[i].CreateDate.After(cache.feed[j].CreateDate)
	})
}

func (cache *FeedCache) AddComment(comment entities.FeedComment) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	comments, ok := cache.comments[comment.PostId]
	if !ok {
		comments = make([]entities.FeedComment, 0)
	}
	comments = append(comments, comment)
	sort.Slice(comments, func(i, j int) bool {
		return comments[i].CreateDate.After(comments[j].CreateDate)
	})
	cache.comments[comment.PostId] = comments
}

func (cache *FeedCache) UpdatePost(post entities.FeedPost) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	for i := range cache.feed {
		if cache.feed[i].PostId == post.PostId {
			cache.feed[i] = post
			return
		}
	}

}

func (cache *FeedCache) UpdateComment(comment entities.FeedComment) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	comments, ok := cache.comments[comment.PostId]
	if !ok {
		return
	}
	for i := range comments {
		if comments[i].CommentId == comment.CommentId {
			cache.comments[comment.PostId][i] = comment
			return
		}
	}
}

func (cache *FeedCache) DeletePost(postId int) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	for i := range cache.feed {
		if cache.feed[i].PostId == postId {
			resliced := make([]entities.FeedPost, 0, cap(cache.feed))
			resliced = append(resliced, cache.feed[:i]...)
			resliced = append(resliced, cache.feed[i+1:]...)
			cache.feed = resliced
			return
		}
	}

}

func (cache *FeedCache) DeleteComment(postId int, commentId int) {
	cache.rwm.Lock()
	defer cache.rwm.Unlock()
	comments, ok := cache.comments[postId]
	if !ok {
		return
	}
	for i := range comments {
		if comments[i].CommentId == commentId {
			resliced := make([]entities.FeedComment, 0, cap(comments))
			resliced = append(resliced, comments[:i]...)
			resliced = append(resliced, comments[i+1:]...)
			cache.comments[commentId] = resliced
			return
		}
	}
}

// TODO: move to utils
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
