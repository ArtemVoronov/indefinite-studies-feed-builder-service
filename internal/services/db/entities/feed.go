package entities

import "time"

type FeedPost struct {
	PostUuid        string
	PostText        string
	PostPreviewText string
	PostTopic       string
	PostState       string
	AuthorUuid      string
	AuthorName      string
	CreateDate      time.Time
	LastUpdateDate  time.Time
	Tags            []string
}

// TODO: use UUID at LinkedCommentId field
type FeedComment struct {
	CommentId       int
	CommentUuid     string
	CommentText     string
	CommentState    string
	AuthorUuid      string
	AuthorName      string
	PostUuid        string
	LinkedCommentId *int
	CreateDate      time.Time
	LastUpdateDate  time.Time
}
