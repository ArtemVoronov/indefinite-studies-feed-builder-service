package entities

import "time"

type FeedPost struct {
	PostId          int
	PostText        string
	PostPreviewText string
	PostTopic       string
	PostState       string
	AuthorId        int
	AuthorName      string
	CreateDate      time.Time
	LastUpdateDate  time.Time
}

type FeedComment struct {
	CommentId       int
	CommentText     string
	CommentState    string
	AuthorId        int
	AuthorName      string
	PostId          int
	LinkedCommentId *int
	CreateDate      time.Time
	LastUpdateDate  time.Time
}
