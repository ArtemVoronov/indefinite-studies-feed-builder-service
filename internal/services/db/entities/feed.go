package entities

import "time"

type FeedPost struct {
	PostUuid        string
	PostText        string
	PostPreviewText string
	PostTopic       string
	PostState       string
	AuthorId        int
	AuthorName      string
	CreateDate      time.Time
	LastUpdateDate  time.Time
	Tags            []string
}

type FeedComment struct {
	CommentId       int
	CommentUuid     string
	CommentText     string
	CommentState    string
	AuthorId        int
	AuthorName      string
	PostUuid        string
	LinkedCommentId *int
	CreateDate      time.Time
	LastUpdateDate  time.Time
}
