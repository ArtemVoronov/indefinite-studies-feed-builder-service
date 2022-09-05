package queries

import (
	"context"
	"database/sql"
	"fmt"
)

// TODO: implement update and delete user ops

type CreateFeedPostParams struct {
	PostId          interface{}
	PostText        interface{}
	PostPreviewText interface{}
	PostTopic       interface{}
	PostState       interface{}
	AuthorId        interface{}
	AuthorName      interface{}
	CreateDate      interface{}
	LastUpdateDate  interface{}
}

type UpdateFeedPostParams struct {
	PostId          interface{}
	PostText        interface{}
	PostPreviewText interface{}
	PostTopic       interface{}
	PostState       interface{}
	AuthorId        interface{}
	AuthorName      interface{}
	CreateDate      interface{}
	LastUpdateDate  interface{}
}

type CreateFeedCommentParams struct {
	CommentId       interface{}
	CommentText     interface{}
	CommentState    interface{}
	AuthorId        interface{}
	AuthorName      interface{}
	PostId          interface{}
	LinkedCommentId interface{}
	CreateDate      interface{}
	LastUpdateDate  interface{}
}

type UpdateFeedCommentParams struct {
	CommentId       interface{}
	CommentText     interface{}
	CommentState    interface{}
	AuthorId        interface{}
	AuthorName      interface{}
	PostId          interface{}
	LinkedCommentId interface{}
	CreateDate      interface{}
	LastUpdateDate  interface{}
}

const (
	CREATE_POST_QUERY = `INSERT INTO feed_posts
		(post_id, post_text, post_preview_text, post_topic, post_state, author_id, author_name, create_date, last_update_date) 
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	UPDATE_POST_QUERY = `UPDATE feed_posts
	SET post_text = COALESCE($2, post_text),
		post_preview_text = COALESCE($3, post_preview_text),
		post_topic = COALESCE($4, post_topic),
		post_state = COALESCE($5, post_state),
		author_id = COALESCE($6, author_id),
		author_name = COALESCE($7, author_name),
		create_date = COALESCE($8, create_date),
		last_update_date = COALESCE($9, last_update_date)
	WHERE post_id = $1`

	DELETE_POST_QUERY = `DELETE FROM feed_posts WHERE post_id = $1`

	CREATE_COMMENT_QUERY = `INSERT INTO feed_comments
		(comment_id, comment_text, comment_state, author_id, author_name, post_id, linked_comment_id, create_date, last_update_date) 
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	UPDATE_COMMENT_QUERY = `UPDATE feed_comments
		SET comment_text = COALESCE($2, comment_text),
		comment_state = COALESCE($3, comment_state),
		author_id = COALESCE($4, author_id),
		author_name = COALESCE($5, author_name),
		post_id = COALESCE($6, post_id),
		linked_comment_id = COALESCE($7, linked_comment_id),
		create_date = COALESCE($8, create_date),
		last_update_date = COALESCE($9, last_update_date)
	WHERE comment_id = $1`

	DELETE_COMMENT_QUERY = `DELETE FROM feed_comments WHERE comment_id = $1`
)

func CreateFeedPost(tx *sql.Tx, ctx context.Context, params *CreateFeedPostParams) error {
	stmt, err := tx.PrepareContext(ctx, CREATE_POST_QUERY)
	if err != nil {
		return fmt.Errorf("error at creating feed post, case after preparing statement: %s", err)
	}

	_, err = stmt.ExecContext(ctx,
		params.PostId, params.PostText, params.PostPreviewText, params.PostTopic, params.PostState,
		params.AuthorId, params.AuthorName,
		params.CreateDate, params.LastUpdateDate)
	if err != nil {
		return fmt.Errorf("error at creating feed post, case after ExecContext: %s", err)
	}

	return nil
}

func UpdateFeedPost(tx *sql.Tx, ctx context.Context, params *UpdateFeedPostParams) error {
	stmt, err := tx.PrepareContext(ctx, UPDATE_POST_QUERY)
	if err != nil {
		return fmt.Errorf("error at updating feed post, case after preparing statement: %s", err)
	}
	res, err := stmt.ExecContext(ctx,
		params.PostId, params.PostText, params.PostPreviewText, params.PostTopic, params.PostState,
		params.AuthorId, params.AuthorName,
		params.CreateDate, params.LastUpdateDate)
	if err != nil {
		return fmt.Errorf("error at updating feed post (%v), case after executing statement: %s", params, err)
	}

	affectedRowsCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error at updating post (%v), case after counting affected rows: %s", params, err)
	}
	if affectedRowsCount == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func DeleteFeedPost(tx *sql.Tx, ctx context.Context, id int) error {
	stmt, err := tx.PrepareContext(ctx, DELETE_POST_QUERY)
	if err != nil {
		return fmt.Errorf("error at deleting feed post, case after preparing statement: %s", err)
	}
	res, err := stmt.ExecContext(ctx, id)
	if err != nil {
		return fmt.Errorf("error at deleting feed post by id '%d', case after executing statement: %s", id, err)
	}
	affectedRowsCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error at deleting feed post by id '%d', case after counting affected rows: %s", id, err)
	}
	if affectedRowsCount == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func CreateFeedComment(tx *sql.Tx, ctx context.Context, params *CreateFeedCommentParams) error {
	stmt, err := tx.PrepareContext(ctx, CREATE_COMMENT_QUERY)
	if err != nil {
		return fmt.Errorf("error at creating feed comment, case after preparing statement: %s", err)
	}

	_, err = stmt.ExecContext(ctx,
		params.CommentId, params.CommentText, params.CommentState,
		params.AuthorId, params.AuthorName,
		params.PostId, params.LinkedCommentId,
		params.CreateDate, params.LastUpdateDate)
	if err != nil {
		return fmt.Errorf("error at creating feed comment, case after ExecContext: %s", err)
	}

	return nil
}

func UpdateFeedComment(tx *sql.Tx, ctx context.Context, params *UpdateFeedCommentParams) error {
	stmt, err := tx.PrepareContext(ctx, UPDATE_COMMENT_QUERY)
	if err != nil {
		return fmt.Errorf("error at updating feed comment, case after preparing statement: %s", err)
	}
	res, err := stmt.ExecContext(ctx,
		params.CommentId, params.CommentText, params.CommentState,
		params.AuthorId, params.AuthorName,
		params.PostId, params.LinkedCommentId,
		params.CreateDate, params.LastUpdateDate)
	if err != nil {
		return fmt.Errorf("error at updating feed comment (%v), case after executing statement: %s", params, err)
	}

	affectedRowsCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error at updating comment (%v), case after counting affected rows: %s", params, err)
	}
	if affectedRowsCount == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func DeleteFeedComment(tx *sql.Tx, ctx context.Context, id int) error {
	stmt, err := tx.PrepareContext(ctx, DELETE_COMMENT_QUERY)
	if err != nil {
		return fmt.Errorf("error at deleting feed comment, case after preparing statement: %s", err)
	}
	res, err := stmt.ExecContext(ctx, id)
	if err != nil {
		return fmt.Errorf("error at deleting feed comment by id '%d', case after executing statement: %s", id, err)
	}
	affectedRowsCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error at deleting feed comment by id '%d', case after counting affected rows: %s", id, err)
	}
	if affectedRowsCount == 0 {
		return sql.ErrNoRows
	}
	return nil
}
