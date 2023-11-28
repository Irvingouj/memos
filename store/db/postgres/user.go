package postgres

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/dm"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/bob/dialect/psql/um"
	"github.com/usememos/memos/store"
)

// -- user
// CREATE TABLE "\"user\"" (
//   id SERIAL PRIMARY KEY,
//   created_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
//   updated_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
//   row_status VARCHAR(255) NOT NULL DEFAULT 'NORMAL',
//   username VARCHAR(255) NOT NULL UNIQUE,
//   role VARCHAR(255) NOT NULL DEFAULT \"user\",
//   email VARCHAR(255) NOT NULL DEFAULT '',
//   nickname VARCHAR(255) NOT NULL DEFAULT '',
//   password_hash VARCHAR(255) NOT NULL,
//   avatar_url TEXT NOT NULL
// );

func (d *DB) CreateUser(ctx context.Context, create *store.User) (*store.User, error) {
	column_list := []string{"username", "role", "email", "nickname", "password_hash", "avatar_url"}
	arg_list := []any{create.Username, create.Role, create.Email, create.Nickname, create.PasswordHash, create.AvatarURL}

	if create.RowStatus != "" {
		column_list = append(column_list, "row_status")
		arg_list = append(arg_list, create.RowStatus)
	}

	if create.CreatedTs != 0 {
		column_list = append(column_list, "created_ts")
		arg_list = append(arg_list, create.CreatedTs)
	}

	if create.UpdatedTs != 0 {
		column_list = append(column_list, "updated_ts")
		arg_list = append(arg_list, create.UpdatedTs)
	}

	if create.ID != 0 {
		column_list = append(column_list, "id")
		arg_list = append(arg_list, create.ID)
	}

	query, args, err := psql.Insert(
		im.Into("\"user\"", column_list...),
		im.Values(psql.Arg(arg_list...)),
		im.Returning("id"),
	).Build()

	if err != nil {
		return nil, err
	}

	lastInsertId := 0
	err = d.db.QueryRow(query, args...).Scan(&lastInsertId)
	if err != nil {
		return nil, err
	}

	id32 := int32(lastInsertId)
	list, err := d.ListUsers(ctx, &store.FindUser{ID: &id32})
	if err != nil {
		return nil, err
	}
	if len(list) != 1 {
		return nil, errors.Wrapf(nil, "unexpected user count: %d", len(list))
	}

	return list[0], nil
}

func (d *DB) UpdateUser(ctx context.Context, update *store.UpdateUser) (*store.User, error) {
	columns, args := []string{}, []any{}
	if v := update.UpdatedTs; v != nil {
		columns, args = append(columns, "updated_ts"), append(args, *v)
	}
	if v := update.RowStatus; v != nil {
		columns, args = append(columns, "row_status"), append(args, *v)
	}
	if v := update.Username; v != nil {
		columns, args = append(columns, "username"), append(args, *v)
	}
	if v := update.Email; v != nil {
		columns, args = append(columns, "email"), append(args, *v)
	}
	if v := update.Nickname; v != nil {
		columns, args = append(columns, "nickname"), append(args, *v)
	}
	if v := update.PasswordHash; v != nil {
		columns, args = append(columns, "password_hash"), append(args, *v)
	}
	if v := update.AvatarURL; v != nil {
		columns, args = append(columns, "avatar_url"), append(args, *v)
	}

	args = append(args, update.ID)
	// enumerate columns and args then map to um.Set(column).Value(arg)
	set_clause := make([]bob.Mod[*dialect.UpdateQuery], len(columns)+1)

	// Populate set_clause
	for i, v := range columns {
		set_clause[i+1] = um.Set(v).To(args[i])
	}
	set_clause[0] = um.Table("\"user\"")
	// Now you can use set_clause in your query
	_, args, err := psql.Update(
		set_clause..., // This will expand the slice into individual arguments
	).Build()

	return nil, err

}

func (d *DB) ListUsers(ctx context.Context, find *store.FindUser) ([]*store.User, error) {
	builder := psql.Select(
		sm.Columns("id", "username", "role", "email", "nickname", "password_hash", "avatar_url", "row_status", "created_ts", "updated_ts"),
		sm.From("\"user\""),
		sm.OrderBy("created_ts").Desc(),
		sm.OrderBy("row_status").Desc(),
	)

	if v := find.ID; v != nil {
		builder.Apply(
			sm.Where(psql.Quote("id").EQ(psql.Arg(*v))),
		)
	}

	if v := find.Username; v != nil {
		builder.Apply(
			sm.Where(psql.Quote("username").EQ(psql.Arg(*v))),
		)
	}

	if v := find.Role; v != nil {
		builder.Apply(
			sm.Where(psql.Quote("role").EQ(psql.Arg(*v))),
		)
	}

	if v := find.Email; v != nil {
		builder.Apply(
			sm.Where(psql.Quote("email").EQ(psql.Arg(*v))),
		)
	}

	if v := find.Nickname; v != nil {
		builder.Apply(
			sm.Where(psql.Quote("nickname").EQ(psql.Arg(*v))),
		)
	}

	query, args, err := builder.Build()

	if err != nil {
		return nil, err
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	defer rows.Close()

	list := make([]*store.User, 0)
	for rows.Next() {
		var user store.User
		var created, updated time.Time
		err := rows.Scan(&user.ID, &user.Username, &user.Role, &user.Email, &user.Nickname, &user.PasswordHash, &user.AvatarURL, &user.RowStatus, &created, &updated)
		if err != nil {
			return nil, err
		}
		user.CreatedTs = created.Unix()
		user.UpdatedTs = updated.Unix()
		list = append(list, &user)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

func (d *DB) GetUser(ctx context.Context, find *store.FindUser) (*store.User, error) {
	list, err := d.ListUsers(ctx, find)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, errors.Wrapf(nil, "unexpected user count: %d", len(list))
	}
	return list[0], nil
}

func (d *DB) DeleteUser(ctx context.Context, delete *store.DeleteUser) error {
	query, _, err := psql.Delete(
		dm.From("\"user\""),
		dm.Where(psql.Quote("id").EQ(psql.Arg(delete.ID)))).
		Build()

	result, err := d.db.ExecContext(ctx, query, delete.ID)
	if err != nil {
		return err
	}
	if _, err := result.RowsAffected(); err != nil {
		return err
	}

	if err := d.Vacuum(ctx); err != nil {
		// Prevent linter warning.
		return err
	}

	return nil
}
