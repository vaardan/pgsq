# pgsq
## About
Wrapper for [pgx/v5](https://github.com/jackc/pgx/) pool. 

[scany/v2](https://github.com/georgysavva/scany/) is used to provide _Select_ & _Get_ convenience methods.

All basic (i.e. not raw) methods take _sqlizer_ interface as a query, which is provided by
[squirrel](https://github.com/Masterminds/squirrel) query builder.


## Usage example
Actual data-access methods should take _pgsq.Queryable_ as an argument–this way _CreateEntity_ can be called using a connection
pool or a transaction depending on logical requirements.

```go
package database

import (
    "context"
    "fmt"

    "github.com/vaardan/pgsq"
    "github.com/Masterminds/squirrel"
)


// CreateEntity creates new entity with the given name and returns its ID.
func CreateEntity(ctx context.Context, q pgsq.Queryable, name string) (int, error) {
	query := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Insert("entity_table").
		Columns("name").
		Values(name).
		Suffix("returning id")

	var id int
	err := q.Get(ctx, &id, query)
	if err != nil {
		return 0, fmt.Errorf("insert entity: %w", err)
	}

	return id, nil
}

```