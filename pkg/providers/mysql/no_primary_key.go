package mysql

import (
	"context"
	"fmt"
	"sort"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

const noPrimaryKeyTablesQuery = `
	SELECT
		t.table_schema,
		t.table_name
	FROM information_schema.tables t
	WHERE
		t.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		AND (t.table_schema = ? || ? = '')
		AND t.table_type = 'BASE TABLE'
		AND NOT EXISTS (
			SELECT 1
			FROM information_schema.table_constraints c
			WHERE
				c.table_schema = t.table_schema
				AND c.table_name = t.table_name
				AND c.constraint_type = 'PRIMARY KEY'
		)
`

func loadNoPrimaryKeyTables(ctx context.Context, tx queryExecutor, database string) (map[abstract.TableID]bool, error) {
	rows, err := tx.QueryContext(ctx, noPrimaryKeyTablesQuery, database, database)
	if err != nil {
		return nil, xerrors.Errorf("unable to select tables without primary key: %w", err)
	}
	defer rows.Close()

	tables := make(map[abstract.TableID]bool)
	for rows.Next() {
		var tID abstract.TableID
		if err := rows.Scan(&tID.Namespace, &tID.Name); err != nil {
			return nil, xerrors.Errorf("unable to scan table without primary key: %w", err)
		}
		tables[tID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("unable to read tables without primary key: %w", err)
	}
	return tables, nil
}

func excludedTableNames[V any](tables map[abstract.TableID]V, noPrimaryKeyTables map[abstract.TableID]bool) []string {
	var excluded []string
	for tID := range noPrimaryKeyTables {
		if _, ok := tables[tID]; ok {
			delete(tables, tID)
			excluded = append(excluded, tID.Fqtn())
		}
	}
	sort.Strings(excluded)
	return excluded
}

func (s *Storage) excludeNoPrimaryKeyTables(ctx context.Context, tables abstract.TableMap) error {
	noPrimaryKeyTables, err := loadNoPrimaryKeyTables(ctx, s.DB, s.database)
	if err != nil {
		return xerrors.Errorf("unable to load tables without primary key: %w", err)
	}
	if excluded := excludedTableNames(tables, noPrimaryKeyTables); len(excluded) > 0 {
		logger.Log.Warn("Tables without primary key are excluded from the transfer", log.Strings("tables", excluded))
	}
	return nil
}

func (p *publisher) excludeNoPrimaryKeyTables(schema abstract.DBSchema) error {
	noPrimaryKeyTables, err := loadNoPrimaryKeyTables(context.Background(), p.storage.DB, p.storage.database)
	if err != nil {
		return xerrors.Errorf("unable to load tables without primary key: %w", err)
	}
	p.canal.excludeNoPrimaryKeyTables(noPrimaryKeyTables)
	if excluded := excludedTableNames(schema, noPrimaryKeyTables); len(excluded) > 0 {
		p.logger.Warn("Tables without primary key are excluded from the replication", log.Strings("tables", excluded))
	}
	return nil
}

func (c *Canal) excludeNoPrimaryKeyTables(tables map[abstract.TableID]bool) {
	c.tableLock.Lock()
	defer c.tableLock.Unlock()
	for tID := range tables {
		c.noPrimaryKeyTables[fmt.Sprintf("%s.%s", tID.Namespace, tID.Name)] = true
	}
}
