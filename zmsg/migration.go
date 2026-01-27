package zmsg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Migration 数据库迁移
type Migration struct {
	z    *zmsg
	sqls []string
	name string
}

// Load 加载单个 SQL 文件
func (z *zmsg) Load(filePath string) *Migration {
	m := &Migration{z: z, name: filepath.Base(filePath)}

	content, err := os.ReadFile(filePath)
	if err != nil {
		z.logger.Error("failed to read migration file", "path", filePath, "error", err)
		return m
	}

	m.sqls = parseSQLStatements(string(content))
	return m
}

// LoadSQL 直接加载 SQL 字符串
func (z *zmsg) LoadSQL(sql string) *Migration {
	m := &Migration{z: z, name: "inline"}
	m.sqls = parseSQLStatements(sql)
	return m
}

// LoadDir 加载目录下所有 SQL 文件（按文件名排序）
func (z *zmsg) LoadDir(dirPath string) *MigrationSet {
	ms := &MigrationSet{z: z}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		z.logger.Error("failed to read migration dir", "path", dirPath, "error", err)
		return ms
	}

	// 按文件名排序
	var sqlFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sql") {
			sqlFiles = append(sqlFiles, filepath.Join(dirPath, f.Name()))
		}
	}
	sort.Strings(sqlFiles)

	for _, file := range sqlFiles {
		ms.migrations = append(ms.migrations, z.Load(file))
	}

	return ms
}

// MigrationSet 迁移集合（用于增量迁移）
type MigrationSet struct {
	z          *zmsg
	migrations []*Migration
}

// Migrate 执行增量迁移
func (ms *MigrationSet) Migrate(ctx context.Context) error {
	// 确保迁移表存在
	if err := ms.ensureMigrationTable(ctx); err != nil {
		return err
	}

	applied := 0
	for _, m := range ms.migrations {
		// 检查是否已应用
		if ms.isApplied(ctx, m.name) {
			ms.z.logger.Debug("migration already applied", "name", m.name)
			continue
		}

		// 执行迁移
		if err := m.Migrate(ctx); err != nil {
			return fmt.Errorf("migration %s failed: %w", m.name, err)
		}

		// 记录已应用
		if err := ms.markApplied(ctx, m.name); err != nil {
			return err
		}

		applied++
	}

	if applied > 0 {
		ms.z.logger.Info("migrations completed", "applied", applied, "total", len(ms.migrations))
	} else {
		ms.z.logger.Info("no new migrations to apply")
	}

	return nil
}

func (ms *MigrationSet) ensureMigrationTable(ctx context.Context) error {
	_, err := ms.z.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS zmsg_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT NOW()
		)
	`)
	return err
}

func (ms *MigrationSet) isApplied(ctx context.Context, name string) bool {
	var exists bool
	err := ms.z.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM zmsg_migrations WHERE version = $1)", name).Scan(&exists)
	return err == nil && exists
}

func (ms *MigrationSet) markApplied(ctx context.Context, name string) error {
	_, err := ms.z.db.ExecContext(ctx,
		"INSERT INTO zmsg_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING", name)
	return err
}

// Migrate 执行单个迁移
func (m *Migration) Migrate(ctx context.Context) error {
	if len(m.sqls) == 0 {
		return nil
	}

	for i, sqlStmt := range m.sqls {
		if _, err := m.z.db.ExecContext(ctx, sqlStmt); err != nil {
			return fmt.Errorf("statement %d failed: %w\nSQL: %s", i+1, err, truncateSQL(sqlStmt))
		}
	}

	m.z.logger.Info("migration executed", "name", m.name, "statements", len(m.sqls))
	return nil
}

// MigrateWithTx 在事务中执行迁移
func (m *Migration) MigrateWithTx(ctx context.Context) error {
	if len(m.sqls) == 0 {
		return nil
	}

	tx, err := m.z.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for i, sqlStmt := range m.sqls {
		if _, err := tx.ExecContext(ctx, sqlStmt); err != nil {
			return fmt.Errorf("statement %d failed: %w\nSQL: %s", i+1, err, truncateSQL(sqlStmt))
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	m.z.logger.Info("migration executed", "name", m.name, "statements", len(m.sqls))
	return nil
}

// GetDB 获取数据库连接（用于高级操作）
func (z *zmsg) GetDB() *sql.DB {
	return z.db
}

// parseSQLStatements 解析 SQL 语句
func parseSQLStatements(content string) []string {
	var statements []string

	// 按分号分割，但要处理字符串内的分号
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(content); i++ {
		c := content[i]

		// 处理字符串
		if (c == '\'' || c == '"') && (i == 0 || content[i-1] != '\\') {
			if !inString {
				inString = true
				stringChar = c
			} else if c == stringChar {
				inString = false
			}
		}

		// 处理分号
		if c == ';' && !inString {
			stmt := cleanStatement(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// 处理最后一条语句（可能没有分号）
	stmt := cleanStatement(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// cleanStatement 清理语句：移除注释行，返回纯 SQL
func cleanStatement(s string) string {
	lines := strings.Split(s, "\n")
	var cleaned []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// 跳过空行和单行注释
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		// 移除行内注释
		if idx := strings.Index(line, "--"); idx > 0 {
			line = line[:idx]
		}
		cleaned = append(cleaned, line)
	}

	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

// truncateSQL 截断 SQL 用于错误显示
func truncateSQL(sqlStmt string) string {
	if len(sqlStmt) > 100 {
		return sqlStmt[:100] + "..."
	}
	return sqlStmt
}
