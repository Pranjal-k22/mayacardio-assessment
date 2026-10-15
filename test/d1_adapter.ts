import { DatabaseSync } from 'node:sqlite';

export class D1PreparedStatement {
  private db: DatabaseSync;
  private sql: string;
  private params: any[];

  constructor(db: DatabaseSync, sql: string, params: any[] = []) {
    this.db = db;
    this.sql = sql;
    this.params = params;
  }

  public bind(...values: any[]): D1PreparedStatement {
    return new D1PreparedStatement(this.db, this.sql, values);
  }

  public async first<T = any>(colName?: string): Promise<T | null> {
    const stmt = this.db.prepare(this.sql);
    const row = stmt.get(...this.params) as any;
    if (!row) return null;
    if (colName) return row[colName] ?? null;
    return row as T;
  }

  public async all<T = any>(): Promise<{ results: T[]; success: boolean; meta: any }> {
    const stmt = this.db.prepare(this.sql);
    const rows = stmt.all(...this.params) as T[];
    return {
      results: rows,
      success: true,
      meta: {
        duration: 0.1,
        rows_read: rows.length,
        rows_written: 0,
      },
    };
  }

  public async run(): Promise<{ success: boolean; meta: { changes: number; last_row_id: number } }> {
    const stmt = this.db.prepare(this.sql);
    stmt.run(...this.params);
    const changes = (this.db.prepare('SELECT changes() as c;').get() as any)?.c || 0;
    const lastId = (this.db.prepare('SELECT last_insert_rowid() as id;').get() as any)?.id || 0;
    return {
      success: true,
      meta: {
        changes,
        last_row_id: lastId,
      },
    };
  }

  public getRawSql(): string {
    return this.sql;
  }

  public getRawParams(): any[] {
    return this.params;
  }
}

export class D1DatabaseAdapter {
  private db: DatabaseSync;

  constructor(dbPath: string = ':memory:') {
    this.db = new DatabaseSync(dbPath);
  }

  public prepare(sql: string): D1PreparedStatement {
    return new D1PreparedStatement(this.db, sql);
  }

  public async batch(statements: D1PreparedStatement[]): Promise<any[]> {
    this.db.exec('BEGIN TRANSACTION;');
    const results: any[] = [];
    try {
      for (const s of statements) {
        const stmt = this.db.prepare(s.getRawSql());
        stmt.run(...s.getRawParams());
        const changes = (this.db.prepare('SELECT changes() as c;').get() as any)?.c || 0;
        results.push({ success: true, meta: { changes } });
      }
      this.db.exec('COMMIT;');
      return results;
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {}
      throw err;
    }
  }

  public async exec(sql: string): Promise<void> {
    this.db.exec(sql);
  }

  public close(): void {
    try {
      this.db.close();
    } catch {}
  }
}
