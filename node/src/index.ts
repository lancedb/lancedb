/**
 * Connect to a LanceDB instance at the given URI
 * @param uri The uri of the database.
 */
export function connect (uri: string): Connection {
  return new Connection(uri)
}

/**
 * A connection to a LanceDB database.
 */
export class Connection {
  private readonly _uri: string

  constructor (uri: string) {
    this._uri = uri
  }

  get uri (): string {
    return this._uri
  }

  /**
     * Get the names of all tables in the database.
     */
  tableNames (): string[] {
    return ['vectors']
  }

  /**
     * Open a table in the database.
     * @param name The name of the table.
     */
  openTable (name: string): Table {
    return new Table(this, name)
  }
}

/**
 * A table in a LanceDB database.
 */
export class Table {
  private readonly _connection: Connection
  private readonly _name: string

  constructor (connection: Connection, name: string) {
    this._connection = connection
    this._name = name
  }

  get name (): string {
    return this._name
  }

  /**
     * Create a search query to find the nearest neighbors of the given query vector.
     * @param query The query vector.
     */
  query (query: number[]): QueryBuilder {
    return new QueryBuilder(this, query)
  }
}

/**
 * A builder for nearest neighbor queries for LanceDB.
 */
export class QueryBuilder {
  private readonly _table: Table
  private readonly _query: number[]
  private readonly _limit: number
  private readonly _refine_factor?: number
  private readonly _nprobes: number
  private readonly _columns?: string[]
  private readonly _where?: string
  private readonly _metric = 'L2'

  constructor (table: Table, query: number[]) {
    this._table = table
    this._query = query
    this._limit = 10
    this._nprobes = 20
    this._refine_factor = undefined
    this._columns = undefined
    this._where = undefined
  }

  /**
     * Execute the query and return the results as an Array of Objects
     */
  execute (): Array<Record<string, unknown>> {
    return [
      {
        vector: [1.0, 2.0, 3.0], id: 1, text: 'Hello World'
      }
    ]
  }

  /**
     * Execute the query and return the results as an Array of the generic type provided
     */
  execute_cast<T>(): T[] {
    return [
      {
        vector: [1.0, 2.0, 3.0], id: 1, text: 'Hello World'
      }
    ] as T[]
  }
}
