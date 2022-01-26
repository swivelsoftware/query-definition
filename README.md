# query-definition

# Documentation

## IQueryParams

```js
{
  // DISTINCT flag
  distinct?: boolean

  // SELECT fields
  fields?: FieldParams[]

  // FROM tables
  tables?: string[]

  // subqueries applied
  subqueries?: { [key: string]: true | { value: any } | { from: any; to: any } | any }

  // GROUP BY subqueries
  groupBy?: GroupByParams[]

  // ORDER BY subqueries
  sorting?: OrderByParams | OrderByParams[]

  // LIMIT rows
  limit?: number | ILimitOffset
}
```

## QueryArg

[IQuery](https://github.com/swivelsoftware/node-jql#IQuery)  
[IQueryParams](#IQueryParams)

```js
type QueryArg = Partial<IQuery> | ((params: IQueryParams) => Partial<IQuery> | Promise<Partial<IQuery>>)
```

## SubqueryArg

[IQuery](https://github.com/swivelsoftware/node-jql#IQuery)  
[IQueryParams](#IQueryParams)

```js
type SubqueryArg = Partial<IQuery> | ((value: any, params?: IQueryParams) => Partial<IQuery> | Promise<Partial<IQuery>>)
```

## ExpressionArg

[IExpression](https://github.com/swivelsoftware/node-jql#IExpression)  
[IQueryParams](#IQueryParams)

```js
type ExpressionArg = IExpression | ((params: IQueryParams) => IExpression | Promise<IExpression>)
```

## Prerequisite

[IQuery](https://github.com/swivelsoftware/node-jql#IQuery)  
[IQueryParams](#IQueryParams)

```js
type Prerequisite = Partial<IQuery> | ((params: IQueryParams) => Partial<IQuery> | Promise<Partial<IQuery>>)
```

## IOptions

```js
interface IOptions {
  // automatically apply subquery:default. default to be true
  withDefault?: boolean

  // skip field not registered. otherwise automatically apply new ResultColumn(fieldNotRegistered)
  skipDefFields?: boolean
}
```

## IVariableOptions

```js
interface IVariableOptions {
  default?: any
  format?: string
}
```

## SubqueryDef

[IVariableOptions](#IVariableOptions)

```js
let subqueryDef: SubqueryDef

// check if Unknown(s) registered
subqueryDef.hasVariables: boolean

// register an Unknown
subqueryDef.register(name: string, i: number, options: IVariableOptions): SubqueryDef
```

## IBaseShortcut

[Prerequisite](#Prerequisite)

```js
interface IBaseShortcut {
  // to check which shortcut function to be used. by default 'field' | 'table' | 'subquery' | 'groupBy' | 'orderBy'
  type: string

  // name of the subquery
  name: string

  // subquery(s) to be applied with this subquery
  prerequisite?: Prerequisite
}
```

## ShortcutFunc

```js
type ShortcutFunc<T extends IBaseShortcut, U = any, R = any> = (this: QueryDef, shortcut: T, context: IShortcutContext & U, options?: R) => Promise<void>
```

## QueryDef

[ShortcutFunc](#ShortcutFunc)  
[IBaseShortcut](#IBaseShortcut)  
[QueryArg](#QueryArg)  
[SubqueryArg](#SubqueryArg)  
[ExpressionArg](#ExpressionArg)  
[Prerequisite](#Prerequisite)  
[IQueryParams](#IQueryParams)  
[SubqueryDef](#SubqueryDef)
[DefaultShortcuts](#Shortcuts)  
[IOptions](#IOptions)

```js
// register customized shortcut type
QueryDef.registerShortcut<T extends IBaseShortcut>(name: string, func: ShortcutFunc<T>)

const queryDef = new QueryDef(QueryArg)

// register field
queryDef.field(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
queryDef.field(overwrite: true, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef

// register field for grouping
queryDef.groupField(name: string, arg: ExpressionArg, prefix: string, prerequisite?: Prerequisite): QueryDef

// register table. to register JOIN table, use the same FROM table with the JOIN statement
queryDef.table(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
queryDef.table(overwrite: true, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef

// register subquery
queryDef.subquery(name: string, arg: SubqueryArg, prerequisite?: Prerequisite): SubqueryDef
queryDef.subquery(overwrite: true, name: string, arg: SubqueryArg, prerequisite?: Prerequisite): SubqueryDef

// register GROUP BY
queryDef.groupBy(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
queryDef.groupBy(overwrite: true, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef

// register ORDER BY
queryDef.orderBy(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
queryDef.orderBy(overwrite: true, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef

// register the above subqueries as shortcuts
queryDef.useShortcuts<T extends IBaseShortcut = IBaseShortcut, U = any>(shortcuts: Array<DefaultShortcuts | T>, options?: U): Promise<QueryDef>

// get SQL query with parameters
queryDef.apply(params?: IQueryParams, options?: IOptions): Promise<Query>
```

## Shortcuts

[IBaseShortcut](#IBaseShortcut)  
[QueryArg](#QueryArg)  
[SubqueryArg](#SubqueryArg)

```js
type UnknownType = boolean    // has 1 Unknown
  | { noOfUnknowns?: number } // has n Unknowns which are all linked to {value}
  | { fromTo?: boolean }      // has 2 Unknowns which are linked to {from} and {to} respectively
  | Array<[string, number]>   // has n Unknowns which are linked to 1st {value1}, 2nd {value2}, ... respectively

interface IQueryArgShortcut extends IBaseShortcut {
  type: 'field' | 'table' | 'groupBy' | 'orderBy'
  queryArg: QueryArg | ((registered: any) => QueryArg | Promise<QueryArg>)
}

interface IFieldShortcut extends IBaseShortcut {
  type: 'field'
  expression: IExpression | ((registered: any) => IExpression | Promise<IExpression>)

  // to be registered for re-use
  registered?: boolean
}

interface ITableShortcut extends IBaseShortcut {
  type: 'table'
  fromTable: IFromTable | ((registered: any) => IFromTable | Promise<IFromTable>)
}

interface ISubqueryShortcut extends IBaseShortcut {
  type: 'subquery'
  expression: IExpression | ((registered: any) => IExpression | Promise<IExpression>)
  unknowns?: UnknownType
}

interface ISubqueryArgShortcut extends IBaseShortcut {
  type: 'subquery'
  subqueryArg: SubqueryArg | ((registered: any) => SubqueryArg | Promise<SubqueryArg>)
  unknowns?: UnknownType
}

interface IGroupByShortcut extends IBaseShortcut {
  type: 'groupBy'
  expression: IExpression | ((registered: any) => IExpression | Promise<IExpression>)
}

interface IOrderByShortcut extends IBaseShortcut {
  type: 'orderBy'
  expression: IExpression | ((registered: any) => IExpression | Promise<IExpression>)
  direction?: 'ASC'|'DESC'
}
```