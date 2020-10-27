import { IResultColumn, IOrderBy, IConditionalExpression, ILimitOffset, IGroupBy, IQuery, IExpression, IFromTable, Expression } from 'node-jql'
import { QueryDef } from '.'

// general partial query argument
export type QueryArg = Partial<IQuery> | ((params: IQueryParams) => Partial<IQuery>)

// sub-query argument
export type SubqueryArg = Partial<IQuery> | ((value: any, params?: IQueryParams) => Partial<IQuery>)

// expression argument
export type ExpressionArg = IExpression | ((params: IQueryParams) => IExpression)

// companion argument
export type ICompanions = string[]|((params: IQueryParams) => string[])

// result column argument
export type ResultColumnArg =
  | IResultColumn
  | ((params: IQueryParams) => IResultColumn)
  | ((params: IQueryParams) => IResultColumn[])

// group by argument
export type GroupByArg = IGroupBy | ((params: IQueryParams) => IGroupBy)

// shortcut of ResultColumn
export interface IResultColumnShortcut {
  // [table, column]
  column: [string, string]

  // AS alias
  $as?: string
}

// for applying to QueryDef
export interface IQueryParams {
  // DISTINCT flag
  distinct?: boolean

  // fields to be shown. affects SELECT and FROM
  fields?: Array<IResultColumn | string | [string, string] | IResultColumnShortcut>

  // tables to be joined. affects FROM
  tables?: string[]

  // sub-queries to be applied. affects FROM, WHERE and GROUP BY
  subqueries?: { [key: string]: true | { value: any } | { from: any; to: any } | any }

  // extra WHERE conditions
  conditions?: IConditionalExpression

  // extra GROUP BY statements
  groupBy?: Array<IGroupBy | string>

  // extra ORDER BY statements
  sorting?: string | IOrderBy | Array<string|IOrderBy>

  // extra LIMIT statement
  limit?: number | ILimitOffset

  // constants injected by program
  constants?: any
}

export interface IShortcutContext {
  registered: { [key: string]: Expression }
  registeredCompanions: { [key: string]: string[] }
  options: any
}

export type IShortcutFunc = (this: QueryDef, sc: IShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: IShortcutContext & any) => void

export interface IBaseShortcut {
  type: 'field'|'table'|'subquery'|'groupBy'|'orderBy'|'combination'|'nestedSummary'|'summaryField'|'queryCondition'|'dateField'
  name: string
  companions?: string[]|((params: IQueryParams) => string[])
}

export interface ITableShortcut extends IBaseShortcut {
  type: 'table'
  fromTable: IFromTable|((registered: { [key: string]: Expression }) => IFromTable)
}

export interface ITableArgShortcut extends IBaseShortcut {
  type: 'table'
  queryArg: (registered: { [key: string]: Expression }) => QueryArg
}

export interface IFieldShortcut extends IBaseShortcut {
  type: 'field'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
  registered?: boolean
}

export interface IFieldArgShortcut extends IBaseShortcut {
  type: 'field'
  queryArg: (registered: { [key: string]: Expression }) => QueryArg
}

export interface IUnknownShortcut {
  noOfUnknowns?: number
  fromTo?: boolean
}

export type IUnknownType = boolean|IUnknownShortcut|Array<[string, number]>

export interface ISubqueryShortcut extends IBaseShortcut {
  type: 'subquery'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
  unknowns?: IUnknownType
}

export interface ISubqueryArgShortcut extends IBaseShortcut {
  type: 'subquery'
  subqueryArg: (registered: { [key: string]: Expression }) => SubqueryArg
  unknowns?: IUnknownType
}

export interface IGroupByShortcut extends IBaseShortcut {
  type: 'groupBy'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
}

export interface IGroupByArgShortcut extends IBaseShortcut {
  type: 'groupBy'
  queryArg: (registered: { [key: string]: Expression }) => QueryArg
}

export interface IOrderByShortcut extends IBaseShortcut {
  type: 'orderBy'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
  direction?: 'ASC'|'DESC'
}

export interface IOrderByArgShortcut extends IBaseShortcut {
  type: 'orderBy'
  queryArg: (registered: { [key: string]: Expression }) => QueryArg
}

export interface ICombinationShortcut extends IBaseShortcut {
  type: 'combination'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
  registered?: true
  dateField?: boolean
}

export interface ICombinationArgShortcut extends IBaseShortcut {
  type: 'combination'
  exprArg: (registered: { [key: string]: Expression }) => (params: IQueryParams) => Expression
  dateField?: boolean
}

export interface INestedSummaryShortcut extends IBaseShortcut {
  type: 'nestedSummary'
  cases: Array<{ typeCode: string; condition: IConditionalExpression|((registered: { [key: string]: Expression }) => IConditionalExpression)}>
}

export interface ISummaryFieldShortcut extends IBaseShortcut {
  type: 'summaryField'
  summaryType: 'count' | 'sum'
  expression: Expression|((registered: { [key: string]: Expression }) => Expression)
  inReportExpression?: Expression|((registered: { [key: string]: Expression }) => Expression)
  jobDateExpression: Expression|((registered: { [key: string]: Expression }) => Expression)
}

export interface IQueryConditionShortcut extends IBaseShortcut {
  type: 'queryCondition'
  query: IQuery|((registered: { [key: string]: Expression }) => IQuery)
  idExpression: Expression|((registered: { [key: string]: Expression }) => Expression)
}

export type IShortcut =
  ITableShortcut|ITableArgShortcut|
  IFieldShortcut|IFieldArgShortcut|
  ISubqueryShortcut|ISubqueryArgShortcut|
  IGroupByShortcut|IGroupByArgShortcut|
  IOrderByShortcut|IOrderByArgShortcut|
  ICombinationShortcut|ICombinationArgShortcut|
  INestedSummaryShortcut|
  ISummaryFieldShortcut|
  IQueryConditionShortcut