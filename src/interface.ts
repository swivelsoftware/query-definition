import { IExpression, IGroupBy, IQuery, IResultColumn } from 'node-jql'
import { IQueryParams } from './queryParams'

type CommonFunc<T> = T | ((params: IQueryParams) => T | Promise<T>)

export type QueryArg = CommonFunc<Partial<IQuery>>

export type ResultColumnArg = CommonFunc<IResultColumn | IResultColumn[]>

export type ExpressionArg = CommonFunc<IExpression>

export type GroupByArg = CommonFunc<IGroupBy>

export type SubqueryArg = Partial<IQuery> | ((value: any, params?: IQueryParams) => Partial<IQuery> | Promise<Partial<IQuery>>)

export interface IVariableOptions {
  default?: any
  format?: string
}

export interface IVariable extends IVariableOptions {
  name: string
}

export type Prerequisite = CommonFunc<IQueryParams | string[]>

export interface IOptions {
  withDefault?: boolean   // apply subquery:default. default to be true
  skipDefFields?: boolean // skip field not registered
}
