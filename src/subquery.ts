import { IQuery, Query } from 'node-jql'
import { ICompanions, IQueryParams, SubqueryArg } from './interface'
import { findUnknowns, newQueryWithoutWildcard } from './utils'
import * as swig from 'swig-templates'
import debug = require('debug')

const log = debug('QueryDef:log')

interface IVariable {
  // variable name
  name: string

  // default value
  default?: any

  // value formatting
  format?: string
}

export class SubqueryDef {
  protected readonly arg: SubqueryArg
  protected readonly variables: IVariable[] = []
  protected readonly companion: ICompanions

  constructor(arg: SubqueryArg, companion: ICompanions = []) {
    this.arg = arg
    this.companion = companion
  }

  public get hasVariables(): boolean {
    return this.variables.length > 0
  }

  public get default(): any {
    return !this.variables.length
      ? true
      : this.variables.reduce((r, v) => {
        r[v.name] = v.default || null
        return r
      }, {} as any)
  }

  public register(
    name: string,
    i: number,
    { default: default_, format }: Partial<IVariable> = {}
  ): SubqueryDef {
    this.variables[i] = { name, default: default_, format }
    return this
  }

  public getCompanions(params: IQueryParams): string[] {
    return Array.isArray(this.companion) ? this.companion : this.companion(params)
  }

  public apply(params: IQueryParams): IQuery
  public apply(name: string, params: IQueryParams): IQuery
  public apply(...args: any[]): IQuery {
    let name: string, params: IQueryParams

    if (args.length === 1) {
      name = '__null__'
      params = args[0]
    } else {
      name = args[0]
      params = args[1]
    }

    let result: Query
    if (args.length > 1) {
      let value = params.subqueries && params.subqueries[name] || undefined
      result = newQueryWithoutWildcard(
        typeof this.arg === 'function' ? this.arg(value, params) : this.arg
      )
      const unknowns = findUnknowns(result)
      if (value === true) {
        value = this.default
      } else {
        value = Object.assign(this.default, value)
      }

      const applied: any[] = []
      for (let i = 0, length = unknowns.length; i < length; i += 1) {
        const unknown = unknowns[i]
        const variable = this.variables[i]
        if (variable) {
          let v = value[variable.name]
          if (variable.format) {
            v = swig.render(variable.format, { locals: { value } })
          }
          unknown.value = v
        }
        applied.push([variable ? variable.name : '(not-registered)', unknown.value])
      }

      if (!applied.length && typeof value === 'object') {
        const keys = Object.keys(value)
        for (const k of keys) applied.push([k, value[k]])
      }

      log(`Apply subquery '${name}' with (${applied.map(v => `${v[0]}=${JSON.stringify(v[1])}`).join(', ')})`)
    } else {
      result = newQueryWithoutWildcard(typeof this.arg === 'function' ? this.arg(params) : this.arg)
    }

    return result
  }

  public clone(): SubqueryDef {
    const newDef = new SubqueryDef(
      typeof this.arg === 'function' ? this.arg : new Query(this.arg),
      Array.isArray(this.companion) ? [...this.companion] : this.companion
    )
    for (let i = 0, length = this.variables.length; i < length; i += 1) {
      const v = this.variables[i]
      newDef.register(v.name, i, v)
    }
    return newDef
  }
}
