import merge from 'deepmerge'
import { IQuery, Query } from 'node-jql'
import { IVariable, IVariableOptions, Prerequisite, SubqueryArg } from './interface'
import { IQueryParams } from './queryParams'
import { dummyQuery, getUnknowns } from './utils'
import * as swig from 'swig-templates'

export class SubqueryDef {
  private readonly variables: IVariable[] = []

  constructor(
    private readonly arg: SubqueryArg,
    private readonly prerequisite?: Prerequisite
  ) {}

  get hasVariables(): boolean {
    return this.variables.length > 0
  }

  get default(): any {
    if (!this.variables.length) return true
    
    return this.variables.reduce<any>((r, v) => {
      r[v.name] = v.default || null
      return r
    }, {})
  }

  register(name: string, i: number, options: IVariableOptions = {}): SubqueryDef {
    this.variables[i] = { name, ...options }
    return this
  }

  async apply(params: IQueryParams): Promise<IQuery>
  async apply(name: string, params: IQueryParams): Promise<IQuery>
  async apply(arg0: string|IQueryParams, arg1?: IQueryParams): Promise<IQuery> {
    let name = '', params: IQueryParams
    if (arg1) {
      name = arg0 as string
      params = arg1
    }
    else {
      params = arg0 as IQueryParams
    }

    let result: Query
    if (name && name.length) {
      let value = params.subqueries && params.subqueries[name] || undefined
      result = dummyQuery(typeof this.arg === 'function' ? await this.arg(value, params) : this.arg)
      const unknowns = getUnknowns(result)
      value = value === true ? this.default : Object.assign(this.default, value)

      for (let i = 0, length = unknowns.length; i < length; i += 1) {
        const unknown = unknowns[i]
        const variable = this.variables[i]
        if (variable) {
          unknown.value = variable.format ? swig.render(variable.format, { locals: { value } }) : value[variable.name]
        }
      }
    }
    else {
      result = dummyQuery(typeof this.arg === 'function' ? await this.arg(params) : this.arg)
    }

    return result
  }

  async applyPrerequisite(params: IQueryParams) {
    if (this.prerequisite) {
      let prerequisite = this.prerequisite
      if (Array.isArray(prerequisite)) return prerequisite
      else if (typeof prerequisite === 'function') return prerequisite(params)
      else merge(params, prerequisite)
    }
    return {}
  }

  clone(): SubqueryDef {
    const subqueryDef = new SubqueryDef(
      typeof this.arg === 'function' ? this.arg : new Query(this.arg),
      !Array.isArray(this.prerequisite) ? this.prerequisite : [...(this.prerequisite || [])]
    )
    for (let i = 0, length = this.variables.length; i < length; i += 1) {
      const { name, ...v } = this.variables[i]
      subqueryDef.register(name, i, v)
    }
    return subqueryDef
  }
}