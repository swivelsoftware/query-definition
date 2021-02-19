import { IExpression, IQuery, Query, ColumnExpression, ResultColumn, AndExpressions, IResultColumn, GroupBy, IConditionalExpression, IGroupedExpressions, IGroupBy, OrderBy, Expression } from 'node-jql'
import _ = require('lodash')
import { ExpressionArg, GroupByArg, IBaseShortcut, ICompanions, IFieldArgShortcut, IFieldShortcut, IGroupByArgShortcut, IGroupByShortcut, IOptions, IOrderByArgShortcut, IOrderByShortcut, IQueryParams, IShortcut, IShortcutContext, IShortcutFunc, ISubqueryArgShortcut, ISubqueryShortcut, ITableArgShortcut, ITableShortcut, QueryArg, ResultColumnArg, SubqueryArg } from './interface'
import { SubqueryDef } from './subquery'
import { fixRegexp, merge, newQueryWithoutWildcard } from './utils'
import debug = require('debug')

const log = debug('QueryDef:log')
const warn = debug('QueryDef:warn')

export function registerShortcut<T extends IBaseShortcut>(name: string, func: IShortcutFunc<T>) {
  if (['table', 'field', 'subquery', 'groupBy', 'orderBy'].indexOf(name) === -1) {
    availableShortcuts[name] = func
  }
  else {
    warn(`Shortcut '${name}' cannot be overwritten`)
  }
}

const availableShortcuts: { [key: string]: IShortcutFunc<any> } = {
  table: function (this: QueryDef, { name, ...sc }: ITableShortcut | ITableArgShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: any) {
    let queryArg: QueryArg | undefined
    if ('fromTable' in sc) {
      queryArg = { $from: typeof sc.fromTable === 'function' ? sc.fromTable(context.registered) : sc.fromTable }
    }
    else if ('queryArg' in sc) {
      queryArg = sc.queryArg(context.registered)
    }
    if (queryArg) {
      if (typeof companions === 'function') {
        this.table(name, queryArg, companions)
      }
      else {
        this.table(name, queryArg, ...companions)
      }
    }
    else {
      warn(`Invalid table:${name}`)
    }
  },
  field: function (this: QueryDef, { name, ...sc }: IFieldShortcut | IFieldArgShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: any) {
    let queryArg: QueryArg | undefined
    if ('expression' in sc) {
      const expression = typeof sc.expression === 'function' ? sc.expression(context.registered) : sc.expression
      if ('registered' in sc && sc.registered) {
        context.registered[name] = expression
        if (typeof companions !== 'function') context.registeredCompanions[name] = companions
      }
      queryArg = { $select: new ResultColumn(expression, name) }
    }
    else if ('queryArg' in sc) {
      queryArg = sc.queryArg(context.registered)
    }
    if (queryArg) {
      if (typeof companions === 'function') {
        this.field(name, queryArg, companions)
      }
      else {
        this.field(name, queryArg, ...companions)
      }
    }
    else {
      warn(`Invalid field:${name}`)
    }
  },
  subquery: function (this: QueryDef, { name, ...sc }: ISubqueryShortcut | ISubqueryArgShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: any) {
    let subqueryArg: SubqueryArg | undefined
    if ('expression' in sc) {
      subqueryArg = { $where: typeof sc.expression === 'function' ? sc.expression(context.registered) : sc.expression }
    }
    else if ('subqueryArg' in sc) {
      subqueryArg = sc.subqueryArg(context.registered)
    }
    let subqueryDef: SubqueryDef
    if (subqueryArg) {
      if (typeof companions === 'function') {
        subqueryDef = this.subquery(name, subqueryArg, companions)
      }
      else {
        subqueryDef = this.subquery(name, subqueryArg, ...companions)
      }
      if ('unknowns' in sc) {
        if (Array.isArray(sc.unknowns)) {
          for (const [name, index] of sc.unknowns) {
            subqueryDef.register(name, index)
          }
        }
        else if (sc.unknowns && typeof sc.unknowns !== 'boolean' && sc.unknowns.fromTo) {
          const noOfUnknowns = sc.unknowns.noOfUnknowns || 2
          for (let i = 0, length = noOfUnknowns; i < length; i += 2) {
            subqueryDef.register('from', i)
            subqueryDef.register('to', i + 1)
          }
        }
        else if (sc.unknowns) {
          const noOfUnknowns = typeof sc.unknowns !== 'boolean' && sc.unknowns.noOfUnknowns || 1
          for (let i = 0, length = noOfUnknowns; i < length; i += 1) {
            subqueryDef.register('value', i)
          }
        }
      }
    }
    else {
      warn(`Invalid subquery:${name}`)
    }
  },
  groupBy: function (this: QueryDef, { name, ...sc }: IGroupByShortcut | IGroupByArgShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: any) {
    let queryArg: QueryArg | undefined
    if ('expression' in sc) {
      queryArg = { $group: new GroupBy([typeof sc.expression === 'function' ? sc.expression(context.registered) : sc.expression]) }
    }
    else if ('queryArg' in sc) {
      queryArg = sc.queryArg(context.registered)
    }
    if (queryArg) {
      if (typeof companions === 'function') {
        this.groupBy(name, queryArg, companions)
      }
      else {
        this.groupBy(name, queryArg, ...companions)
      }
    }
    else {
      warn(`Invalid groupBy:${name}`)
    }
  },
  orderBy: function (this: QueryDef, { name, ...sc }: IOrderByShortcut | IOrderByArgShortcut, companions: string[] | ((params: IQueryParams) => string[]), context: any) {
    let queryArg: QueryArg | undefined
    if ('expression' in sc) {
      const direction: 'ASC'|'DESC' = sc['direction'] || 'ASC'
      queryArg = { $order: new OrderBy(typeof sc.expression === 'function' ? sc.expression(context.registered) : sc.expression, direction) }
    }
    else if ('queryArg' in sc) {
      queryArg = sc.queryArg(context.registered)
    }
    if (queryArg) {
      if (typeof companions === 'function') {
        this.orderBy(name, queryArg, companions)
      }
      else {
        this.orderBy(name, queryArg, ...companions)
      }
    }
    else {
      warn(`Invalid orderBy:${name}`)
    }
  }
}

export class QueryDef {
  protected readonly base: QueryArg
  protected readonly subqueries: { [key: string]: SubqueryDef } = {}

  constructor(arg: QueryArg) {
    this.base = arg
  }

  public registered() {
    return Object.keys(this.subqueries)
  }

  public field(overwrite: boolean, name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public field(name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public field(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public field(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public field(...args: any[]): QueryDef {
    let overwrite = false,
      name: string,
      arg: QueryArg,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = typeof args[2] === 'function' ? args[2] : args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    }

    if (!name) throw new Error(`Invalid name '${name}'`)

    name = `field:${name}`
    if (!overwrite && this.subqueries[name]) throw new Error(`Field '${name}' already registered`)
    this.subqueries[name] = new SubqueryDef(arg, companion)
    log(`register ${name}`)
    return this
  }

  public groupField( overwrite: boolean, name: string, arg: ExpressionArg, prefix?: string, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public groupField( name: string, arg: ExpressionArg, prefix?: string, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public groupField( overwrite: boolean, name: string, arg: ExpressionArg, prefix?: string, ...companion: string[]): QueryDef
  public groupField( name: string, arg: ExpressionArg, prefix?: string, ...companion: string[]): QueryDef
  public groupField(...args: any[]): QueryDef {
    let overwrite = false,
      name: string,
      arg: ExpressionArg,
      prefix: string,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      prefix = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      prefix = args[3]
      companion = typeof args[4] === 'function' ? args[4] : args.slice(4)
    }

    if (!name) throw new Error(`Invalid name '${name}'`)

    function get(arg: ExpressionArg, params: IQueryParams): IExpression {
      return typeof arg === 'function' ? arg(params) : arg
    }
    function check(name: string, params: IQueryParams): boolean {
      if (params.fields && params.fields.length && params.groupBy && params.groupBy.length) {
        return params.fields.indexOf(name) > -1 && params.groupBy.indexOf(name) > -1
      }
      return false
    }

    // register field and sub-query
    const func = params => {
      const expr = get(arg, params)
      let resultColumn: ResultColumn
      if (check(name, params)) {
        resultColumn = new ResultColumn(expr, `${prefix}${name}`)
      } else {
        resultColumn = new ResultColumn(expr, name)
      }
      return { $select: [resultColumn] } as Partial<IQuery>
    }
    const func2 = params => {
      let groupBy: GroupBy
      if (check(name, params)) {
        groupBy = new GroupBy(`${prefix}${name}`)
      } else {
        groupBy = new GroupBy(get(arg, params))
      }
      return { $group: groupBy } as Partial<IQuery>
    }
    if (Array.isArray(companion)) {
      this.field(overwrite, name, func, ...companion)
      this.groupBy(overwrite, name, func2, ...companion)
    }
    else {
      this.field(overwrite, name, func, companion)
      this.groupBy(overwrite, name, func2, companion)
    }

    return this
  }

  public table(overwrite: boolean, name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public table(name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public table(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public table(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public table(...args: any[]): QueryDef {
    let overwrite = false,
      name: string,
      arg: QueryArg,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = typeof args[2] === 'function' ? args[2] : args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    }

    if (!name) throw new Error(`Invalid name '${name}'`)

    name = `table:${name}`
    if (!overwrite && this.subqueries[name]) throw new Error(`Table '${name}' already registered`)
    this.subqueries[name] = new SubqueryDef(arg, companion)
    log(`register ${name}`)
    return this
  }

  public groupBy(overwrite: boolean, name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public groupBy(name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public groupBy(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public groupBy(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public groupBy(...args: any[]): QueryDef {
    let overwrite = false,
      name: string,
      arg: QueryArg,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = typeof args[2] === 'function' ? args[2] : args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    }

    if (!name) throw new Error(`Invalid name '${name}'`)

    name = `groupBy:${name}`
    if (!overwrite && this.subqueries[name]) throw new Error(`Sub-query '${name}' already registered`)
    this.subqueries[name] = new SubqueryDef(arg, companion)
    log(`register ${name}`)
    return this
  }

  public subquery(overwrite: boolean, name: string, arg: SubqueryArg, getCompanions?: (params: IQueryParams) => string[]): SubqueryDef
  public subquery(name: string, arg: SubqueryArg, getCompanions?: (params: IQueryParams) => string[]): SubqueryDef
  public subquery(overwrite: boolean, name: string, arg: SubqueryArg, ...companion: string[]): SubqueryDef
  public subquery(name: string, arg: SubqueryArg, ...companion: string[]): SubqueryDef
  public subquery(...args: any[]): SubqueryDef {
    let overwrite = false,
      name: string,
      arg: SubqueryArg,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = typeof args[2] === 'function' ? args[2] : args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    }

    if (!name || name.endsWith(':')) throw new Error(`Invalid name '${name}'`)

    if (!overwrite && this.subqueries[name]) throw new Error(`Sub-query '${name}' already registered`)

    try {
      return (this.subqueries[name] = new SubqueryDef(arg, companion))
    }
    finally {
      log(`register subquery '${name}'`)
    }
  }

  public orderBy(overwrite: boolean, name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public orderBy(name: string, arg: QueryArg, getCompanions?: (params: IQueryParams) => string[]): QueryDef
  public orderBy(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public orderBy(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  public orderBy(...args: any[]): QueryDef {
    let overwrite = false,
      name: string,
      arg: QueryArg,
      companion: ICompanions

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = typeof args[2] === 'function' ? args[2] : args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = typeof args[3] === 'function' ? args[3] : args.slice(3)
    }

    if (!name) throw new Error(`Invalid name '${name}'`)

    name = `orderBy:${name}`
    if (!overwrite && this.subqueries[name]) throw new Error(`Sub-query '${name}' already registered`)
    this.subqueries[name] = new SubqueryDef(arg, companion)
    log(`register ${name}`)
    return this
  }

  public apply(params: IQueryParams = {}, options: IOptions = {}): Query {
    if (options.withDefault === undefined) options.withDefault = true
    const { withDefault, skipDefFields } = options

    if (!params.subqueries) params.subqueries = {}

    // register companions
    let allCompanions: string[] = []
    const depandCount: { [key: string]: number } = {}
    function register(this: QueryDef, key: string, path: string[] = []) {
      if (this.subqueries[key]) {
        if (allCompanions.indexOf(key) === -1) allCompanions.push(key)
        const count = path.length
        if (depandCount[key] === undefined) {
          depandCount[key] = count
        } else {
          depandCount[key] += count
        }
        path = [...path, key]
        const companions = this.subqueries[key].getCompanions(params)
        log(`Apply companions of ${key} = [${companions.join(', ')}]`)
        for (const k of companions) {
          if (path.indexOf(k) !== -1)
            throw new Error('Recursive dependency: ' + path.join(' -> ') + ' -> ' + k)
          register.apply(this, [k, path])
        }
      } else if (path.length) {
        throw new Error(`Companion '${key}' not found`)
      }
    }
    for (const f of params.fields || []) {
      if (typeof f === 'string') {
        const key = `field:${f}`
        register.apply(this, [key])
      }
    }
    for (const t of params.tables || []) {
      const key = `table:${t}`
      register.apply(this, [key])
    }
    for (const s of Object.keys(params.subqueries)) {
      register.apply(this, [s])
    }
    for (const g of params.groupBy || []) {
      if (typeof g === 'string') {
        const key = `groupBy:${g}`
        register.apply(this, [key])
      }
    }
    if (params.sorting) {
      for (const o of !Array.isArray(params.sorting) ? [params.sorting] : params.sorting) {
        if (typeof o === 'string') {
          const key = `orderBy:${o}`
          register.apply(this, [key])
        }
      }
    }

    // sort companions
    allCompanions = [...new Set(allCompanions)].sort((l, r) => {
      const lc = depandCount[l]
      const rc = depandCount[r]
      return lc < rc ? 1 : lc > rc ? -1 : 0
    })

    // classify companions
    const fields: string[] = []
    const tables: string[] = []
    const subqueries: string[] = []
    const groupbys: string[] = []
    const orderbys: string[] = []
    for (const k of allCompanions) {
      const pcs = k.split(':')
      switch (pcs[0]) {
        case 'field':
          fields.push(pcs[1])
          break
        case 'table':
          tables.push(pcs[1])
          break
        case 'groupBy':
          groupbys.push(pcs[1])
          break
        case 'orderBy':
          orderbys.push(pcs[1])
          break
        default:
          subqueries.push(k)
          break
      }
    }

    (() => {
      const { conditions, constants, ...params_ } = params
      params_['conditions'] = params_['constants'] = '[Object object]'
      log(`params before: ${JSON.stringify(params_)}`)
    })()

    // apply companions
    params = _.cloneDeep(params)
    if (params.fields) {
      params.fields = params.fields.reduce<any[]>((r, f) => {
        if (typeof f !== 'string' || r.indexOf(f) === -1) r.push(f)
        return r
      }, fields)
    } else {
      params.fields = fields
    }
    if (params.tables) {
      params.tables = params.tables.reduce<any[]>((r, f) => {
        if (typeof f !== 'string' || r.indexOf(f) === -1) r.push(f)
        return r
      }, tables)
    } else {
      params.tables = tables
    }
    if (params.subqueries) {
      for (const s of subqueries) {
        if (!params.subqueries[s] && this.subqueries[s]) {
          params.subqueries[s] = this.subqueries[s].default
        }
      }
    } else {
      params.subqueries = subqueries.reduce((r, s) => {
        if (this.subqueries[s]) {
          r[s] = this.subqueries[s].default
          return r
        }
      }, {} as any)
    }
    if (params.groupBy) {
      params.groupBy = groupbys.reduce<any[]>((r, f) => {
        if (r.indexOf(f) === -1) r.push(f)
        return r
      }, params.groupBy)
    } else {
      params.groupBy = groupbys
    }
    if (params.sorting) {
      if (!Array.isArray(params.sorting)) {
        params.sorting = [params.sorting]
      }
      params.sorting = orderbys.reduce<any[]>((r, f) => {
        if (r.indexOf(f) === -1) r.push(f)
        return r
      }, params.sorting)
    } else {
      params.sorting = orderbys
    }

    // default
    if (withDefault && this.subqueries['default']) {
      if (!params.subqueries) params.subqueries = {}
      params.subqueries = {
        ...params.subqueries,
        default: true
      }
    }

    (() => {
      const { conditions, constants, ...params_ } = params
      params_['conditions'] = params_['constants'] = '[Object object]'
      log(`params after: ${JSON.stringify(params_)}`)
    })()

    // prepare base query
    let base: IQuery
    if (typeof this.base === 'function') {
      const params_ = _.cloneDeep(params)
      base = newQueryWithoutWildcard(this.base(params_))
    } else {
      base = newQueryWithoutWildcard(this.base)
    }

    // fields
    if (params.fields && params.fields.length) {
      const $select = (base.$select = [] as IResultColumn[])
      $select.push(
        ...params.fields.reduce((r, f) => {
          let fields: IResultColumn | IResultColumn[] | undefined

          // string
          if (typeof f === 'string') {
            const key = `field:${f}`
            if (this.subqueries[key]) {
              const registered = this.subqueries[key]
              const { $distinct, $select } = registered.apply(params)
              if ($distinct) base.$distinct = true
              fields = newQueryWithoutWildcard({ $select }).$select
              if (!fields.length) throw new Error(`No result columns returned from '${key}'`)
            } else if (!skipDefFields) {
              fields = { expression: new ColumnExpression(f) }
            }
          }
          // [string, string]
          else if (Array.isArray(f)) {
            fields = { expression: new ColumnExpression(...f) }
          }
          // IResultColumnShortcut
          else if ('column' in f) {
            fields = {
              expression: new ColumnExpression(...f.column),
              $as: f.$as
            }
          }
          // IResultColumn
          else {
            fields = f
          }

          if (fields) {
            if (!Array.isArray(fields)) fields = [fields]
            r.push(...fields.map(f => new ResultColumn(f)))
          }
          return r
        }, [] as ResultColumn[])
      )
    }

    // tables
    if (params.tables && params.tables.length) {
      for (const t of params.tables) {
        const key = `table:${t}`
        if (this.subqueries[key]) {
          const { $from, $where } = this.subqueries[key].apply(params)
          merge(base, newQueryWithoutWildcard({ $from, $where }))
          log(`Apply ${key}`)
        }
      }
    }

    // subqueries
    if (params.subqueries) {
      for (const s of Object.keys(params.subqueries)) {
        if (this.subqueries[s]) {
          const subquery = newQueryWithoutWildcard(this.subqueries[s].apply(s, params))
          merge(base, subquery)
        }
      }
    }

    // conditions
    if (params.conditions) {
      if (!base.$where) {
        base.$where = params.conditions
      } else if ((base.$where as IConditionalExpression).classname !== 'AndExpressions') {
        base.$where = new AndExpressions([base.$where as IConditionalExpression, params.conditions])
      } else {
        ;(base.$where as IGroupedExpressions).expressions.push(params.conditions)
      }
    }

    // groupBy
    if (params.groupBy && params.groupBy.length) {
      const $group = (base.$group = (base.$group || { expressions: [] }) as IGroupBy)
      const expressions = $group.expressions as IExpression[]
      const $having: IConditionalExpression[] = $group.$having
        ? [$group.$having as IConditionalExpression]
        : []

      function apply({ expressions: e, $having: h }: IGroupBy) {
        const e_ = Array.isArray(e) ? e : [e]
        expressions.push(...e_)

        if (h) {
          const h_ = Array.isArray(h) ? h : [h]
          $having.push(...h_)
        }
      }

      for (const g of params.groupBy) {
        // string
        if (typeof g === 'string') {
          const key = `groupBy:${g}`
          if (this.subqueries[key]) {
            const $group = this.subqueries[key].apply(params).$group
            const group = newQueryWithoutWildcard({ $group }).$group
            if (!group) throw new Error(`No GROUP BY returned from '${key}'`)
            apply(group)
            log(`Apply ${key}`)
          } else {
            expressions.push(new ColumnExpression(g))
          }
        }
        // IGroupBy
        else {
          apply(g)
        }
      }

      $group.$having = !$having.length
        ? undefined
        : $having.length === 1
          ? $having[0]
          : new AndExpressions($having)

      base.$group = { expressions, $having }
    }

    // sorting
    if (params.sorting) {
      if (!Array.isArray(params.sorting)) params.sorting = [params.sorting]
      if (!base.$order) base.$order = []
      let $order = base.$order
      if (typeof $order === 'string') base.$order = $order = [new OrderBy($order)]
      if (!Array.isArray($order)) base.$order = $order = [$order]
      for (const o of params.sorting) {
        // string
        if (typeof o === 'string') {
          const key = `orderBy:${o}`
          if (this.subqueries[key]) {
            const value = this.subqueries[key].apply(params).$order
            const order = newQueryWithoutWildcard({ $order: value }).$order
            if (!order) throw new Error(`No ORDER BY returned from '${key}'`)
            $order.push(...order)
            log(`Apply ${key}`)
          } else {
            $order.push(new OrderBy(o))
          }
        }
        // IOrderBy
        else {
          $order.push(o)
        }
      }
    }

    // limit
    if (params.limit) {
      if (typeof params.limit === 'number') {
        params.limit = { $limit: params.limit }
      }
      base.$limit = params.limit
    }

    if (params.distinct) {
      base.$distinct = true
    }

    const result = new Query(base)
    fixRegexp(result)
    return result
  }

  // backward compatible
  public registerQuery(name: string, arg: Query | QueryArg, ...companion: string[]): SubqueryDef {
    return this.subquery(name, arg, ...companion)
  }

  // backward compatible
  public registerResultColumn(
    name: string,
    arg: IResultColumn | ResultColumnArg,
    ...companion: string[]
  ): QueryDef {
    return this.field(
      name,
      params => {
        const resultColumn = typeof arg === 'function' ? arg(params) : arg
        return { $select: resultColumn }
      },
      ...companion
    )
  }

  // backward compatible
  public registerGroupBy(name: string, arg: IGroupBy | GroupByArg, ...companion: string[]) {
    return this.groupBy(
      name,
      params => {
        const groupBy = typeof arg === 'function' ? arg(params) : arg
        return { $group: groupBy }
      },
      ...companion
    )
  }

  // backward compatible
  public register(name: string, arg: IResultColumn | IGroupBy, ...companion: string[])
  public register(name: string, arg: Query | QueryArg, ...companion: string[]): SubqueryDef
  public register(
    name: string,
    arg: Query | QueryArg | IResultColumn | IGroupBy,
    ...companion: string[]
  ): any {
    if (arg instanceof Query || typeof arg === 'function') {
      return this.registerQuery(name, arg, ...companion)
    } else if ('expression' in arg) {
      return this.registerResultColumn(name, arg, ...companion)
    } else if ('classname' in arg && arg.classname === 'GroupBy') {
      return this.registerGroupBy(name, arg as IGroupBy, ...companion)
    } else {
      return this.registerQuery(name, arg as Partial<IQuery>, ...companion)
    }
  }

  // backward compatible
  public registerBoth(overwrite: boolean, name: string, arg: ExpressionArg, ...companion: string[])
  public registerBoth(name: string, arg: ExpressionArg, ...companion: string[])
  public registerBoth(...args: any[]) {
    let overwrite = false,
      name: string,
      arg: ExpressionArg,
      companion: string[]

    if (typeof args[0] === 'string') {
      name = args[0]
      arg = args[1]
      companion = args.slice(2)
    } else {
      overwrite = args[0]
      name = args[1]
      arg = args[2]
      companion = args.slice(3)
    }

    return this.groupField(overwrite, name, arg, 'group_', ...companion)
  }

  public useShortcuts(shortcuts: IShortcut[], options: any = {}): QueryDef {
    const registered: { [key: string]: Expression } = new Proxy({}, {
      get(target, name) {
        if (!target[name]) throw new Error(`expression '${String(name)}' not registered`)
        if (typeof companions !== 'function' && registeredCompanions[name as string]) companions.push(...registeredCompanions[name as string])
        return target[name]
      }
    })
    const registeredCompanions: { [key: string]: string[] } = {}
    const context: IShortcutContext = { registered, registeredCompanions, options }
    let companions: string[] | ((params: IQueryParams) => string[]) = []

    for (const sc of shortcuts) {
      const { type, name } = sc
      companions = sc.companions || []
      if (availableShortcuts[type]) {
        availableShortcuts[type].apply(this, [sc, companions, context])
      }
      else {
        warn(`Invalid ${String(type)}:${name}`)
      }
    }
    return this
  }

  public clone(): QueryDef {
    const newDef = new QueryDef(typeof this.base === 'function' ? this.base : new Query(this.base))
    for (const name of Object.keys(this.subqueries)) {
      newDef.subqueries[name] = this.subqueries[name].clone()
    }
    return newDef
  }
}
