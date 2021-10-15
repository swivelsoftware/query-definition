import debug = require('debug')
import _ from 'lodash'
import { AndExpressions, ColumnExpression, GroupBy, IConditionalExpression, IExpression, IGroupBy, IGroupedExpressions, IQuery, IResultColumn, OrderBy, Query, ResultColumn } from 'node-jql'
import { ExpressionArg, GroupByArg, Prerequisite, QueryArg, ResultColumnArg, SubqueryArg, IOptions } from './interface'
import { IQueryParams, FieldParams, GroupByParams, OrderByParams } from './queryParams'
import { SubqueryDef } from './subquery'
import { dummyQuery, mergePrerequisite, mergeQuery } from './utils'
import { FixRegexpProcessor, PostProcessor } from './postProcessors'
import { FieldShortcutFunc, GroupByShortcutFunc, IBaseShortcut, OrderByShortcutFunc, ShortcutFunc, SubqueryShortcutFunc, TableShortcutFunc, IShortcutContext, DefaultShortcuts, CombinationShortcutFunc as CombinationShortcutFunc } from './shortcuts'

const log = debug('QueryDef:log')
const warn = debug('QueryDef:warn')

export class QueryDef {
  static readonly postProcessors: PostProcessor[] = [
    FixRegexpProcessor
  ]

  static readonly shortcuts: { [key: string]: ShortcutFunc<any> } = {
    field: FieldShortcutFunc,
    table: TableShortcutFunc,
    subquery: SubqueryShortcutFunc,
    combination: CombinationShortcutFunc,
    groupBy: GroupByShortcutFunc,
    orderBy: OrderByShortcutFunc
  }

  static registerShortcut<T extends IBaseShortcut>(name: string, func: ShortcutFunc<T>) {
    if (['table', 'field', 'subquery', 'groupBy', 'orderBy'].indexOf(name) === -1) {
      QueryDef.shortcuts[name] = func
    }
    else {
      warn(`Default shortcut '${name}' cannot be overwritten`)
    }
  }

  private readonly subqueries: { [key: string]: SubqueryDef } = {}

  // last context
  private context: any

  constructor(private readonly base: QueryArg) {}

  baseQuery(type: string) {
    return typeof this.base === 'function' ? '[Function]' : new Query(this.base).toString(type as any)
  }

  registered() {
    const keys = Object.keys(this.subqueries).sort()

    const table: string[] = []
    const field: string[] = []
    const subquery: string[] = []
    const groupBy: string[] = []
    const orderBy: string[] = []

    for (const key of keys) {
      if (key.startsWith('table:')) {
        table.push(key.substr(6))
      }
      else if (key.startsWith('field:')) {
        field.push(key.substr(6))
      }
      else if (key.startsWith('groupBy:')) {
        groupBy.push(key.substr(8))
      }
      else if (key.startsWith('orderBy:')) {
        orderBy.push(key.substr(8))
      }
      else {
        subquery.push(key)
      }
    }

    return {
      table: table.sort(),
      field: field.sort(),
      subquery: subquery.sort(),
      groupBy: groupBy.sort(),
      orderBy: orderBy.sort()
    }
  }

  private commonFunc<T>(funcName: string, prefix = funcName): (...args: any[]) => SubqueryDef {
    return (...args: any[]) => {
      if (typeof args[0] === 'boolean' || (typeof args[0] !== 'string' && typeof args[1] === 'string')) {
        warn(`QueryDef.${funcName}(overwrite: boolean, ...) is deprecated. use the one without @param overwrite instead`)
        args = args.slice(1)
      }
  
      let name = args[0] as string, arg = args[1] as T
      let prerequisite: Prerequisite = []
      if (args[2]) {
        if (typeof args[2] !== 'string') {
          prerequisite = args[2] as Prerequisite
        }
        else {
          warn(`QueryDef.${funcName}(..., ...companion: string[]) is deprecated. use the one with @param prerequisite instead`)
          prerequisite = args.slice(2) as string[]
        }
      }
      if (!name) throw new Error(`Missing ${funcName} name`)
      if (name.endsWith(':')) throw new Error(`Invalid ${funcName} name: ${name}`)

      if (prefix) name = `${prefix}:${name}`
      // always overwrite
      if (this.subqueries[name]) warn(`${name} overwritten`)
      try{
        return this.subqueries[name] = new SubqueryDef(arg, prerequisite)
      }
      finally {
        log(`${name} registered`)
      }
    }
  }

  subquery(overwrite: boolean, name: string, arg: SubqueryArg, prerequisite?: Prerequisite): SubqueryDef
  subquery(overwrite: boolean, name: string, arg: SubqueryArg, ...companion: string[]): SubqueryDef
  subquery(name: string, arg: SubqueryArg, prerequisite?: Prerequisite): SubqueryDef
  subquery(name: string, arg: SubqueryArg, ...companion: string[]): SubqueryDef
  subquery(...args: any[]): SubqueryDef {
    return this.commonFunc<SubqueryArg>('subquery', '')(...args)
  }

  field(overwrite: boolean, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  field(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  field(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  field(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  field(...args: any[]): QueryDef {
    this.commonFunc<QueryArg>('field')(...args)
    return this
  }

  table(overwrite: boolean, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  table(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  table(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  table(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  table(...args: any[]): QueryDef {
    this.commonFunc<QueryArg>('table')(...args)
    return this
  }

  groupBy(overwrite: boolean, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  groupBy(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  groupBy(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  groupBy(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  groupBy(...args: any[]): QueryDef {
    this.commonFunc<QueryArg>('groupBy')(...args)
    return this
  }

  orderBy(overwrite: boolean, name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  orderBy(overwrite: boolean, name: string, arg: QueryArg, ...companion: string[]): QueryDef
  orderBy(name: string, arg: QueryArg, prerequisite?: Prerequisite): QueryDef
  orderBy(name: string, arg: QueryArg, ...companion: string[]): QueryDef
  orderBy(...args: any[]): QueryDef {
    this.commonFunc<QueryArg>('orderBy')(...args)
    return this
  }

  groupField(overwrite: boolean, name: string, arg: ExpressionArg, prefix: string, prerequisite?: Prerequisite): QueryDef
  groupField(overwrite: boolean, name: string, arg: ExpressionArg, prefix: string, ...companion: string[]): QueryDef
  groupField(name: string, arg: ExpressionArg, prefix: string, prerequisite?: Prerequisite): QueryDef
  groupField(name: string, arg: ExpressionArg, prefix: string, ...companion: string[]): QueryDef
  groupField(...args: any[]): QueryDef {
    if (typeof args[0] === 'boolean'|| (typeof args[0] !== 'string' && typeof args[1] === 'string')) {
      warn('QueryDef.groupField(overwrite: boolean, ...) is deprecated. use the one without @param overwrite instead')
      args = args.slice(1)
    }

    let name = args[0] as string, arg = args[1] as ExpressionArg, prefix = args[2] as string
    let prerequisite: Prerequisite = []
    if (args[3]) {
      if (typeof args[3] !== 'string') {
        prerequisite = args[3] as Prerequisite
      }
      else {
        warn('QueryDef.groupField(..., ...companion: string[]) is deprecated. use the one with @param prerequisite instead')
        prerequisite = args.slice(3) as string[]
      }
    }
    if (!name) throw new Error('Missing group field name')

    async function get(arg: ExpressionArg, params: IQueryParams) {
      return typeof arg === 'function' ? await arg(params) : arg
    }
    function check(name: string, params: IQueryParams) {
      if (params.fields && params.fields.length && params.groupBy && params.groupBy.length) {
        return params.fields.indexOf(name) > -1 && params.groupBy.indexOf(name) > -1
      }
      return false
    }
    this.field(
      name,
      async params => {
        const expr = await get(arg, params)
        let resultColumn: ResultColumn
        if (check(name, params)) {
          resultColumn = new ResultColumn(expr, `${prefix}${name}`)
        } else {
          resultColumn = new ResultColumn(expr, name)
        }
        return { $select: [resultColumn] } as Partial<IQuery>
      },
      prerequisite
    )
    this.groupBy(
      name,
      async params => {
        let groupBy: GroupBy
        if (check(name, params)) {
          groupBy = new GroupBy(`${prefix}${name}`)
        } else {
          groupBy = new GroupBy(await get(arg, params))
        }
        return { $group: groupBy } as Partial<IQuery>
      },
      prerequisite
    )

    return this
  }

  // backward compatible
  register(name: string, arg: IResultColumn | IGroupBy, ...companion: string[])
  register(name: string, arg: QueryArg, ...companion: string[]): SubqueryDef
  register(name: string, arg: QueryArg | IResultColumn | IGroupBy, ...companion: string[]): any {
    if (arg instanceof Query || typeof arg === 'function') {
      warn('QueryDef.register(...) is deprecated. use QueryDef.subquery(...) instead')
      return this.subquery(name, arg, companion)
    }
    else if ('expression' in arg) {
      warn('QueryDef.register(...) is deprecated. use QueryDef.field(...) instead')
      return this.field(name, () => ({ $select: arg }), companion)
    }
    else if ('classname' in arg && arg.classname === 'GroupBy') {
      warn('QueryDef.register(...) is deprecated. use QueryDef.groupBy(...) instead')
      return this.groupBy(name, () => ({ $group: arg as IGroupBy }), companion)
    }
    else {
      warn('QueryDef.register(...) is deprecated. use QueryDef.subquery(...) instead')
      return this.subquery(name, arg as Partial<IQuery>, companion)
    }
  }

  // backward compatible
  registerQuery(name: string, arg: QueryArg, ...companion: string[]): SubqueryDef {
    warn('QueryDef.registerQuery(...) is deprecated. use QueryDef.field(...) instead')
    return this.subquery(name, arg, companion)
  }

  // backward compatible
  registerResultColumn(name: string, arg: ResultColumnArg, ...companion: string[]): QueryDef {
    warn('QueryDef.registerResultColumn(...) is deprecated. use QueryDef.field(...) instead')
    return this.field(name, async params => ({ $select: typeof arg === 'function' ? await arg(params) : arg }), companion)
  }

  // backward compatible
  registerGroupBy(name: string, arg: GroupByArg, ...companion: string[]) {
    warn('QueryDef.registerGroupBy(...) is deprecated. use QueryDef.groupBy(...) instead')
    return this.groupBy(name, async params => ({ $group: typeof arg === 'function' ? await arg(params) : arg }), companion)
  }

  // backward compatible
  registerBoth(overwrite: boolean, name: string, arg: ExpressionArg, ...companion: string[])
  registerBoth(name: string, arg: ExpressionArg, ...companion: string[])
  registerBoth(...args: any[]) {
    warn('QueryDef.registerBoth(...) is deprecated. use QueryDef.groupField(...) instead')
    if (typeof args[0] === 'boolean' || (typeof args[0] !== 'string' && typeof args[1] === 'string')) args = args.slice(1)
    return this.groupField(args[0] as string, args[1] as ExpressionArg, 'group_', args.slice(2))
  }

  async useShortcuts<T extends IBaseShortcut = IBaseShortcut, U = any>(shortcuts: Array<DefaultShortcuts | T>, options?: U): Promise<QueryDef> {
    const regPrerequisites: { [key: string]: Prerequisite } = {}
    const registered: { [key: string]: IExpression } = new Proxy({}, {
      get(target, name) {
        if (!target[name]) throw new Error(`Expression '${String(name)}' not registered`)
        try {
          return target[name]
        }
        finally {
          let left = context.prerequisite
          let right = regPrerequisites[name as string]
          context.prerequisite = left && right ? mergePrerequisite(left, right) : right || left
        }
      }
    })
    const context: IShortcutContext = this.context || (this.context = { registered, regPrerequisites, options })
    
    for (const shortcut of shortcuts) {
      const { name, type } = shortcut
      context.prerequisite = shortcut.prerequisite || shortcut.companions
      if (QueryDef.shortcuts[type]) {
        try {
          await QueryDef.shortcuts[type].bind(this)(shortcut, context)
        }
        catch (e: any) {
          const e2 = new Error(`${e.message}. Fail to register ${type}:${name}`)
          e2.stack += '\n' + e.stack
          throw e2
        }
      }
      else {
        warn(`Invalid shortcut type ${type}`)
      }
    }

    return this
  }

  async apply(params: IQueryParams = {}, options: IOptions = {}): Promise<Query> {
    if (options.withDefault === undefined) options.withDefault = true
    const { withDefault, skipDefFields } = options

    // query params before preparation
    {
      const { conditions, constants, ...params_ } = params
      params_['conditions'] = params_['constants'] = '[Object object]'
      log(`params before: ${JSON.stringify(params_)}`)
    }

    // prepare query params
    params = _.cloneDeep(params)
    if (!params.subqueries) params.subqueries = {}
    if (params.sorting && !Array.isArray(params.sorting)) params.sorting = [params.sorting]

    const allCompanions: string[] = [], depandCount: { [key: string]: number } = {}, subqueries = this.subqueries

    async function register(key: string, registered: string[] = []) {
      if (subqueries[key]) {
        if (allCompanions.indexOf(key) === -1) allCompanions.push(key)
        const count = registered.length
        depandCount[key] = depandCount[key] === undefined ? count : depandCount[key] + count
        registered = [...registered, key]

        const prerequisite = await subqueries[key].applyPrerequisite(params)
        if (Array.isArray(prerequisite)) {
          for (const k of prerequisite) {
            if (registered.indexOf(k) > -1) throw new Error(`Recursive dependency: ${registered.join(' -> ')} -> ${k}`)
            await register(k, registered)
          }
        }
      }
      else if (registered.length) {
        throw new Error(`Companion '${key}' not found`)
      }
    }

    async function checkPrerequisite(attr: string, prefix?: string) {
      let array = params[attr] || []
      if (!Array.isArray(array)) array = Object.keys(array)

      for (const key of array) {
        if (typeof key === 'string') {
          const target = prefix ? `${prefix}:${key}` : key
          await register(target)
        }
      }
    }

    await checkPrerequisite('fields', 'field')
    await checkPrerequisite('tables', 'table')
    await checkPrerequisite('subqueries')
    await checkPrerequisite('groupBy', 'groupBy')
    await checkPrerequisite('sorting', 'orderBy')

    allCompanions.sort((l, r) => {
      const lc = depandCount[l]
      const rc = depandCount[r]
      return lc < rc ? 1 : lc > rc ? -1 : 0
    })

    const companions = allCompanions.reduce<{ field: string[]; table: string[]; subquery: string[]; groupBy: string[]; orderBy: string[] }>((r, k) => {
      const pcs = k.split(':')
      let type = 'subquery'
      if (['field', 'table', 'groupBy', 'orderBy'].indexOf(pcs[0]) > -1) type = pcs[0]
      if (!r[type]) r[type] = []
      r[type].push(pcs[type === 'subquery' ? 0 : 1])
      return r
    }, { field: [], table: [], subquery: [], groupBy: [], orderBy: [] })

    if (!params.fields) params.fields = []
    params.fields = params.fields.reduce<FieldParams[]>((r, f) => {
      if (typeof f !== 'string' || r.indexOf(f) === -1) r.push(f)
      return r
    }, companions.field)

    if (!params.tables) params.tables = []
    params.tables = params.tables.reduce<string[]>((r, t) => {
      if (r.indexOf(t) === -1) r.push(t)
      return r
    }, companions.table)

    if (!params.subqueries) params.subqueries = {}
    for (const s of companions.subquery) {
      if (!params.subqueries[s] && this.subqueries[s]) {
        params.subqueries[s] = this.subqueries[s].default
      }
    }

    if (!params.groupBy) params.groupBy = []
    params.groupBy = companions.groupBy.reduce<GroupByParams[]>((r, g) => {
      if (r.indexOf(g) === -1) r.push(g)
      return r
    }, params.groupBy)

    if (!params.sorting) params.sorting = []
    params.sorting = companions.orderBy.reduce<OrderByParams[]>((r, o) => {
      if (r.indexOf(o) === -1) r.push(o)
      return r
    }, params.sorting as OrderBy[])

    if (withDefault && this.subqueries.default) {
      params.subqueries.default = true
    }

    // prepared query params
    {
      const { conditions, constants, ...params_ } = params
      params_['conditions'] = params_['constants'] = '[Object object]'
      log(`params after: ${JSON.stringify(params_)}`)
    }

    const base: IQuery = dummyQuery(typeof this.base === 'function' ? await this.base(params) : this.base)

    if (params.distinct) {
      base.$distinct = true
    }

    if (params.fields.length) {
      const $select = (base.$select = [] as IResultColumn[])
      for (const f of params.fields) {
        let fields: IResultColumn[] = []

        // string
        if (typeof f === 'string') {
          const key = `field:${f}`
          if (this.subqueries[key]) {
            log(`Apply ${key}`)
            const registered = this.subqueries[key]
            const { $distinct, $select } = await registered.apply(params)
            if ($distinct) base.$distinct = true
            fields = dummyQuery({ $select }).$select
            if (!fields.length) warn(`No result columns for '${key}'`)
          }
          else if (!skipDefFields) {
            fields = [{ expression: new ColumnExpression(f) }]
          }
        }
        // [string, string]
        else if (Array.isArray(f)) {
          fields = [{ expression: new ColumnExpression(f[0], f[1]) }]
        }
        // { column, $as? }
        else if ('column' in f) {
          fields = [{
            expression: new ColumnExpression(f.column[0], f.column[1]),
            $as: f.$as
          }]
        }
        // IResultColumn
        else {
          fields = [f]
        }

        $select.push(...fields.map(f => new ResultColumn(f)))
      }
    }

    if (params.tables.length) {
      for (const t of params.tables) {
        const key = `table:${t}`
        if (this.subqueries[key]) {
          log(`Apply ${key}`)
          const { $from, $where } = await this.subqueries[key].apply(params)
          mergeQuery(base, dummyQuery({ $from, $where }))
        }
      }
    }

    for (const s of Object.keys(params.subqueries)) {
      if (this.subqueries[s]) {
        const subquery = dummyQuery(await this.subqueries[s].apply(s, params))
        mergeQuery(base, subquery)
      }
    }
    if (params.conditions) {
      if (!base.$where) {
        base.$where = params.conditions
      }
      else if ((base.$where as IConditionalExpression).classname !== 'AndExpressions') {
        base.$where = new AndExpressions([base.$where as IConditionalExpression, params.conditions])
      }
      else {
        (base.$where as IGroupedExpressions).expressions.push(params.conditions)
      }
    }

    if (params.groupBy.length) {
      const $group = (base.$group = (base.$group || { expressions: [] }) as IGroupBy)
      const expressions = $group.expressions as IExpression[]
      const $having = $group.$having ? [$group.$having as IConditionalExpression] : []

      function apply({ expressions: e, $having: h }: IGroupBy) {
        expressions.push(...Array.isArray(e) ? e : [e])
        if (h) $having.push(...Array.isArray(h) ? h : [h])
      }

      for (const g of params.groupBy) {
        // string
        if (typeof g === 'string') {
          const key = `groupBy:${g}`
          if (this.subqueries[key]) {
            log(`Apply ${key}`)
            const { $group } = await this.subqueries[key].apply(params)
            const group = dummyQuery({ $group }).$group
            if (group) apply(group)
            else warn(`No group by expressions returned from '${key}'`)
          }
          else {
            expressions.push(new ColumnExpression(g))
          }
        }
        // IGroupBy
        else {
          apply(g)
        }
      }

      base.$group = { expressions, $having }
    }

    if (!base.$order) base.$order = []
    let $order = base.$order
    if (typeof $order === 'string') base.$order = $order = [new OrderBy($order)]
    if (!Array.isArray($order)) base.$order = $order = [$order]

    for (let o of params.sorting) {
      let direction: 'ASC' | 'DESC' = 'ASC'
      if (typeof o !== 'string' && 'key' in o) {
        direction = o.direction || 'ASC'
        o = o.key
      }
      // string
      if (typeof o === 'string') {
        const key = `orderBy:${o}`
        if (this.subqueries[key]) {
          log(`Apply ${key}`)
          const { $order: value } = await this.subqueries[key].apply(params)
          const order = dummyQuery({ $order: value }).$order
          if (order) {
            if (order.length === 1) order[0].order = direction
            $order.push(...order)
          }
          else throw new Error(`No order by expressions returned from '${key}'`)
        }
        else {
          $order.push(new OrderBy(o, direction))
        }
      }
      // IOrderBy
      else {
        $order.push(o)
      }
    }

    if (params.limit) {
      if (typeof params.limit === 'number') params.limit = { $limit: params.limit }
      base.$limit = params.limit
    }

    return new Query(QueryDef.postProcessors.reduce((r, p) => p(r), base))
  }

  clone(): QueryDef {
    const queryDef = new QueryDef(typeof this.base === 'function' ? this.base : new Query(this.base))
    for (const name of Object.keys(this.subqueries)) {
      queryDef.subqueries[name] = this.subqueries[name].clone()
    }
    return queryDef
  }
}

// backward compatible
export function registerShortcut<T extends IBaseShortcut>(name: string, func: ShortcutFunc<T>) {
  warn('registerShortcut(...) is deprecated. use QueryDef.registerShortcut(...) instead')
  QueryDef.registerShortcut(name, func)
}

export { QueryArg, ResultColumnArg, ExpressionArg, GroupByArg, SubqueryArg,  } from './interface'
export { IQueryParams } from './queryParams'
export { CommonFunc, CommonType, IShortcutContext, IBaseShortcut, IQueryArgShortcut, IFieldShortcut, ITableShortcut, ISubqueryShortcut, ISubqueryArgShortcut, IGroupByShortcut, IOrderByShortcut } from './shortcuts'
export { EqualOrInSubqueryArg, IfExpression, IfNullExpression } from './utils'
