import debug = require('debug')
import { AndExpressions, BetweenExpression, BinaryExpression, CaseExpression, ColumnExpression, FunctionExpression, GroupBy, IConditionalExpression, IExpression, IFromTable, InExpression, IsNullExpression, LikeExpression, OrderBy, OrExpressions, ParameterExpression, RegexpExpression, ResultColumn, Value } from '@swivel-admin/node-jql'
import { IfExpression, IfNullExpression, QueryDef } from '.'
import { Prerequisite, QueryArg, SubqueryArg } from './interface'
import { IQueryParams } from './queryParams'

const log = debug('QueryDef:log')
const warn = debug('QueryDef:warn')

export type CommonFunc<T> = (registered: { [key: string]: IExpression }) => T | Promise<T>
export type CommonType<T> = T | CommonFunc<T>

type UnknownType = boolean | { noOfUnknowns?: number; fromTo?: boolean } | Array<[string, number]>

export interface IShortcutContext {
  prerequisite?: Prerequisite
  registered: { [key: string]: IExpression }
  regPrerequisites: { [key: string]: Prerequisite }
}

export type ShortcutFunc<T extends IBaseShortcut, U = any, R = any> =
  (this: QueryDef, shortcut: T, context: IShortcutContext & U, options?: R) => Promise<void>

export interface IBaseShortcut {
  type: string
  name: string
  prerequisite?: Prerequisite

  // backward compatible
  companions?: string[]|((params: IQueryParams) => string[] | Promise<string[]>)
}

export interface IQueryArgShortcut extends IBaseShortcut {
  type: 'field'|'table'|'groupBy'|'orderBy'
  queryArg: CommonFunc<QueryArg>
}

export interface IFieldShortcut extends IBaseShortcut {
  type: 'field'
  expression: CommonType<IExpression>
  registered?: boolean
}

export interface ITableShortcut extends IBaseShortcut {
  type: 'table'
  fromTable: CommonType<IFromTable>
}

export interface ISubqueryShortcut extends IBaseShortcut {
  type: 'subquery'
  expression: CommonType<IExpression>
  unknowns?: UnknownType
}

export interface ISubqueryArgShortcut extends IBaseShortcut {
  type: 'subquery'
  subqueryArg: CommonFunc<SubqueryArg>
  unknowns?: UnknownType
}

export interface IGroupByShortcut extends IBaseShortcut {
  type: 'groupBy'
  expression: CommonType<IExpression>
}

export interface IOrderByShortcut extends IBaseShortcut {
  type: 'orderBy'
  expression: CommonType<IExpression>
  direction?: 'ASC'|'DESC'
}

export interface IConditionsContext extends IShortcutContext {
  prefixConditions: IConditionsShortcut[]
  suffixConditions: IConditionsShortcut[]
  summaryMetrics: Array<ISummaryMetricShortcut | ISummaryMetricArgShortcut>
}

export interface IDateSourceShortcut extends ISubqueryShortcut {
  exprArg: CommonFunc<(params: IQueryParams) => IExpression>
}

export interface IComboShortcut extends IBaseShortcut {
  type: 'combo'
  expression: CommonType<IExpression>
  registered?: true
}

export interface IComboArgShortcut extends IBaseShortcut {
  type: 'combo'
  exprArg: CommonFunc<(params: IQueryParams) => IExpression>
}

interface IConditionsCase {
  value: string
  expression: CommonType<IConditionalExpression>
  prerequisite?: Prerequisite
}

interface IConditionsArgCase {
  value: string
  exprArg: CommonFunc<(params: IQueryParams) => IConditionalExpression>
  prerequisite?: Prerequisite
}

export interface IConditionsShortcut extends IBaseShortcut {
  type: 'conditions'
  cases: Array<IConditionsCase | IConditionsArgCase>
}

export interface ISummaryMetricShortcut extends IBaseShortcut {
  type: 'summaryMetric'
  summaryType?: 'count' | 'sum'
  expression: CommonType<IExpression>
  registered?: boolean
}

export interface ISummaryMetricArgShortcut extends IBaseShortcut {
  type: 'summaryMetric'
  summaryType?: 'count' | 'sum'
  exprArg: CommonFunc<(params: IQueryParams) => IExpression>
}

export type DefaultShortcuts = IQueryArgShortcut | IFieldShortcut | ITableShortcut | ISubqueryShortcut | ISubqueryArgShortcut | IGroupByShortcut | IOrderByShortcut | IComboShortcut | IComboArgShortcut | IConditionsShortcut | ISummaryMetricShortcut | ISummaryMetricArgShortcut

export const FieldShortcutFunc: ShortcutFunc<IFieldShortcut | IQueryArgShortcut> = async function(this: QueryDef, shortcut: IFieldShortcut | IQueryArgShortcut, ctx: IShortcutContext) {
  const { name } = shortcut

  let queryArg: QueryArg | undefined
  if ('expression' in shortcut) {
    const expression = typeof shortcut.expression === 'function' ? await shortcut.expression(ctx.registered) : shortcut.expression
    if ('registered' in shortcut && shortcut.registered) {
      log(`field:${name} registered`)
      ctx.registered[name] = expression
      if (ctx.prerequisite) ctx.regPrerequisites[name] = ctx.prerequisite
    }
    queryArg = { $select: new ResultColumn(expression, name) }
  }
  else if ('queryArg' in shortcut) {
    queryArg = await shortcut.queryArg(ctx.registered)
  }

  if (queryArg) {
    this.field(name, queryArg, ctx.prerequisite)
  }
  else {
    warn(`Invalid field:${name}`)
  }
}

export const TableShortcutFunc: ShortcutFunc<ITableShortcut | IQueryArgShortcut> = async function(this: QueryDef, shortcut: ITableShortcut | IQueryArgShortcut, ctx: IShortcutContext) {
  const { name } = shortcut

  let queryArg: QueryArg | undefined
  if ('fromTable' in shortcut) {
    queryArg = { $from: typeof shortcut.fromTable === 'function' ? await shortcut.fromTable(ctx.registered) : shortcut.fromTable }
  }
  else if ('queryArg' in shortcut) {
    queryArg = await shortcut.queryArg(ctx.registered)
  }

  if (queryArg) {
    this.table(name, queryArg, ctx.prerequisite)
  }
  else {
    warn(`Invalid table:${name}`)
  }
}

export const SubqueryShortcutFunc: ShortcutFunc<ISubqueryShortcut | ISubqueryArgShortcut> = async function(this: QueryDef, shortcut: ISubqueryShortcut | ISubqueryArgShortcut, ctx: IShortcutContext) {
  const { name } = shortcut

  let subqueryArg: SubqueryArg | undefined
  if ('expression' in shortcut) {
    subqueryArg = { $where: typeof shortcut.expression === 'function' ? await shortcut.expression(ctx.registered) : shortcut.expression }
  }
  else if ('subqueryArg' in shortcut) {
    subqueryArg = await shortcut.subqueryArg(ctx.registered)
  }

  if (subqueryArg) {
    const subqueryDef = this.subquery(name, subqueryArg, ctx.prerequisite)

    if ('unknowns' in shortcut) {
      if (Array.isArray(shortcut.unknowns)) {
        for (const [name, index] of shortcut.unknowns) {
          subqueryDef.register(name, index)
        }
      }
      else if (shortcut.unknowns && typeof shortcut.unknowns !== 'boolean' && shortcut.unknowns.fromTo) {
        const noOfUnknowns = shortcut.unknowns.noOfUnknowns || 2
        for (let i = 0, length = noOfUnknowns; i < length; i += 2) {
          subqueryDef.register('from', i)
          subqueryDef.register('to', i + 1)
        }
      }
      else if (shortcut.unknowns) {
        const noOfUnknowns = typeof shortcut.unknowns !== 'boolean' && shortcut.unknowns.noOfUnknowns || 1
        for (let i = 0, length = noOfUnknowns; i < length; i += 1) {
          subqueryDef.register('value', i)
        }
      }
    }
  }
  else {
    warn(`Invalid subquery:${name}`)
  }
}

export const GroupByShortcutFunc: ShortcutFunc<IGroupByShortcut | IQueryArgShortcut> = async function(this: QueryDef, shortcut: IGroupByShortcut | IQueryArgShortcut, ctx: IShortcutContext) {
  const { name } = shortcut

  let queryArg: QueryArg | undefined
  if ('expression' in shortcut) {
    queryArg = { $group: new GroupBy([typeof shortcut.expression === 'function' ? await shortcut.expression(ctx.registered) : shortcut.expression]) }
  }
  else if ('queryArg' in shortcut) {
    queryArg = await shortcut.queryArg(ctx.registered)
  }

  if (queryArg) {
    this.groupBy(name, queryArg, ctx.prerequisite)
  }
  else {
    warn(`Invalid groupBy:${name}`)
  }
}

export const OrderByShortcutFunc: ShortcutFunc<IOrderByShortcut | IQueryArgShortcut> = async function(this: QueryDef, shortcut: IOrderByShortcut | IQueryArgShortcut, ctx: IShortcutContext) {
  const { name } = shortcut

  let queryArg: QueryArg | undefined
  if ('expression' in shortcut) {
    const direction: 'ASC'|'DESC' = shortcut['direction'] || 'ASC'
    queryArg = { $order: new OrderBy(typeof shortcut.expression === 'function' ? await shortcut.expression(ctx.registered) : shortcut.expression, direction) }
  }
  else if ('queryArg' in shortcut) {
    queryArg = await shortcut.queryArg(ctx.registered)
  }
  if (queryArg) {
    this.orderBy(name, queryArg, ctx.prerequisite)
  }
  else {
    warn(`Invalid orderBy:${name}`)
  }
}

export const dateSources: string[] = []

function shipmentDateExpressionFn(subqueriesOrEntityType: any | string) {
  let entityType: string
  if (typeof subqueriesOrEntityType === 'string') {
    entityType = subqueriesOrEntityType
  }
  else {
    entityType = subqueriesOrEntityType.entityType && subqueriesOrEntityType.entityType.value
  }

  return IfNullExpression(
    new CaseExpression(
      [
        {
          $when: new BinaryExpression(new ColumnExpression(entityType, 'boundTypeCode'), '=', new Value('O')),
          $then: IfNullExpression(
            new ColumnExpression(`${entityType}_date`, 'departureDateActual'),
            IfNullExpression(
              new ColumnExpression(`${entityType}`, 'departureDateEstimated'),
              new ColumnExpression(`${entityType}_date`, 'departureDateEstimated')
            )
          )
        },
        {
          $when: new BinaryExpression(new ColumnExpression(entityType, 'boundTypeCode'), '=', new Value('I')),
          $then: IfNullExpression(
            new ColumnExpression(`${entityType}_date`, 'arrivalDateActual'),
            IfNullExpression(
              new ColumnExpression(`${entityType}`, 'arrivalDateEstimated'),
              new ColumnExpression(`${entityType}_date`, 'arrivalDateEstimated')
            )
          )
        }
      ],
      new ColumnExpression(entityType, entityType === 'shipment' ? 'jobDate' : 'createdAt')
    ),
    new ColumnExpression(entityType, entityType === 'shipment' ? 'jobDate' : 'createdAt')
  )
}

function parseDateSource(params: IQueryParams, expression: IExpression, dateExpressions: any = {}) {
  const entityType = params.subqueries && params.subqueries.entityType && params.subqueries.entityType.value
  if (!entityType) throw new Error('Missing entity type')
  const dateSource = params.subqueries && params.subqueries.dateSource && params.subqueries.dateSource.value
  switch (dateSource) {
    case 'shipmentDate': {
      if (entityType === 'shipment') expression = dateExpressions.shipmentDate || shipmentDateExpressionFn(entityType)
      break
    }
    case 'departureDateEstimated':
    case 'arrivalDateEstimated': {
      expression = dateExpressions[dateSource] || IfNullExpression(new ColumnExpression(`${entityType}_date`, dateSource), expression)
      break
    }
    default: {
      expression = dateExpressions[dateSource]
      break
    }
  }
  return expression
}

export const DateSourceShortcutFunc: ShortcutFunc<IDateSourceShortcut> = async function(this: QueryDef, shortcut: IDateSourceShortcut, ctx: IShortcutContext) {
  const { name, prerequisite } = shortcut
  const expression = 'expression' in shortcut
    ? typeof shortcut.expression === 'function'
      ? await shortcut.expression(ctx.registered)
      : shortcut.expression
    : 'exprArg' in shortcut
      ? await (shortcut as IDateSourceShortcut).exprArg(ctx.registered)
      : new Value(null)

  this.field(true, name, params => ({ $select: new ResultColumn(typeof expression === 'function' ? expression(params) : expression, name) }), prerequisite)

  this.subquery(true, name, ({ from, to }, params = {}) => {
    const subqueries = params.subqueries || {}
    if (subqueries.dateSource && subqueries.dateSource.value !== name) throw new Error('MULTIPLE_DATE_TYPES')
    return {
      $where: new OrExpressions([
        new OrExpressions([
          new IsNullExpression(new Value(from), false),
          new IsNullExpression(new Value(to), false)
        ]),
        new BetweenExpression(typeof expression === 'function' ? expression(params) : expression, false, new Value(from), new Value(to))
      ])
    }
  }, prerequisite)

  this.subquery(true, `${name}Before`, ({ from, to }, params = {}) => {
    const subqueries = params.subqueries || {}
    if (subqueries.dateSource && subqueries.dateSource.value !== name) throw new Error('MULTIPLE_DATE_TYPES')
    return {
      $where: new OrExpressions([
        new OrExpressions([
          new IsNullExpression(new Value(to), false)
        ]),
        new BinaryExpression(typeof expression === 'function' ? expression(params) : expression, '<=', new Value(to))
      ])
    }
  }, prerequisite)

  // auto registered
  if (typeof expression !== 'function') ctx.registered[name] = expression
  if (dateSources.indexOf(name) === -1) dateSources.push(name)
}

export const ComboShortcutFunc: ShortcutFunc<IComboShortcut | IComboArgShortcut> = async function(this: QueryDef, { name, prerequisite, ...shortcut }: IComboShortcut | IComboArgShortcut, ctx: IShortcutContext) {
  const regFlag = 'registered' in shortcut && shortcut.registered
  const expression = 'expression' in shortcut
    ? typeof shortcut.expression === 'function'
      ? await shortcut.expression(ctx.registered)
      : shortcut.expression
    : 'exprArg' in shortcut
      ? await shortcut.exprArg(ctx.registered)
      : null
  if (!expression) throw new Error(`Fail to register shortcut '${name}'`)

  if (typeof expression !== 'function' && regFlag) {
    ctx.registered[name] = expression
    if (ctx.prerequisite && typeof ctx.prerequisite !== 'function') ctx.regPrerequisites[name] = ctx.prerequisite
  }

  this.groupField(true, name, params => typeof expression === 'function' ? expression(params) : expression, 'group_', prerequisite)

  this.groupField(true, `${name}Any`, params => new FunctionExpression('ANY_VALUE', typeof expression === 'function' ? expression(params) : expression), 'group_', prerequisite)

  this.subquery(true, name, ({ value, from, to, operator, multiple = [] }, params = {}) => {
    const expr = typeof expression === 'function' ? expression(params) : expression
    if (!multiple.length) multiple = [{ value, from, to, operator }]
    return {
      $where: multiple.map(({ value, from, to, operator = from && to ? 'between' : Array.isArray(value) ? 'in' : !value ? 'is null' : '=' }) => {
        const NOT = operator.startsWith('not ') || operator.indexOf(' not ') > -1
        switch (operator.toLocaleLowerCase()) {
          case '=':
          case '<>':
          case '>':
          case '>=':
          case '<':
          case '<=':
            return new BinaryExpression(expr, operator, new Value(value))
          case 'between':
          case 'not between':
            return new BetweenExpression(expr, NOT, new Value(from), new Value(to))
          case 'in':
          case 'not in':
            return new InExpression(expr, NOT, new Value(value))
          case 'is null':
          case 'is not null':
            return new IsNullExpression(expr, NOT)
          case 'regexp':
          case 'not regexp':
            return new RegexpExpression(expr, NOT, new RegExp(value, 'i'))
          case 'like':
          case 'not like':
            return new LikeExpression(expr, NOT, new Value(`%${value}%`))
          case 'start with':
          case 'not start with':
            return new LikeExpression(expr, NOT, new Value(`%${value}`))
          case 'end with':
          case 'not end with':
            return new LikeExpression(expr, NOT, new Value(`${value}%`))
        }
      })
    }
  }, prerequisite)
}

function initializeContext(ctx: IConditionsContext) {
  if (!ctx.prefixConditions) {
    ctx.prefixConditions = []
  }

  if (!ctx.suffixConditions) {
    ctx.suffixConditions = [
      {
        type: 'conditions',
        name: 'Month',
        cases: [
          {
            value: 'January',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(1))
          },
          {
            value: 'February',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(2))
          },
          {
            value: 'March',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(3))
          },
          {
            value: 'April',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(4))
          },
          {
            value: 'May',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(5))
          },
          {
            value: 'June',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(6))
          },
          {
            value: 'July',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(7))
          },
          {
            value: 'August',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(8))
          },
          {
            value: 'September',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(9))
          },
          {
            value: 'October',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(10))
          },
          {
            value: 'November',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(11))
          },
          {
            value: 'December',
            exprArg: re => params => new BinaryExpression(new FunctionExpression('Month', parseDateSource(params, re['date'], re)), '=', new Value(12))
          }
        ]
      },
      {
        type: 'conditions',
        name: 'Division',
        cases: [
          {
            value: 'AE',
            exprArg: re => () => new AndExpressions([
              new BinaryExpression(re['moduleTypeCode'], '=', new Value('AIR')),
              new BinaryExpression(re['boundTypeCode'], '=', new Value('O'))
            ])
          },
          {
            value: 'AI',
            exprArg: re => () => new AndExpressions([
              new BinaryExpression(re['moduleTypeCode'], '=', new Value('AIR')),
              new BinaryExpression(re['boundTypeCode'], '=', new Value('I'))
            ])
          },
          {
            value: 'SE',
            exprArg: re => () => new AndExpressions([
              new BinaryExpression(re['moduleTypeCode'], '=', new Value('SEA')),
              new BinaryExpression(re['boundTypeCode'], '=', new Value('O'))
            ])
          },
          {
            value: 'SI',
            exprArg: re => () => new AndExpressions([
              new BinaryExpression(re['moduleTypeCode'], '=', new Value('SEA')),
              new BinaryExpression(re['boundTypeCode'], '=', new Value('I'))
            ])
          }
        ]
      }
    ]
  }

  if (!ctx.summaryMetrics) {
    ctx.summaryMetrics = []
  }
}

export const ConditionsShortcutFunc: ShortcutFunc<IConditionsShortcut> = async function(this: QueryDef, prefix: IConditionsShortcut, ctx: IConditionsContext) {
  initializeContext(ctx)

  // in case registered after summary metric
  for (const summaryMetric of ctx.summaryMetrics) {
    for (const suffix of ctx.suffixConditions) {
      await registerSummaryMetricWithConditions.bind(this)([prefix, suffix], summaryMetric, ctx)
    }
  }
  ctx.prefixConditions.push(prefix)
}

async function registerSummaryMetricWithConditions(this: QueryDef, [prefix, suffix = prefix]: [IConditionsShortcut, IConditionsShortcut?], shortcut: ISummaryMetricShortcut | ISummaryMetricArgShortcut, ctx: IShortcutContext) {
  const expression = 'expression' in shortcut
    ? typeof shortcut.expression === 'function'
      ? await shortcut.expression(ctx.registered)
      : shortcut.expression
    : 'exprArg' in shortcut
      ? await shortcut.exprArg(ctx.registered)
      : new Value(null)

  if (prefix === suffix) {
    this.field(true, `${shortcut.name}${suffix.name}`, async params => {
      const resultColumnList = [] as ResultColumn[]

      // by conditions
      for (const item of prefix.cases) {
        const expr = 'expression' in item
          ? typeof item.expression === 'function'
            ? await item.expression(ctx.registered)
            : item.expression
          : 'exprArg' in item
            ? await item.exprArg(ctx.registered)
            : new Value(null)
        const summaryExpression = new FunctionExpression(
          shortcut.summaryType === 'count' ? 'COUNT' : 'SUM',
          new ParameterExpression(shortcut.summaryType === 'count' ? 'DISTINCT' : '',
            IfExpression(
              typeof expr === 'function' ? expr(params) : expr,
              typeof expression === 'function' ? expression(params) : expression
            )
          )
        )
        resultColumnList.push(new ResultColumn(summaryExpression, `${item.value}_${shortcut.name}`))
      }

      // total
      const summaryExpression = new FunctionExpression(
        shortcut.summaryType === 'count' ? 'COUNT' : 'SUM',
        new ParameterExpression(shortcut.summaryType === 'count' ? 'DISTINCT' : '',
          typeof expression === 'function' ? expression(params) : expression
        )
      )
      resultColumnList.push(new ResultColumn(summaryExpression, `total_${shortcut.name}`))

      return { $select: resultColumnList }
    }, shortcut.prerequisite)
  }
  else {
    this.field(true, `${prefix.name}_${shortcut.name}${suffix.name}`, async params => {
      const resultColumnList = [] as ResultColumn[]

      for (const pItem of prefix.cases) {
        const pExpr = 'expression' in pItem
          ? typeof pItem.expression === 'function'
            ? await pItem.expression(ctx.registered)
            : pItem.expression
          : 'exprArg' in pItem
            ? await pItem.exprArg(ctx.registered)
            : new Value(null)

        // by conditions
        for (const sItem of suffix.cases) {
          const sExpr = 'expression' in sItem
            ? typeof sItem.expression === 'function'
              ? await sItem.expression(ctx.registered)
              : sItem.expression
            : 'exprArg' in sItem
              ? await sItem.exprArg(ctx.registered)
              : new Value(null)
          const expr = new AndExpressions([
            typeof pExpr === 'function' ? pExpr(params) : pExpr,
            typeof sExpr === 'function' ? sExpr(params) : sExpr
          ])

          const summaryExpression = new FunctionExpression(
            shortcut.summaryType === 'count' ? 'COUNT' : 'SUM',
            new ParameterExpression(shortcut.summaryType === 'count' ? 'DISTINCT' : '',
              IfExpression(expr, typeof expression === 'function' ? expression(params) : expression)
            )
          )
          resultColumnList.push(new ResultColumn(summaryExpression, `${sItem.value}_${pItem.value}_${shortcut.name}`))
        }

        // total
        const summaryExpression = new FunctionExpression(
          shortcut.summaryType === 'count' ? 'COUNT' : 'SUM',
          new ParameterExpression(shortcut.summaryType === 'count' ? 'DISTINCT' : '',
            IfExpression(
              typeof pExpr === 'function' ? pExpr(params) : pExpr,
              typeof expression === 'function' ? expression(params) : expression
            )
          )
        )
        resultColumnList.push(new ResultColumn(summaryExpression, `total_${pItem.value}_${shortcut.name}`))
      }

      return { $select: resultColumnList }
    })
  }
}

export const SummaryMetricShortcutFunc: ShortcutFunc<ISummaryMetricArgShortcut | ISummaryMetricShortcut> = async function(this: QueryDef, shortcut: ISummaryMetricShortcut | ISummaryMetricArgShortcut, ctx: IConditionsContext) {
  initializeContext(ctx)

  const expression = 'expression' in shortcut
    ? typeof shortcut.expression === 'function'
      ? await shortcut.expression(ctx.registered)
      : shortcut.expression
    : 'exprArg' in shortcut
      ? await shortcut.exprArg(ctx.registered)
      : new Value(null)

  if (typeof expression !== 'function' && 'registered' in shortcut && shortcut.registered) {
    ctx.registered[shortcut.name] = expression
  }

  this.field(shortcut.name, params => {
    return {
      $select: new ResultColumn(
        new FunctionExpression(
          shortcut.summaryType === 'count' ? 'COUNT' : 'SUM',
          new ParameterExpression(shortcut.summaryType === 'count' ? 'DISTINCT' : '',
            typeof expression === 'function' ? expression(params) : expression
          )
        ),
        shortcut.name
      )
    }
  }, shortcut.prerequisite)

  // suffix conditions
  for (const suffix of ctx.suffixConditions) {
    registerSummaryMetricWithConditions.bind(this)([suffix], shortcut, ctx)

    // in case registered after conditions
    for (const prefix of ctx.prefixConditions) {
      registerSummaryMetricWithConditions.bind(this)([prefix, suffix], shortcut, ctx)
    }
  }

  ctx.summaryMetrics.push(shortcut)
}
