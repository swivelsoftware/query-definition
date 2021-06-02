import merge from 'deepmerge'
import { AndExpressions, BetweenExpression, BinaryExpression, CaseExpression, ColumnExpression, ExistsExpression, Expression, FromTable, FunctionExpression, IConditionalExpression, IExpression, IFromTable, IGroupBy, InExpression, IOrderBy, IQuery, IResultColumn, IsNullExpression, LikeExpression, MathExpression, OrExpressions, ParameterExpression, Query, QueryExpression, RegexpExpression, Unknown, Value } from 'node-jql'
import { Prerequisite, SubqueryArg } from './interface'
import { IQueryParams, OrderByParams } from './queryParams'

export function EqualOrInSubqueryArg(leftExpression: IExpression): SubqueryArg {
  return ({ value }) => {
    let expression: IExpression
    if (Array.isArray(value)) {
      expression = new InExpression(leftExpression, false, new Value(value))
    }
    else {
      expression = new BinaryExpression(leftExpression, '=', new Value(value))
    }
    return {
      $where: [expression]
    }
  }
}

export function IfExpression(condition: IExpression, whenTrue: IExpression, whenFalse: IExpression = new Value(null)): FunctionExpression {
  return new FunctionExpression('IF', condition, whenTrue, whenFalse)
}

export function IfNullExpression(value: IExpression, elseValue: IExpression): FunctionExpression {
  return new FunctionExpression('IFNULL', value, elseValue)
}

export function getUnknowns(arg: Query | FromTable | Expression): Unknown[] {
  const result: Unknown[] = []

  if (arg instanceof Query) {
    for (const resultColumn of arg.$select) {
      result.push(...getUnknowns(resultColumn.expression))
    }
    if (arg.$from) {
      for (const tableOrSubquery of arg.$from) {
        result.push(...getUnknowns(tableOrSubquery))
      }
    }
    if (arg.$where) {
      result.push(...getUnknowns(arg.$where))
    }
    if (arg.$group) {
      for (const expression of arg.$group.expressions) {
        result.push(...getUnknowns(expression))
      }
      if (arg.$group.$having) {
        result.push(...getUnknowns(arg.$group.$having))
      }
    }
    if (arg.$order) {
      for (const orderingTerm of arg.$order) {
        result.push(...getUnknowns(orderingTerm.expression))
      }
    }
  }
  else if (arg instanceof FromTable) {
    for (const joinClause of arg.joinClauses) {
      if (joinClause.$on) {
        result.push(...getUnknowns(joinClause.$on))
      }
    }
  }
  else if (arg instanceof AndExpressions || arg instanceof OrExpressions) {
    for (const expression of arg.expressions) {
      result.push(...getUnknowns(expression))
    }
  }
  else if (arg instanceof BetweenExpression) {
    result.push(...getUnknowns(arg.left))
    result.push(...getUnknowns(arg.start))
    result.push(...getUnknowns(arg.end))
  }
  else if (
    arg instanceof BinaryExpression ||
    arg instanceof InExpression ||
    arg instanceof LikeExpression ||
    arg instanceof MathExpression ||
    arg instanceof RegexpExpression
  ) {
    result.push(...getUnknowns(arg.left))
    result.push(...getUnknowns(arg.right))
  }
  else if (arg instanceof CaseExpression) {
    for (const { $when, $then } of arg.cases) {
      result.push(...getUnknowns($when))
      result.push(...getUnknowns($then))
    }
    if (arg.$else) {
      result.push(...getUnknowns(arg.$else))
    }
  }
  else if (arg instanceof ExistsExpression) {
    result.push(...getUnknowns(arg.query))
  }
  else if (arg instanceof FunctionExpression) {
    for (const parameter of arg.parameters) {
      result.push(...getUnknowns(parameter))
    }
  }
  else if (arg instanceof IsNullExpression) {
    result.push(...getUnknowns(arg.left))
  }
  else if (arg instanceof ParameterExpression) {
    result.push(...getUnknowns(arg.expression))
  }
  else if (arg instanceof QueryExpression) {
    result.push(...getUnknowns(arg.query))
  }
  else if (arg instanceof Unknown) {
    result.push(arg)
  }

  return result
}

export function dummyQuery(json: Partial<IQuery>) {
  const query = new Query(json)
  query.$select = query.$select.filter(c => {
    if (c.expression instanceof ColumnExpression) {
      return !(c.expression.name === '*' && !c.expression.table)
    }
    return true
  })
  return query
}

export function mergeQuery(base: Partial<IQuery>, subquery: Partial<IQuery>) {
  if (subquery.$distinct) {
    base.$distinct = true
  }

  if (subquery.$select && (!Array.isArray(subquery.$select) || subquery.$select.length)) {
    function normalize($select?: string | IResultColumn | IResultColumn[]): IResultColumn[] {
      if (!$select) $select = []
      if (typeof $select === 'string') $select = { expression: new ColumnExpression($select) }
      if (!Array.isArray($select)) $select = [$select]
      return $select
    }
    base.$select = [...normalize(base.$select), ...normalize(subquery.$select)]
  }

  if (subquery.$from) {
    function normalize(table: string | IFromTable | IFromTable[]): IFromTable[] {
      if (typeof table === 'string') table = { table }
      if (!Array.isArray(table)) table = [table]
      return table
    }
  
    base.$from = normalize(base.$from || [])
    subquery.$from = normalize(subquery.$from)
  
    if (!base.$from.length) base.$from = subquery.$from
    else if (subquery.$from.length) {
      for (const t of subquery.$from) {
        const exists = base.$from.find(({ table, $as }) => (typeof table === 'object' ? $as : table) === t.table)
    
        // new table
        if (!exists) base.$from.push(t)
        // new JOIN
        else if (t.joinClauses) {
          exists.joinClauses = exists.joinClauses || []
          if (!Array.isArray(t.joinClauses)) t.joinClauses = [t.joinClauses]
          if (!Array.isArray(exists.joinClauses)) exists.joinClauses = [exists.joinClauses]
          exists.joinClauses.push(...t.joinClauses)
        }
      }
    }
  }

  if (subquery.$where) {
    function normalize($where?: IConditionalExpression | IConditionalExpression[]): IConditionalExpression[] {
      if (!$where) $where = []
      if (!Array.isArray($where)) $where = [$where]
      return $where
    }
    base.$where = new AndExpressions([...normalize(base.$where), ...normalize(subquery.$where)])
  }

  if (subquery.$group) {
    function normalize($group?: string | IGroupBy): IGroupBy {
      if (!$group) $group = { expressions: [] }
      if (typeof $group === 'string') $group = { expressions: [new ColumnExpression($group)] }
      if (!Array.isArray($group.expressions)) $group.expressions = [$group.expressions]
      if (!Array.isArray($group.$having)) $group.$having = $group.$having ? [$group.$having] : []
      return $group
    }

    base.$group = normalize(base.$group)
    subquery.$group = normalize(subquery.$group)

    const expressions = base.$group.expressions as IExpression[]
    expressions.push(...(subquery.$group.expressions as IExpression[]))
    if (!(base.$group.$having as IConditionalExpression[]).length && !(subquery.$group.$having as IConditionalExpression[]).length) {
      delete base.$group.$having
    }
    else {
      const $having = base.$group.$having as IConditionalExpression[]
      $having.push(...(subquery.$group.$having as IConditionalExpression[]))
    }
  }

  if (subquery.$order) {
    function normalize(order?: string | IOrderBy | IOrderBy[]): IOrderBy[] {
      if (!order) order = []
      if (typeof order === 'string') order = { expression: new ColumnExpression(order) }
      if (!Array.isArray(order)) order = [order]
      return order
    }
    base.$order = [...normalize(base.$order), ...normalize(subquery.$order)]
  }

  if (subquery.$limit && !base.$limit) {
    base.$limit = subquery.$limit
  }
}

export function mergePrerequisite(left: Prerequisite, right: Prerequisite): Prerequisite {
  // array vs array
  if (Array.isArray(left) && Array.isArray(right)) {
    return [...left, ...right]
  }
  // object vs object
  else if ((typeof left === 'object' && !Array.isArray(left)) && (typeof right === 'object' && !Array.isArray(right))) {
    return merge(left, right)
  }
  // function vs function
  else if (typeof left === 'function' && typeof right === 'function') {
    return async(params: IQueryParams) => {
      const left_ = await left(params)
      const right_ = await right(params)
      return mergePrerequisite(left_, right_) as string[] | IQueryParams
    }
  }
  // function vs array
  else if (typeof left === 'function' && Array.isArray(right)) {
    return mergePrerequisite(left, () => right)
  }
  // function vs object
  else if (typeof left === 'function' && (typeof right === 'object' && !Array.isArray(right))) {
    return mergePrerequisite(left, () => right)
  }
  // array vs object
  else if (Array.isArray(left) && (typeof right === 'object' && !Array.isArray(right))) {
    return mergePrerequisite(left.reduce<IQueryParams>((r, k) => {
      const pcs = k.split(':')
      let type = 'subquery'
      if (['field', 'table', 'groupBy', 'orderBy'].indexOf(pcs[0]) > -1) type = pcs[0]
      switch (type) {
        case 'field': {
          if (!r.fields) r.fields = []
          r.fields.push(pcs[1])
          break
        }
        case 'table': {
          if (!r.tables) r.tables = []
          r.tables.push(pcs[1])
          break
        }
        case 'subquery': {
          if (!r.subqueries) r.subqueries = {}
          r.subqueries[pcs[0]] = true
          break
        }
        case 'groupBy': {
          if (!r.groupBy) r.groupBy = []
          r.groupBy.push(pcs[1])
          break
        }
        case 'orderBy': {
          if (!r.sorting) r.sorting = [] as OrderByParams[]
          (r.sorting as OrderByParams[]).push(pcs[1])
          break
        }
      }
      return r
    }, {}), right)
  }
  // revert
  else {
    return mergePrerequisite(right, left)
  }
}