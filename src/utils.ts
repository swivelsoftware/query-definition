import {
  IQuery,
  IUnknown,
  Query,
  FromTable,
  Expression,
  Unknown,
  AndExpressions,
  OrExpressions,
  BetweenExpression,
  BinaryExpression,
  InExpression,
  LikeExpression,
  MathExpression,
  RegexpExpression,
  CaseExpression,
  ExistsExpression,
  FunctionExpression,
  IsNullExpression,
  ParameterExpression,
  IExpression,
  IFromTable,
  IBetweenExpression,
  IBinaryExpression,
  ICaseExpression,
  IExistsExpression,
  IFunctionExpression,
  IGroupedExpressions,
  IInExpression,
  IIsNullExpression,
  IParameterExpression,
  IRegexpExpression,
  IValue,
  QueryExpression,
  ColumnExpression,
  IResultColumn,
  IConditionalExpression,
  IOrderBy,
  IGroupBy,
  ILikeExpression,
  IMathExpression
} from 'node-jql'
import { types } from 'util'

// register unknowns
export function findUnknowns(arg: Query | FromTable | Expression): Unknown[] {
  const result: Unknown[] = []

  if (arg instanceof Query) {
    for (const resultColumn of arg.$select) {
      result.push(...findUnknowns(resultColumn.expression))
    }

    if (arg.$from) {
      for (const tableOrSubquery of arg.$from) {
        result.push(...findUnknowns(tableOrSubquery))
      }
    }

    if (arg.$where) {
      result.push(...findUnknowns(arg.$where))
    }

    if (arg.$group) {
      for (const expression of arg.$group.expressions) {
        result.push(...findUnknowns(expression))
      }
      if (arg.$group.$having) {
        result.push(...findUnknowns(arg.$group.$having))
      }
    }

    if (arg.$order) {
      for (const orderingTerm of arg.$order) {
        result.push(...findUnknowns(orderingTerm.expression))
      }
    }

    if (arg.$union) {
      result.push(...findUnknowns(arg.$union))
    }
  } else if (arg instanceof FromTable) {
    for (const joinClause of arg.joinClauses) {
      if (joinClause.$on) {
        result.push(...findUnknowns(joinClause.$on))
      }
    }
  } else if (arg instanceof AndExpressions || arg instanceof OrExpressions) {
    for (const expression of arg.expressions) {
      result.push(...findUnknowns(expression))
    }
  } else if (arg instanceof BetweenExpression) {
    result.push(...findUnknowns(arg.left))
    result.push(...findUnknowns(arg.start))
    result.push(...findUnknowns(arg.end))
  } else if (
    arg instanceof BinaryExpression ||
    arg instanceof InExpression ||
    arg instanceof LikeExpression ||
    arg instanceof MathExpression ||
    arg instanceof RegexpExpression
  ) {
    result.push(...findUnknowns(arg.left))
    result.push(...findUnknowns(arg.right))
  } else if (arg instanceof CaseExpression) {
    for (const { $when, $then } of arg.cases) {
      result.push(...findUnknowns($when))
      result.push(...findUnknowns($then))
    }
    if (arg.$else) {
      result.push(...findUnknowns(arg.$else))
    }
  } else if (arg instanceof ExistsExpression) {
    result.push(...findUnknowns(arg.query))
  } else if (arg instanceof FunctionExpression) {
    for (const parameter of arg.parameters) {
      result.push(...findUnknowns(parameter))
    }
  } else if (arg instanceof IsNullExpression) {
    result.push(...findUnknowns(arg.left))
  } else if (arg instanceof ParameterExpression) {
    result.push(...findUnknowns(arg.expression))
  } else if (arg instanceof QueryExpression) {
    result.push(...findUnknowns(arg.query))
  } else if (arg instanceof Unknown) {
    result.push(arg)
  }

  return result
}

// merge queries
export function merge(base: Partial<IQuery>, subquery: Partial<IQuery>) {
  if (subquery.$distinct) {
    base.$distinct = true
  }

  if (subquery.$select && (!Array.isArray(subquery.$select) || subquery.$select.length)) {
    // normalize
    function normalize($select?: string | IResultColumn | IResultColumn[]): IResultColumn[] {
      if (!$select) {
        $select = []
      }
      if (typeof $select === 'string') {
        $select = { expression: new ColumnExpression($select) }
      }
      if (!Array.isArray($select)) {
        $select = [$select]
      }

      return $select
    }

    base.$select = normalize(base.$select)
    subquery.$select = normalize(subquery.$select)
    base.$select.push(...subquery.$select)
  }

  if (subquery.$from) {
    base.$from = mergeTable(base.$from, subquery.$from)
  }

  if (subquery.$where) {
    // normalize
    function normalize(
      $where?: IConditionalExpression | IConditionalExpression[]
    ): IConditionalExpression[] {
      if (!$where) {
        $where = []
      }
      if (!Array.isArray($where)) {
        $where = [$where]
      }
      return $where
    }

    base.$where = normalize(base.$where)
    subquery.$where = normalize(subquery.$where)
    base.$where = new AndExpressions([...base.$where, ...subquery.$where])
  }

  if (subquery.$group) {
    // normalize
    function normalize($group?: string | IGroupBy): IGroupBy {
      if (!$group) {
        $group = { expressions: [] }
      }
      if (typeof $group === 'string') {
        $group = { expressions: [new ColumnExpression($group)] }
      }
      if (!Array.isArray($group.expressions)) {
        $group.expressions = [$group.expressions]
      }
      if (!Array.isArray($group.$having)) {
        $group.$having = $group.$having ? [$group.$having] : []
      }
      return $group
    }

    base.$group = normalize(base.$group)
    subquery.$group = normalize(subquery.$group)

    const expressions = base.$group.expressions as IExpression[]
    expressions.push(...(subquery.$group.expressions as IExpression[]))

    if (
      !(base.$group.$having as IConditionalExpression[]).length &&
      !(subquery.$group.$having as IConditionalExpression[]).length
    ) {
      delete base.$group.$having
    } else {
      const $having = base.$group.$having as IConditionalExpression[]
      $having.push(...(subquery.$group.$having as IConditionalExpression[]))
    }
  }

  if (subquery.$order) {
    // normalize
    function normalize(order?: string | IOrderBy | IOrderBy[]): IOrderBy[] {
      if (!order) {
        order = []
      }
      if (typeof order === 'string') {
        order = { expression: new ColumnExpression(order) }
      }
      if (!Array.isArray(order)) {
        order = [order]
      }
      return order
    }

    base.$order = normalize(base.$order)
    subquery.$order = normalize(subquery.$order)
    base.$order.push(...subquery.$order)
  }

  if (subquery.$limit && !base.$limit) {
    base.$limit = subquery.$limit
  }

  if (subquery.$union) {
    if (!base.$union) {
      base.$union = subquery.$union
    } else {
      let $union = base.$union
      while ($union.$union) {
        $union = $union.$union
      }
      $union.$union = subquery.$union
    }
  }
}

function mergeTable(
  base: string | IFromTable | IFromTable[] = [],
  subquery: string | IFromTable | IFromTable[] = []
): IFromTable[] {
  // normalize
  function normalize(table: string | IFromTable | IFromTable[]): IFromTable[] {
    if (typeof table === 'string') {
      table = { table }
    }
    if (!Array.isArray(table)) {
      table = [table]
    }
    return table
  }

  base = normalize(base)
  subquery = normalize(subquery)

  if (!base.length) return subquery
  if (!subquery.length) return base

  for (const t of subquery) {
    const exists = base.find(
      ({ table, $as }) => (typeof table === 'object' ? $as : table) === t.table
    )

    // new table
    if (!exists) {
      base.push(t)
    }

    // new JOIN
    else if (t.joinClauses) {
      exists.joinClauses = exists.joinClauses || []
      if (!Array.isArray(t.joinClauses)) {
        t.joinClauses = [t.joinClauses]
      }
      if (!Array.isArray(exists.joinClauses)) {
        exists.joinClauses = [exists.joinClauses]
      }
      exists.joinClauses.push(...t.joinClauses)
    }
  }
  return base
}

// fix REGEX
export function fixRegexp(query: IQuery) {
  if (query.$select && typeof query.$select !== 'string') {
    for (const { expression } of Array.isArray(query.$select) ? query.$select : [query.$select]) {
      fix_(expression)
    }
  }

  if (query.$from && typeof query.$from !== 'string') {
    function fixTable({ table, joinClauses }: IFromTable) {
      if (typeof table !== 'string' && 'classname' in table) {
        fixRegexp(table)
      }
      if (joinClauses) {
        for (const { table, $on } of Array.isArray(joinClauses) ? joinClauses : [joinClauses]) {
          if (typeof table !== 'string') {
            fixTable(table)
          }
          if ($on) {
            for (const expr of Array.isArray($on) ? $on : [$on]) fix_(expr)
          }
        }
      }
    }

    for (const table of Array.isArray(query.$from) ? query.$from : [query.$from]) {
      fixTable(table)
    }
  }

  if (query.$where) {
    for (const expr of Array.isArray(query.$where) ? query.$where : [query.$where]) {
      fix_(expr)
    }
  }

  if (query.$group && typeof query.$group !== 'string') {
    for (const expr of Array.isArray(query.$group.expressions)
      ? query.$group.expressions
      : [query.$group.expressions]) {
      fix_(expr)
    }
    if (query.$group.$having) {
      for (const expr of Array.isArray(query.$group.$having)
        ? query.$group.$having
        : [query.$group.$having]) {
        fix_(expr)
      }
    }
  }

  if (query.$order && typeof query.$order !== 'string') {
    for (const { expression } of Array.isArray(query.$order) ? query.$order : [query.$order]) {
      fix_(expression)
    }
  }

  if (query.$limit && typeof query.$limit !== 'number') {
    if (typeof query.$limit.$limit !== 'number') {
      fix_(query.$limit.$limit)
    }
    if (query.$limit.$offset && typeof query.$limit.$offset !== 'number') {
      fix_(query.$limit.$offset)
    }
  }

  if (query.$union) {
    fixRegexp(query.$union)
  }
}

function fix_(json: IExpression, isRegexp?: boolean) {
  switch (json.classname) {
    case 'BetweenExpression': {
      const json_ = json as IBetweenExpression
      fix_(json_.left)
      fix_(json_.start)
      fix_(json_.end)
      break
    }
    case 'BinaryExpression':
    case 'MathExpression': {
      const json_ = json as IBinaryExpression | IMathExpression
      fix_(json_.left)
      fix_(json_.right)
      break
    }
    case 'CaseExpression': {
      const json_ = json as ICaseExpression
      for (const { $when, $then } of Array.isArray(json_.cases) ? json_.cases : [json_.cases]) {
        fix_($when)
        fix_($then)
      }
      if (json_.$else) {
        fix_(json_.$else)
      }
      break
    }
    case 'ExistsFunction': {
      const json_ = json as IExistsExpression
      fixRegexp(json_.query)
      break
    }
    case 'FunctionExpression': {
      const json_ = json as IFunctionExpression
      for (const p of json_.parameters) {
        if (typeof p === 'object') {
          fix_(p)
        }
      }
      break
    }
    case 'OrExpressions':
    case 'AndExpressions': {
      const json_ = json as IGroupedExpressions
      for (const expr of json_.expressions) {
        fix_(expr)
      }
      break
    }
    case 'InExpression': {
      const json_ = json as IInExpression
      fix_(json_.left)
      if (json_.right && !Array.isArray(json_.right) && json_.right.classname === 'Query') {
        fixRegexp(json_.right as IQuery)
      }
      break
    }
    case 'IsNullExpression':
    case 'LikeExpression': {
      const json_ = json as IIsNullExpression | ILikeExpression
      fix_(json_.left)
      break
    }
    case 'ParameterExpression': {
      const json_ = json as IParameterExpression
      fix_(json_.expression)
      break
    }
    case 'QueryExpression': {
      const query: IQuery = json['query']
      fixRegexp(query)
      break
    }
    case 'RegexpExpression': {
      const json_ = json as IRegexpExpression
      fix_(json_.left)
      if (typeof json_.right === 'string') {
        json_.right = escapeRegex(json_.right)
      }
      else if (json_.right && !types.isRegExp(json_.right)) {
        fix_(json_.right, true)
      }
      break
    }
    case 'Unknown': {
      const json_ = json as IUnknown
      if (isRegexp && typeof json_['value'] === 'string') {
        json_['value'] = escapeRegex(json_['value'])
      }
      break
    }
    case 'Value': {
      const json_ = json as IValue
      if (isRegexp && typeof json_.value === 'string') {
        json_.value = escapeRegex(json_.value)
      }
      break
    }
  }
}

function escapeRegex(value: string): string {
  return value.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
}

// create partial query
export function newQueryWithoutWildcard(json: Partial<IQuery>) {
  const query = new Query(json)
  query.$select = query.$select.filter(c => {
    if (c.expression instanceof ColumnExpression) {
      return !(c.expression.name === '*' && !c.expression.table)
    }
    return true
  })
  return query
}
