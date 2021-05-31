import { IBetweenExpression, IBinaryExpression, ICaseExpression, IExistsExpression, IExpression, IFromTable, IFunctionExpression, IGroupedExpressions, IInExpression, IIsNullExpression, ILikeExpression, IMathExpression, IParameterExpression, IQuery, IRegexpExpression, IUnknown, IValue, Query } from 'node-jql'
import { types } from 'util'

export type PostProcessor = (query: IQuery) => IQuery

export const FixRegexpProcessor: PostProcessor = function fixRegexp(query: IQuery) {
  function escapeRegex(value: string): string {
    return value.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
  }

  function fix(json: IExpression, isRegexp?: boolean) {
    switch (json.classname) {
      case 'BetweenExpression': {
        const json_ = json as IBetweenExpression
        fix(json_.left)
        fix(json_.start)
        fix(json_.end)
        break
      }
      case 'BinaryExpression':
      case 'MathExpression': {
        const json_ = json as IBinaryExpression | IMathExpression
        fix(json_.left)
        fix(json_.right)
        break
      }
      case 'CaseExpression': {
        const json_ = json as ICaseExpression
        for (const { $when, $then } of Array.isArray(json_.cases) ? json_.cases : [json_.cases]) {
          fix($when)
          fix($then)
        }
        if (json_.$else) {
          fix(json_.$else)
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
            fix(p)
          }
        }
        break
      }
      case 'OrExpressions':
      case 'AndExpressions': {
        const json_ = json as IGroupedExpressions
        for (const expr of json_.expressions) {
          fix(expr)
        }
        break
      }
      case 'InExpression': {
        const json_ = json as IInExpression
        fix(json_.left)
        if (json_.right && !Array.isArray(json_.right) && json_.right.classname === 'Query') {
          fixRegexp(json_.right as IQuery)
        }
        break
      }
      case 'IsNullExpression':
      case 'LikeExpression': {
        const json_ = json as IIsNullExpression | ILikeExpression
        fix(json_.left)
        break
      }
      case 'ParameterExpression': {
        const json_ = json as IParameterExpression
        fix(json_.expression)
        break
      }
      case 'QueryExpression': {
        const query: IQuery = json['query']
        fixRegexp(query)
        break
      }
      case 'RegexpExpression': {
        const json_ = json as IRegexpExpression
        fix(json_.left)
        if (typeof json_.right === 'string') {
          json_.right = escapeRegex(json_.right)
        }
        else if (json_.right && !types.isRegExp(json_.right)) {
          fix(json_.right, true)
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

  if (query.$select && typeof query.$select !== 'string') {
    for (const { expression } of Array.isArray(query.$select) ? query.$select : [query.$select]) {
      fix(expression)
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
            for (const expr of Array.isArray($on) ? $on : [$on]) fix(expr)
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
      fix(expr)
    }
  }

  if (query.$group && typeof query.$group !== 'string') {
    for (const expr of Array.isArray(query.$group.expressions)
      ? query.$group.expressions
      : [query.$group.expressions]) {
      fix(expr)
    }
    if (query.$group.$having) {
      for (const expr of Array.isArray(query.$group.$having)
        ? query.$group.$having
        : [query.$group.$having]) {
        fix(expr)
      }
    }
  }

  if (query.$order && typeof query.$order !== 'string') {
    for (const { expression } of Array.isArray(query.$order) ? query.$order : [query.$order]) {
      fix(expression)
    }
  }

  if (query.$limit && typeof query.$limit !== 'number') {
    if (typeof query.$limit.$limit !== 'number') {
      fix(query.$limit.$limit)
    }
    if (query.$limit.$offset && typeof query.$limit.$offset !== 'number') {
      fix(query.$limit.$offset)
    }
  }

  return query
}