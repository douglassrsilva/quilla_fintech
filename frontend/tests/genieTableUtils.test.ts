import assert from 'node:assert/strict'

import { numericValue, plainText, resolveMetricIndex } from '../src/genieTableUtils.ts'

assert.equal(plainText(undefined), '')
assert.equal(plainText(null), '')
assert.equal(plainText('[Fonte](https://example.com)'), 'Fonte')

assert.equal(numericValue(undefined), null)
assert.equal(numericValue('1.234,56%'), 1234.56)
assert.equal(numericValue('sem dado'), null)

assert.equal(resolveMetricIndex(4, [1, 4]), 4)
assert.equal(resolveMetricIndex(4, [1, 2]), 1)
assert.equal(resolveMetricIndex(4, []), -1)

console.log('Genie table regression tests passed')
