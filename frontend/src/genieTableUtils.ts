export function plainText(value: unknown): string {
  return String(value ?? '')
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/[*_`]/g, '')
    .trim()
}

export function numericValue(value: unknown): number | null {
  let clean = plainText(value).replace(/\s/g, '').replace(/[^0-9eE+.,%\-]/g, '').replace('%', '')
  if (!clean || !/[0-9]/.test(clean)) return null
  const comma = clean.lastIndexOf(',')
  const dot = clean.lastIndexOf('.')
  if (comma >= 0 && dot >= 0) {
    clean = comma > dot ? clean.replace(/\./g, '').replace(',', '.') : clean.replace(/,/g, '')
  } else if (comma >= 0) {
    clean = clean.replace(',', '.')
  }
  const number = Number(clean)
  return Number.isFinite(number) ? number : null
}

export function resolveMetricIndex(selected: number, available: readonly number[], fallback = -1): number {
  return available.includes(selected) ? selected : (available[0] ?? fallback)
}
