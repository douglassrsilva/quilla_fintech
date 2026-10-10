export type Evidence = {
  url?: string
  title?: string
  name?: string
  source?: string
  snippet?: string
  tool?: string
  type?: string
  status?: string
  [key: string]: unknown
}

export type RegionalCountryMetric = {
  country_code: string
  gross_volume_usd: number
  total_events: number
  completion_rate_pct: number
  fraud_rate_pct: number
}

export type RegionalOverview = {
  source: string
  statement_id: string
  generated_at: string
  gross_volume_usd: number
  total_events: number
  countries: RegionalCountryMetric[]
}

export type TrustStatus = {
  checked_at: string
  controls: { key: string; state: 'operational' | 'configured' | 'degraded' | 'unavailable'; evidence: string }[]
}

export type Recommendation = {
  recommendation_id: string
  rank: number
  category: 'growth' | 'payments' | 'risk'
  tone: 'blue' | 'teal' | 'coral'
  urgency: 'medium' | 'high' | 'critical'
  state: 'candidate'
  title: string
  summary: string
  impact: string
  action: string
  internal_prompt: string
  external_prompt: string
  country_code: string
  model?: string | null
  model_status: 'validated_candidate' | 'unavailable'
  online_scoring: boolean
  internal_source: string
  external_signal: {
    status: 'on_demand'
    tool: string
    prompt: string
  }
  explanation: {
    version: string
    why_now: string
    method: string
    determining_signals: {
      label: string
      value: string
      numeric_value?: number | null
      unit: string
      direction: 'supports' | 'caution' | 'context'
      source_field: string
    }[]
    provenance: {
      source: string
      statement_id?: string | null
      observed_at?: string | null
      generated_at: string
      synthetic: boolean
    }
    model: {
      registered_name?: string | null
      version?: string | null
      registry: string
      validation_status: 'PASSED' | 'UNAVAILABLE'
      lifecycle_status: string
      online_scoring: boolean
      role: string
    }
    limitations: string[]
    alternatives: { key: 'no_action' | 'educate' | 'protect' | 'offer'; label: string; rationale: string }[]
    controls: string[]
  }
}

export type RecommendationFeed = {
  scope: string
  locale: string
  generated_at: string
  statement_id: string
  synthetic: boolean
  contract: string
  recommendations: Recommendation[]
}

export type Customer360 = {
  party_id: string
  display_name: string
  country_code: string
  currency_code: string
  locale_code: string
  preferred_language: string
  customer_segment: string
  risk_band: string
  kyc_status: string
  onboarding_ts?: string | null
  account_count: number
  current_balance_local: number
  available_balance_local: number
  last_account_update_at?: string | null
  lifetime_event_count: number
  lifetime_volume_usd: number
  last_event_at?: string | null
  average_event_risk_score: number
  risk_alert_count: number
  open_risk_alert_count: number
  last_alert_at?: string | null
  interaction_count: number
  average_sentiment_score: number
  last_interaction_at?: string | null
}

export type Customer360Feed = {
  scope: string
  source: string
  statement_id: string
  generated_at: string
  synthetic: boolean
  customers: Customer360[]
}

export type BusinessOverview = {
  scope: string
  locale: string
  source: string
  statement_ids: string[]
  generated_at: string
  synthetic: boolean
  kpis: {
    gross_volume_usd: number
    net_flow_usd: number
    completion_rate_pct: number
    declined_volume_usd: number
    revenue_usd_90d: number
    margin_usd_90d: number
  }
  trend: { event_date: string; net_flow_usd: number; gross_volume_usd: number }[]
  receivables: { country_code: string; declined_volume_usd: number; declined_events: number }[]
}

export type DecisionActionResult = {
  request_id: string
  action_id: string
  country_code: string
  decision_type: string
  status: string
  action: 'approve' | 'reject' | 'defer'
  selected_alternative: 'no_action' | 'educate' | 'protect' | 'offer'
  model: {
    status: string
    prediction?: string
    endpoint?: string
    scope?: string
    latency_ms?: number
    scored_at?: string
    statement_id?: string
    feature_source?: string
    feature_snapshot?: Record<string, string | number>
  }
  policy: Record<string, unknown>
  persisted: boolean
  created_at: string
  decided_at: string
}

export type RealtimeTask = {
  task_key: string
  run_id?: number
  life_cycle_state: string
  result_state?: string | null
}

export type RealtimeRun = {
  run_id: number
  run_name?: string
  state: string
  life_cycle_state: string
  result_state?: string | null
  state_message?: string | null
  started_at?: number | null
  completed_at?: number | null
  tasks: RealtimeTask[]
  scenario?: string
  event_count?: number
  synthetic?: boolean
  evidence?: Record<string, unknown> | null
}

export type OperationsOverview = {
  checked_at: string
  source: string
  statement_id: string
  synthetic: boolean
  controls: { key: string; state: 'operational' | 'configured' | 'requires_review' | 'degraded' | 'unavailable'; evidence: string }[]
  models: { total: number; validated: number; serving_endpoints: number; ready: number; online_endpoint: string; validated_at?: string | null }
  data_quality: {
    datasets: number
    reconciled_datasets: number
    reconciliation_rate_pct: number
    reconciliation_state: 'operational' | 'degraded'
    quarantine_rows: number
    quarantine_scope: 'current_materialized_snapshot'
    acceptance_rate_pct: number
    acceptance_state: 'requires_review'
    acceptance_target_pct: number | null
    acceptance_target_status: 'not_defined'
    metric_source: string
    observed_at: string
  }
  realtime: Record<string, unknown>
  dashboards: { key: string; title_pt: string; title_es: string; url: string }[]
}

export type ChatResult = {
  conversation_id: string
  message: string
  trace_id?: string | null
  route?: string | null
  evidence: Evidence[]
  requires_human_review: boolean
  request_id: string
  idempotent_replay: boolean
}

export type StreamEvent =
  | { type: 'accepted'; data: { request_id: string; conversation_id: string; stage: 'request_accepted'; elapsed_seconds: number } }
  | { type: 'heartbeat'; data: { request_id: string; stage: 'awaiting_supervisor'; elapsed_seconds: number } }
  | { type: 'delta'; data: { text: string } }
  | { type: 'evidence'; data: { items: Evidence[] } }
  | { type: 'completed'; data: ChatResult }
  | { type: 'error'; data: { request_id?: string; code: string; message: string } }

export type ConversationSummary = {
  session_id: string
  external_conversation_id: string
  title: string
  country_code: string
  locale: string
  status: string
  created_at: string
  last_activity_at: string
  message_count: number
}

export type StoredMessage = {
  message_id: string
  sequence_number: number
  role: 'user' | 'assistant'
  content: string
  language_code: string
  model_endpoint?: string | null
  mlflow_trace_id?: string | null
  citations?: Evidence[] | string | null
  metadata?: Record<string, unknown> | string | null
  created_at: string
}

export type ConversationDetail = ConversationSummary & {
  context?: Record<string, unknown> | string
  messages: StoredMessage[]
  next_message_cursor?: string | null
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const payload = await response.json() as { detail?: string; message?: string }
    return payload.detail || payload.message || `HTTP ${response.status}`
  } catch {
    return `HTTP ${response.status}`
  }
}

class HttpStatusError extends Error {
  constructor(message: string, readonly status: number) {
    super(message)
    this.name = 'HttpStatusError'
  }
}

async function jsonRequest<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: 'same-origin',
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  })
  if (!response.ok) throw new HttpStatusError(await errorMessage(response), response.status)
  return response.json() as Promise<T>
}

const delay = (milliseconds: number) => new Promise(resolve => window.setTimeout(resolve, milliseconds))

async function transientJsonRequest<T>(url: string, init?: RequestInit): Promise<T> {
  const retryable = new Set([502, 503, 504])
  const backoffMilliseconds = [250, 650]
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await jsonRequest<T>(url, init)
    } catch (error) {
      if (!(error instanceof HttpStatusError) || !retryable.has(error.status) || attempt >= backoffMilliseconds.length) throw error
      await delay(backoffMilliseconds[attempt] + Math.floor(Math.random() * 100))
    }
  }
}

export function getRegionalOverview(): Promise<RegionalOverview> {
  return jsonRequest('/api/analytics/regional')
}

export function getTrustStatus(): Promise<TrustStatus> {
  return jsonRequest('/api/analytics/trust')
}

export function getRecommendations(countryCode: string, lang: 'pt' | 'es'): Promise<RecommendationFeed> {
  const params = new URLSearchParams({
    country_code: countryCode,
    locale: lang === 'pt' ? 'pt-BR' : 'es-419',
  })
  return transientJsonRequest(`/api/analytics/recommendations?${params}`)
}

export function getCustomerProfiles(countryCode: string): Promise<Customer360Feed> {
  const params = new URLSearchParams({ country_code: countryCode, limit: '12' })
  return jsonRequest(`/api/analytics/customers?${params}`)
}

export function getBusinessOverview(countryCode: string, lang: 'pt' | 'es'): Promise<BusinessOverview> {
  const params = new URLSearchParams({
    country_code: countryCode,
    locale: lang === 'pt' ? 'pt-BR' : 'es-419',
  })
  return jsonRequest(`/api/analytics/business?${params}`)
}

export function getOperationsOverview(): Promise<OperationsOverview> {
  return jsonRequest('/api/platform/operations')
}

export function createDecisionAction(input: {
  recommendation_id: string
  country_code: string
  decision_type: 'growth' | 'payments' | 'risk'
  selected_alternative: 'no_action' | 'educate' | 'protect' | 'offer'
  selected_alternative_label: string
  selected_alternative_rationale: string
  action: 'approve' | 'reject' | 'defer'
  reason?: string
}): Promise<DecisionActionResult> {
  const idempotencyKey = `decision-${input.recommendation_id}-${input.selected_alternative}-${input.action}`
  return jsonRequest('/api/platform/decisions', {
    method: 'POST',
    headers: { 'X-Idempotency-Key': idempotencyKey },
    body: JSON.stringify(input),
  })
}

export function startRealtimeSimulation(input: {
  event_count: number
  scenario: 'normal' | 'fraud_burst' | 'cross_border' | 'dq_violations' | 'mixed'
}): Promise<RealtimeRun> {
  return jsonRequest('/api/platform/realtime', {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export function getRealtimeSimulation(runId: number): Promise<RealtimeRun> {
  return jsonRequest(`/api/platform/realtime/${runId}`)
}

export async function streamChat(
  input: { message: string; conversation_id: string; country_code: string; locale: string; experience?: 'market_intelligence' },
  idempotencyKey: string,
  signal: AbortSignal,
  onEvent: (event: StreamEvent) => void | Promise<void>,
): Promise<void> {
  const response = await fetch('/api/chat/stream', {
    method: 'POST',
    credentials: 'same-origin',
    signal,
    headers: {
      Accept: 'text/event-stream',
      'Content-Type': 'application/json',
      'X-Idempotency-Key': idempotencyKey,
      'X-Request-ID': idempotencyKey,
    },
    body: JSON.stringify(input),
  })
  if (!response.ok) throw new Error(await errorMessage(response))
  if (!response.body) {
    throw new Error(input.locale.startsWith('pt')
      ? 'Streaming não disponível neste navegador'
      : 'El streaming no está disponible en este navegador')
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  const consume = async (frame: string) => {
    let type = 'message'
    const data: string[] = []
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) type = line.slice(6).trim()
      if (line.startsWith('data:')) data.push(line.slice(5).trimStart())
    }
    if (!data.length) return
    await onEvent({ type, data: JSON.parse(data.join('\n')) } as StreamEvent)
  }

  while (true) {
    const { done, value } = await reader.read()
    buffer += decoder.decode(value, { stream: !done }).replaceAll('\r\n', '\n')
    let boundary = buffer.indexOf('\n\n')
    while (boundary >= 0) {
      await consume(buffer.slice(0, boundary))
      buffer = buffer.slice(boundary + 2)
      boundary = buffer.indexOf('\n\n')
    }
    if (done) break
  }
  if (buffer.trim()) await consume(buffer)
}

export function listConversations(includeArchived = false, experience?: 'market_intelligence' | 'genie_research'): Promise<{
  conversations: ConversationSummary[]
  next_cursor?: string | null
}> {
  const params = new URLSearchParams({ limit: '50', include_archived: String(includeArchived) })
  if (experience) params.set('experience', experience)
  return jsonRequest(`/api/conversations?${params}`)
}

export function getConversation(sessionId: string): Promise<ConversationDetail> {
  return jsonRequest(`/api/conversations/${encodeURIComponent(sessionId)}?message_limit=200`)
}

export function updateConversation(
  sessionId: string,
  changes: { title?: string; archived?: boolean },
): Promise<{ session_id: string; title: string; status: string; updated_at: string }> {
  return jsonRequest(`/api/conversations/${encodeURIComponent(sessionId)}`, {
    method: 'PATCH',
    body: JSON.stringify(changes),
  })
}

export function createFeedback(input: {
  conversation_id: string
  message_id?: string
  trace_id: string
  rating: -1 | 1
  category?: string
  comment?: string
}, idempotencyKey: string): Promise<{ feedback_id: string; review_status: string }> {
  return jsonRequest('/api/feedback', {
    method: 'POST',
    headers: { 'X-Idempotency-Key': idempotencyKey },
    body: JSON.stringify(input),
  })
}

export type GenieResearchEvent =
  | { type: 'accepted'; data: { mode: string; domain: string; stage: GenieResearchStage; elapsed_seconds: number } }
  | { type: 'progress'; data: { stage: GenieResearchStage; elapsed_seconds: number } }
  | { type: 'conversation'; data: { conversation_id: string } }
  | { type: 'step'; data: { text: string } }
  | { type: 'query'; data: { title: string; sql: string } }
  | { type: 'answer'; data: { text: string } }
  | { type: 'completed'; data: { mode: string; conversation_id?: string; session_id?: string; persisted?: boolean; answer?: string; elapsed_seconds?: number } }
  | { type: 'error'; data: { message: string } }

export type GenieResearchStage = 'connecting' | 'planning' | 'querying' | 'synthesizing' | 'persisting'

export async function streamGenieResearch(
  input: { question: string; domain: string; conversation_id?: string; request_id: string; country_code: string; locale: string },
  signal: AbortSignal,
  onEvent: (event: GenieResearchEvent) => void,
): Promise<void> {
  const response = await fetch('/api/genie/research/stream', {
    method: 'POST', credentials: 'same-origin', signal,
    headers: { Accept: 'text/event-stream', 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  })
  if (!response.ok) throw new Error(await errorMessage(response))
  if (!response.body) throw new Error('Streaming unavailable')
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read()
    buffer += decoder.decode(value, { stream: !done }).replaceAll('\r\n', '\n')
    let boundary = buffer.indexOf('\n\n')
    while (boundary >= 0) {
      const frame = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      const event = frame.split('\n').find(line => line.startsWith('event:'))?.slice(6).trim()
      const data = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trimStart()).join('\n')
      if (event && data) onEvent({ type: event, data: JSON.parse(data) } as GenieResearchEvent)
      boundary = buffer.indexOf('\n\n')
    }
    if (done) break
  }
}
