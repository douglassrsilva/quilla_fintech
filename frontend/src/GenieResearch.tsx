import {
  Archive,
  BarChart3,
  BrainCircuit,
  Check,
  Clock3,
  Database,
  ExternalLink,
  History,
  LoaderCircle,
  MessageSquarePlus,
  Search,
  Send,
  ShieldCheck,
  Square,
  Table2,
  TrendingUp,
  Waypoints,
  X,
} from 'lucide-react'
import { Component, ErrorInfo, FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import {
  ConversationDetail,
  ConversationSummary,
  GenieResearchStage,
  getConversation,
  listConversations,
  StoredMessage,
  streamGenieResearch,
  updateConversation,
} from './api'
import { numericValue, plainText, resolveMetricIndex } from './genieTableUtils'

type Lang = 'pt' | 'es'
type GenieQuery = { title: string; sql: string }
type GenieTable = { headers: string[]; rows: string[][] }
type AnswerSegment = { type: 'markdown'; content: string } | { type: 'table'; table: GenieTable }
type Props = {
  lang: Lang
  selectedCountry: string
  launch?: { id: number; domain: string; question: string }
}

const domains = [
  ['executive', 'Executivo', 'Ejecutivo'], ['payments', 'Pagamentos', 'Pagos'],
  ['risk', 'Risco & Fraude', 'Riesgo y fraude'], ['credit', 'Crédito', 'Crédito'],
  ['customers', 'Clientes', 'Clientes'], ['growth', 'Growth & Wealth', 'Growth & Wealth'],
] as const

function safeUrl(value: unknown): string | null {
  if (typeof value !== 'string') return null
  try {
    const parsed = new URL(value)
    return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null
  } catch { return null }
}

function normalizeGenieMarkdown(value: string): string {
  return value
    .replace(/\\?\[\[(\d+)\]\((https?:\/\/[^)\s]+)\)\\?\]/g, '[$1]($2)')
    .replace(/(^|\n)\s*--\s*(?=\n|$)/g, '$1\n---\n')
}

function localizedStep(step: string, lang: Lang): string {
  const normalized = step.trim().toLowerCase()
  if (normalized === 'writing final response') {
    return lang === 'pt' ? 'Preparando a síntese executiva' : 'Preparando la síntesis ejecutiva'
  }
  if (normalized === 'completing checklist item') {
    return lang === 'pt' ? 'Etapa da investigação concluída' : 'Etapa de la investigación completada'
  }
  return step
}

function progressStageIcon(stage: GenieResearchStage) {
  if (stage === 'connecting') return <Search size={12}/>
  if (stage === 'planning') return <Waypoints size={12}/>
  if (stage === 'querying') return <Database size={12}/>
  if (stage === 'synthesizing') return <BrainCircuit size={12}/>
  return <Archive size={12}/>
}

function tableCells(line: string): string[] {
  return line.trim().replace(/^\|/, '').replace(/\|$/, '').split('|').map(cell => cell.trim())
}

function isTableDivider(line: string): boolean {
  const cells = tableCells(line)
  return cells.length > 1 && cells.every(cell => /^:?-{3,}:?$/.test(cell.replace(/\s/g, '')))
}

function answerSegments(markdown: string): AnswerSegment[] {
  const lines = markdown.split('\n')
  const segments: AnswerSegment[] = []
  let prose: string[] = []
  const flush = () => {
    const content = prose.join('\n').trim()
    if (content) segments.push({ type: 'markdown', content })
    prose = []
  }
  for (let index = 0; index < lines.length;) {
    if (lines[index].includes('|') && index + 1 < lines.length && isTableDivider(lines[index + 1])) {
      flush()
      const headers = tableCells(lines[index])
      const rows: string[][] = []
      index += 2
      while (index < lines.length && lines[index].includes('|') && lines[index].trim()) {
        const cells = tableCells(lines[index])
        if (cells.length === headers.length) rows.push(cells)
        index += 1
      }
      segments.push({ type: 'table', table: { headers, rows } })
      continue
    }
    prose.push(lines[index])
    index += 1
  }
  flush()
  return segments
}

const metricPriority = ['gross_volume_usd', 'net_flow_usd', 'total_events', 'completed_events', 'completion_rate', 'decline_rate', 'fraud_rate', 'avg_risk_score']

function metricLabel(value: unknown, lang: Lang): string {
  const key = plainText(value).toLowerCase().replace(/\s+/g, '_')
  const labels: Record<string, [string, string]> = {
    gross_volume_usd: ['Volume bruto', 'Volumen bruto'], net_flow_usd: ['Fluxo líquido', 'Flujo neto'],
    total_events: ['Eventos', 'Eventos'], completed_events: ['Concluídos', 'Completados'],
    completion_rate: ['Taxa de conclusão', 'Tasa de finalización'], decline_rate: ['Taxa de recusa', 'Tasa de rechazo'],
    fraud_rate: ['Taxa de fraude', 'Tasa de fraude'], avg_risk_score: ['Risco médio', 'Riesgo promedio'],
  }
  return labels[key]?.[lang === 'pt' ? 0 : 1] || plainText(value).replaceAll('_', ' ')
}

function metricNumber(value: number, metric: string): number {
  return /(rate|pct|percent)/i.test(metric) && Math.abs(value) <= 1.5 ? value * 100 : value
}

function formatMetric(value: number, metric: string, lang: Lang): string {
  const locale = lang === 'pt' ? 'pt-BR' : 'es-419'
  if (/(rate|pct|percent)/i.test(metric)) return `${new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(value)}%`
  return new Intl.NumberFormat(locale, { notation: Math.abs(value) >= 10_000 ? 'compact' : 'standard', maximumFractionDigits: 2 }).format(value)
}

function RawTable({ table, lang }: { table: GenieTable; lang: Lang }) {
  return <details className="genie-raw-table"><summary><Table2 size={14}/>{lang === 'pt' ? 'Ver dados detalhados' : 'Ver datos detallados'}</summary><div><table><thead><tr>{table.headers.map((header,index) => <th key={`${index}-${header}`}>{metricLabel(header, lang)}</th>)}</tr></thead><tbody>{table.rows.map((row,index)=><tr key={index}>{table.headers.map((_,cellIndex)=><td key={cellIndex}>{plainText(row[cellIndex])}</td>)}</tr>)}</tbody></table></div></details>
}

function GenieVisualization({ table, lang }: { table: GenieTable; lang: Lang }) {
  const numericColumns = table.headers.map((header, index) => ({ header, index, score: table.rows.filter(row => numericValue(row[index]) !== null).length }))
    .filter(column => column.score >= Math.max(2, Math.ceil(table.rows.length * .6)))
    .sort((a,b) => {
      const left = metricPriority.indexOf(plainText(a.header).toLowerCase())
      const right = metricPriority.indexOf(plainText(b.header).toLowerCase())
      return (left < 0 ? 99 : left) - (right < 0 ? 99 : right)
    })
  const labelColumn = table.headers.map((header,index)=>({header,index})).find(column => !numericColumns.some(metric=>metric.index===column.index)) || { header: table.headers[0] ?? '', index: 0 }
  const defaultMetricIndex = numericColumns[0]?.index ?? -1
  const metricSignature = numericColumns.map(column => column.index).join(',')
  const [selectedMetricIndex, setSelectedMetricIndex] = useState(defaultMetricIndex)
  const metricIndex = resolveMetricIndex(selectedMetricIndex, numericColumns.map(column => column.index))
  useEffect(() => { setSelectedMetricIndex(defaultMetricIndex) }, [defaultMetricIndex, metricSignature])
  const metric = plainText(table.headers[metricIndex])
  const values = table.rows.flatMap((row, rowIndex) => {
    const number = numericValue(row?.[metricIndex])
    const label = plainText(row?.[labelColumn.index]) || `${lang === 'pt' ? 'Linha' : 'Fila'} ${rowIndex + 1}`
    return number === null ? [] : [{ label, value: metricNumber(number, metric) }]
  }).slice(0, 18)
  const temporal = /(date|month|year|quarter|week|dia|mes|año|ano|trimestre)/i.test(labelColumn.header) || values.filter(item => /^\d{4}[-/]/.test(item.label)).length > 1
  if (numericColumns.length === 0 || values.length < 2) return <RawTable table={table} lang={lang}/>

  const width = 760
  const lineHeight = 270
  const minValue = Math.min(0, ...values.map(item=>item.value))
  const maxValue = Math.max(0, ...values.map(item=>item.value))
  const range = maxValue - minValue || 1
  const y = (value:number) => 20 + (maxValue-value)/range*195
  const x = (index:number) => 64 + index/Math.max(1,values.length-1)*670
  const barHeight = Math.max(230, values.length*38+35)
  const zeroX = 190 + (0-minValue)/range*500
  const barX = (value:number) => 190 + (value-minValue)/range*500
  const shownMetrics = numericColumns.slice(0, 6)

  return <section className="genie-visualization">
    <div className="genie-viz-head"><div><span>{temporal?<TrendingUp size={15}/>:<BarChart3 size={15}/>} {lang==='pt'?'Visualização analítica':'Visualización analítica'}</span><b>{metricLabel(metric,lang)} · {metricLabel(labelColumn.header,lang)}</b></div><em>{values.length} {lang==='pt'?'pontos':'puntos'}</em></div>
    <div className="genie-metric-tabs">{shownMetrics.map(column=><button className={metricIndex===column.index?'active':''} key={column.index} onClick={()=>setSelectedMetricIndex(column.index)}>{metricLabel(column.header,lang)}</button>)}</div>
    {temporal ? <svg className="genie-line-chart" viewBox={`0 0 ${width} ${lineHeight}`} role="img" aria-label={`${metricLabel(metric,lang)} por ${metricLabel(labelColumn.header,lang)}`}>
      {[0,.25,.5,.75,1].map(mark=>{const value=maxValue-range*mark;const position=y(value);return <g key={mark}><line x1="64" x2="734" y1={position} y2={position}/><text x="56" y={position+4} textAnchor="end">{formatMetric(value,metric,lang)}</text></g>})}
      {minValue<0&&maxValue>0&&<line className="zero" x1="64" x2="734" y1={y(0)} y2={y(0)}/>}
      <polyline points={values.map((item,index)=>`${x(index)},${y(item.value)}`).join(' ')}/>
      {values.map((item,index)=><circle key={`${item.label}-${index}`} cx={x(index)} cy={y(item.value)} r="4"><title>{item.label}: {formatMetric(item.value,metric,lang)}</title></circle>)}
      <text className="axis-label" x="64" y="252">{values[0].label.slice(0,16)}</text><text className="axis-label" x="734" y="252" textAnchor="end">{values.at(-1)?.label.slice(0,16)}</text>
    </svg> : <svg className="genie-bar-chart" viewBox={`0 0 ${width} ${barHeight}`} role="img" aria-label={`${metricLabel(metric,lang)} por ${metricLabel(labelColumn.header,lang)}`}>
      <line className="zero" x1={zeroX} x2={zeroX} y1="8" y2={barHeight-15}/>
      {values.map((item,index)=>{const valueX=barX(item.value);const top=14+index*38;return <g key={`${item.label}-${index}`}><text x="8" y={top+16}>{item.label.slice(0,22)}</text><rect x={Math.min(zeroX,valueX)} y={top} width={Math.max(2,Math.abs(valueX-zeroX))} height="22" rx="5"/><text className="bar-value" x={item.value>=0?Math.min(742,valueX+8):Math.max(198,valueX-8)} y={top+16} textAnchor={item.value>=0?'start':'end'}>{formatMetric(item.value,metric,lang)}</text></g>})}
    </svg>}
    <RawTable table={table} lang={lang}/>
  </section>
}

function MarkdownContent({ content }: { content: string }) {
  return <ReactMarkdown remarkPlugins={[remarkGfm]} components={{a:({children,href})=>{const url=safeUrl(href);return url?<a href={url} target="_blank" rel="noreferrer">{children}<ExternalLink size={12}/></a>:<span>{children}</span>}}}>{content}</ReactMarkdown>
}

function GenieAnswer({ answer, lang }: { answer: string; lang: Lang }) {
  const segments = useMemo(() => answerSegments(normalizeGenieMarkdown(answer)), [answer])
  return <>{segments.map((segment,index)=>segment.type==='markdown'
    ? <MarkdownContent key={`markdown-${index}`} content={segment.content}/>
    : <GenieVisualization key={`table-${index}-${segment.table.headers.join('|')}`} table={segment.table} lang={lang}/>)}</>
}

type GenieAnswerBoundaryProps = { answer: string; lang: Lang; children: ReactNode }
type GenieAnswerBoundaryState = { failed: boolean }

class GenieAnswerBoundary extends Component<GenieAnswerBoundaryProps, GenieAnswerBoundaryState> {
  state: GenieAnswerBoundaryState = { failed: false }

  static getDerivedStateFromError(): GenieAnswerBoundaryState {
    return { failed: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Genie answer rendering failed', error, info)
  }

  componentDidUpdate(previous: GenieAnswerBoundaryProps) {
    if (this.state.failed && previous.answer !== this.props.answer) {
      this.setState({ failed: false })
    }
  }

  render() {
    if (!this.state.failed) return this.props.children
    return <div className="genie-answer-fallback" role="alert">
      <b>{this.props.lang === 'pt' ? 'Visualização simplificada' : 'Visualización simplificada'}</b>
      <p>{this.props.lang === 'pt'
        ? 'A resposta foi preservada, mas uma visualização não pôde ser exibida.'
        : 'La respuesta fue preservada, pero no fue posible mostrar una visualización.'}</p>
      <pre>{this.props.answer}</pre>
    </div>
  }
}

function responseLocale(country: string, lang: Lang): string {
  if (lang === 'pt') return 'pt-BR'
  return country === 'BR' ? 'es-MX' : `es-${country}`
}

function requestGeo(selectedCountry: string, lang: Lang): { country: string; locale: string } {
  const country = selectedCountry === 'REG' ? (lang === 'pt' ? 'BR' : 'MX') : selectedCountry
  return { country, locale: responseLocale(country, lang) }
}

function valueAsObject(value: unknown): Record<string, unknown> {
  if (typeof value === 'string') {
    try { return JSON.parse(value) as Record<string, unknown> } catch { return {} }
  }
  return value && typeof value === 'object' ? value as Record<string, unknown> : {}
}

function valueAsQueries(value: unknown): GenieQuery[] {
  if (!Array.isArray(value)) return []
  return value.flatMap(item => {
    const candidate = valueAsObject(item)
    return typeof candidate.sql === 'string'
      ? [{ title: typeof candidate.title === 'string' ? candidate.title : 'Consulta governada', sql: candidate.sql }]
      : []
  })
}

function restoredResearch(messages: StoredMessage[]) {
  let assistantIndex = -1
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (messages[index].role === 'assistant') { assistantIndex = index; break }
  }
  if (assistantIndex < 0) return null
  let userMessage: StoredMessage | undefined
  for (let index = assistantIndex - 1; index >= 0; index -= 1) {
    if (messages[index].role === 'user') { userMessage = messages[index]; break }
  }
  const assistant = messages[assistantIndex]
  const metadata = valueAsObject(assistant.metadata)
  return {
    question: userMessage?.content || '',
    answer: assistant.content,
    steps: Array.isArray(metadata.genie_steps)
      ? metadata.genie_steps.filter((step): step is string => typeof step === 'string')
      : [],
    queries: valueAsQueries(metadata.genie_queries),
    domain: typeof metadata.domain === 'string' ? metadata.domain : undefined,
  }
}

export default function GenieResearch({ lang, selectedCountry, launch }: Props) {
  const [domain, setDomain] = useState('executive')
  const [question, setQuestion] = useState('')
  const [steps, setSteps] = useState<string[]>([])
  const [queries, setQueries] = useState<GenieQuery[]>([])
  const [answer, setAnswer] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [conversationId, setConversationId] = useState<string>()
  const [sessionId, setSessionId] = useState<string>()
  const [conversationGeo, setConversationGeo] = useState<{ country: string; locale: string }>()
  const [conversations, setConversations] = useState<ConversationSummary[]>([])
  const [loadingHistory, setLoadingHistory] = useState(true)
  const [historyOpen, setHistoryOpen] = useState(false)
  const [historyError, setHistoryError] = useState('')
  const [pendingLaunch, setPendingLaunch] = useState<{domain:string;question:string}>()
  const [progressStage, setProgressStage] = useState<GenieResearchStage>('connecting')
  const [elapsed, setElapsed] = useState(0)
  const abort = useRef<AbortController | null>(null)
  const startedAt = useRef(0)
  const formRef = useRef<HTMLFormElement | null>(null)
  const handledLaunch = useRef<number | undefined>(undefined)
  const tr = (pt: string, es: string) => lang === 'pt' ? pt : es
  const geo = useMemo(() => requestGeo(selectedCountry, lang), [selectedCountry, lang])
  const activeGeo = conversationId && conversationGeo ? conversationGeo : geo
  const activeDomain = domains.find(([id]) => id === domain)

  const refreshConversations = useCallback(async () => {
    try {
      const result = await listConversations(false, 'genie_research')
      setConversations(result.conversations)
      setHistoryError('')
      return result.conversations
    } catch (reason) {
      setHistoryError(reason instanceof Error ? reason.message : tr('Histórico indisponível.', 'Historial no disponible.'))
      return []
    } finally {
      setLoadingHistory(false)
    }
  }, [lang])

  useEffect(() => { void refreshConversations() }, [refreshConversations])
  useEffect(() => () => abort.current?.abort(), [])
  useEffect(() => {
    if (!busy) return
    const timer = window.setInterval(() => setElapsed(Math.floor((Date.now()-startedAt.current)/1000)), 1000)
    return () => window.clearInterval(timer)
  }, [busy])

  const resetResearch = () => {
    abort.current?.abort()
    setQuestion('')
    setSteps([])
    setQueries([])
    setAnswer('')
    setError('')
    setConversationId(undefined)
    setSessionId(undefined)
    setConversationGeo(undefined)
    setProgressStage('connecting')
    setElapsed(0)
    setHistoryOpen(false)
  }

  const selectDomain = (nextDomain: string) => {
    if (nextDomain === domain || busy) return
    resetResearch()
    setDomain(nextDomain)
  }

  useEffect(() => {
    if (!launch || handledLaunch.current === launch.id) return
    handledLaunch.current = launch.id
    resetResearch()
    setDomain(domains.some(([id]) => id === launch.domain) ? launch.domain : 'executive')
    setQuestion(launch.question)
    setPendingLaunch({ domain: launch.domain, question: launch.question })
  }, [launch])

  const openConversation = async (summary: ConversationSummary) => {
    if (busy) return
    setLoadingHistory(true)
    try {
      const detail: ConversationDetail = await getConversation(summary.session_id)
      const restored = restoredResearch(detail.messages)
      const context = valueAsObject(detail.context)
      if (!restored) throw new Error(tr('A conversa não possui resposta armazenada.', 'La conversación no tiene respuesta almacenada.'))
      setQuestion(restored.question)
      setAnswer(restored.answer)
      setSteps(restored.steps)
      setQueries(restored.queries)
      setDomain(restored.domain || (typeof context.domain === 'string' ? context.domain : 'executive'))
      setConversationId(detail.external_conversation_id)
      setSessionId(detail.session_id)
      setConversationGeo({ country: detail.country_code, locale: detail.locale })
      setError('')
      setHistoryError('')
      setHistoryOpen(false)
    } catch (reason) {
      setHistoryError(reason instanceof Error ? reason.message : tr('Não foi possível abrir a conversa.', 'No fue posible abrir la conversación.'))
    } finally {
      setLoadingHistory(false)
    }
  }

  const archiveConversation = async () => {
    if (!sessionId || busy) return
    try {
      await updateConversation(sessionId, { archived: true })
      resetResearch()
      await refreshConversations()
    } catch (reason) {
      setHistoryError(reason instanceof Error ? reason.message : tr('Não foi possível arquivar.', 'No fue posible archivar.'))
    }
  }

  const ask = async (event: FormEvent) => {
    event.preventDefault()
    const prompt = question.trim()
    if (!prompt || busy) return
    const controller = new AbortController()
    const requestId = crypto.randomUUID()
    let resolvedConversationId = conversationId
    const requestContext = conversationId && conversationGeo ? conversationGeo : geo
    abort.current = controller
    setConversationGeo(requestContext)
    setBusy(true)
    startedAt.current = Date.now()
    setProgressStage('connecting')
    setElapsed(0)
    setSteps([])
    setQueries([])
    setAnswer('')
    setError('')
    try {
      await streamGenieResearch({
        question: prompt,
        domain,
        conversation_id: resolvedConversationId,
        request_id: requestId,
        country_code: requestContext.country,
        locale: requestContext.locale,
      }, controller.signal, item => {
        if (item.type === 'accepted' || item.type === 'progress') {
          setProgressStage(item.data.stage)
          setElapsed(Math.floor(item.data.elapsed_seconds))
        }
        if (item.type === 'conversation') {
          resolvedConversationId = item.data.conversation_id
          setConversationId(resolvedConversationId)
        }
        if (item.type === 'step') { setProgressStage('planning'); setSteps(current => [...current, item.data.text]) }
        if (item.type === 'query') { setProgressStage('querying'); setQueries(current => [...current, item.data]) }
        if (item.type === 'answer') { setProgressStage('synthesizing'); setAnswer(item.data.text) }
        if (item.type === 'completed') {
          setProgressStage('persisting')
          if (item.data.answer) setAnswer(item.data.answer)
          if (item.data.conversation_id) setConversationId(item.data.conversation_id)
          if (item.data.session_id) setSessionId(item.data.session_id)
          void refreshConversations()
        }
        if (item.type === 'error') setError(item.data.message)
      })
    } catch (reason) {
      if (!(reason instanceof DOMException && reason.name === 'AbortError')) {
        setError(reason instanceof Error ? reason.message : tr('Não foi possível concluir a pesquisa.', 'No fue posible concluir la investigación.'))
      }
    } finally {
      setBusy(false)
      abort.current = null
    }
  }

  useEffect(() => {
    if (!pendingLaunch || busy || question !== pendingLaunch.question || domain !== pendingLaunch.domain) return
    setPendingLaunch(undefined)
    formRef.current?.requestSubmit()
  }, [busy, domain, pendingLaunch, question])

  const progressStages: { id: GenieResearchStage; pt: string; es: string }[] = [
    {id:'connecting',pt:'Conectando ao Genie Agent',es:'Conectando con Genie Agent'},
    {id:'planning',pt:'Planejando a investigação',es:'Planificando la investigación'},
    {id:'querying',pt:'Consultando dados governados',es:'Consultando datos gobernados'},
    {id:'synthesizing',pt:'Construindo a análise executiva',es:'Construyendo el análisis ejecutivo'},
    {id:'persisting',pt:'Salvando evidências no Lakebase',es:'Guardando evidencias en Lakebase'},
  ]
  const currentProgress = Math.max(0, progressStages.findIndex(item=>item.id===progressStage))
  const elapsedLabel = `${Math.floor(elapsed/60)}:${String(elapsed%60).padStart(2,'0')}`

  return <div className="iq-layout iq-live genie-live">
    <section className="iq-main genie-research-page">
      <div className="page-heading"><div><span className="eyebrow"><BrainCircuit size={14}/> GENIE AGENT · DEEP RESEARCH</span>
        <h1 className="page-title">{tr('Inteligência do negócio,', 'Inteligencia del negocio,')}<br/><span className="gradient-text">{tr('investigada a fundo.', 'investigada a fondo.')}</span></h1>
        <p className="page-sub">{tr('Pesquisa multi-etapas sobre Metric Views e dados Gold governados da Quilla.', 'Investigación en varios pasos sobre Metric Views y datos Gold gobernados de Quilla.')}</p></div>
        <div className="genie-heading-actions"><span className="genie-mode"><i className="live-dot"/> Agent Mode Beta</span>
          <button className="iq-history-mobile" onClick={() => setHistoryOpen(true)} aria-label={tr('Abrir histórico', 'Abrir historial')}><History size={18}/></button></div>
      </div>
      <div className="genie-domain-tabs">{domains.map(([id,pt,es]) => <button key={id} disabled={busy} className={domain===id?'active':''} onClick={()=>selectDomain(id)}>{lang==='pt'?pt:es}</button>)}</div>
      <section className="panel genie-research-panel">
        <form ref={formRef} className="genie-research-form" onSubmit={ask}><Search size={18}/><input disabled={busy} value={question} onChange={event=>setQuestion(event.target.value)} placeholder={tr('Pergunte sobre performance, risco, crédito, clientes ou crescimento…','Pregunta sobre desempeño, riesgo, crédito, clientes o crecimiento…')}/>
          {busy ? <button type="button" onClick={()=>abort.current?.abort()} aria-label={tr('Interromper pesquisa','Detener investigación')}><Square size={15}/></button> : <button disabled={!question.trim()} aria-label={tr('Pesquisar','Investigar')}><Send size={17}/></button>}</form>
        {busy && <div className="genie-progress-live" role="status" aria-live="polite"><div className="genie-progress-orbit"><i/><i/><BrainCircuit size={22}/></div><div className="genie-progress-copy"><span>{tr('PESQUISA EM ANDAMENTO','INVESTIGACIÓN EN CURSO')} · {elapsedLabel}</span><b>{lang==='pt'?progressStages[currentProgress].pt:progressStages[currentProgress].es}</b><small>{queries.length?`${queries.length} ${tr('consultas identificadas','consultas identificadas')}`:tr('O agente permanece ativo; você pode interromper a qualquer momento.','El agente sigue activo; puedes detenerlo en cualquier momento.')}</small></div><div className="genie-progress-track" aria-label={tr('Pipeline técnico da pesquisa','Pipeline técnico de la investigación')}>{progressStages.map((stage,index)=>{const label=lang==='pt'?stage.pt:stage.es;return <i key={stage.id} title={label} aria-label={label} className={index<currentProgress?'done':index===currentProgress?'active':''}><span>{index<currentProgress?<Check size={12}/>:progressStageIcon(stage.id)}</span></i>})}</div></div>}
        {!!steps.length && <div className="genie-steps"><b><Waypoints size={15}/>{busy?tr('Plano em execução','Plan en ejecución'):tr('Plano executado','Plan ejecutado')}</b>{steps.map((step,index)=><div key={`${index}-${step}`}><span>{index+1}</span><p>{localizedStep(step, lang)}</p></div>)}</div>}
        {!!answer && <div className="genie-answer"><b><Check size={15}/> GENIE AGENT</b><div className="genie-answer-text">
          <GenieAnswerBoundary answer={answer} lang={lang}>
            <GenieAnswer answer={answer} lang={lang}/>
          </GenieAnswerBoundary>
        </div></div>}
        {!!queries.length && <details className="genie-queries"><summary><Database size={14}/>{queries.length} {tr('consultas governadas','consultas gobernadas')}</summary>{queries.map((query,index)=><div key={`${index}-${query.title}`}><b>{query.title}</b><pre>{query.sql}</pre></div>)}</details>}
        {error && <div className="genie-error">{error}</div>}
        {!busy && !answer && !error && <div className="genie-empty"><BrainCircuit size={28}/><p>{tr('O Agent Mode decompõe perguntas complexas e executa múltiplas consultas SQL governadas.','Agent Mode descompone preguntas complejas y ejecuta múltiples consultas SQL gobernadas.')}</p></div>}
      </section>
    </section>

    {historyOpen && <button className="iq-context-scrim" onClick={() => setHistoryOpen(false)} aria-label={tr('Fechar', 'Cerrar')}/>} 
    <aside className={`iq-context ${historyOpen ? 'open' : ''}`}>
      <div className="iq-context-header"><div className="context-title"><History size={15}/>{tr('Pesquisas', 'Investigaciones')}</div>
        <button className="iq-context-close" onClick={() => setHistoryOpen(false)} aria-label={tr('Fechar', 'Cerrar')}><X size={17}/></button></div>
      <button className="iq-new-chat" onClick={resetResearch}><MessageSquarePlus size={16}/>{tr('Nova pesquisa', 'Nueva investigación')}</button>
      <div className="iq-history-list">
        {loadingHistory && <p><LoaderCircle size={14}/>{tr('Carregando histórico…', 'Cargando historial…')}</p>}
        {!loadingHistory && !conversations.length && <p>{tr('Seu histórico aparecerá aqui.', 'Tu historial aparecerá aquí.')}</p>}
        {historyError && <p className="error">{historyError}</p>}
        {conversations.map(item => <button key={item.session_id} className={item.session_id === sessionId ? 'active' : ''} onClick={() => void openConversation(item)}>
          <span>{item.title || tr('Nova pesquisa', 'Nueva investigación')}</span>
          <small><Clock3 size={11}/>{new Date(item.last_activity_at).toLocaleString(lang === 'pt' ? 'pt-BR' : 'es-419', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })}</small>
        </button>)}
      </div>
      {sessionId && <button className="iq-archive" onClick={() => void archiveConversation()}><Archive size={14}/>{tr('Arquivar pesquisa', 'Archivar investigación')}</button>}
      <div className="iq-context-block">
        <div className="context-title"><ShieldCheck size={15}/>{tr('Contexto governado', 'Contexto gobernado')}</div>
        <div className="context-list">
          <span>{tr('Domínio', 'Dominio')}<b>{lang === 'pt' ? activeDomain?.[1] : activeDomain?.[2]}</b></span>
          <span>{tr('País', 'País')}<b>{activeGeo.country}</b></span>
          <span>{tr('Idioma', 'Idioma')}<b>{activeGeo.locale}</b></span>
          <span>{tr('Estado', 'Estado')}<b className="online"><i className="live-dot"/>{tr('Genie online', 'Genie en línea')}</b></span>
        </div>
        <div className="guardrail"><Database size={18}/><div><b>Unity Catalog + Lakebase</b><small>{tr('Dados governados e histórico persistente', 'Datos gobernados e historial persistente')}</small></div></div>
      </div>
    </aside>
  </div>
}
