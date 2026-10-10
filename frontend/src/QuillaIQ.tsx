import {
  Archive,
  ArrowUpRight,
  Check,
  Clock3,
  Database,
  ExternalLink,
  History,
  LoaderCircle,
  MessageSquarePlus,
  Send,
  ShieldCheck,
  Sparkles,
  Square,
  ThumbsDown,
  ThumbsUp,
  X,
} from 'lucide-react'
import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import {
  ConversationDetail,
  ConversationSummary,
  createFeedback,
  Evidence,
  getConversation,
  listConversations,
  StoredMessage,
  streamChat,
  updateConversation,
} from './api'

type Lang = 'pt' | 'es'
type MessageStatus = 'complete' | 'working' | 'error' | 'cancelled'

type ChatMessage = {
  id: string
  role: 'user' | 'assistant'
  content: string
  status: MessageStatus
  traceId?: string
  route?: string
  evidence?: Evidence[]
  requiresReview?: boolean
  storedMessageId?: string
}

type Props = {
  lang: Lang
  selectedCountry: string
  avatar: ReactNode
  launch?: { id: number; question: string }
}

const words = {
  pt: {
    title: 'O mercado, em perspectiva.',
    subtitle: 'Notícias, macroeconomia, mercados e investimentos com pesquisa pública governada e fontes verificáveis.',
    greeting: 'O que você quer entender sobre o mercado agora?',
    placeholder: 'Pergunte sobre mercados, economia, ativos ou acontecimentos recentes…',
    send: 'Enviar pergunta',
    cancel: 'Interromper geração',
    newChat: 'Nova conversa',
    history: 'Conversas',
    emptyHistory: 'Seu histórico aparecerá aqui.',
    loadingHistory: 'Carregando histórico…',
    noTitle: 'Nova conversa',
    analysing: 'Analisando e consultando fontes governadas',
    connecting: 'Conectando ao Quilla Supervisor',
    waiting: 'Aguardando o Supervisor e as ferramentas governadas',
    elapsed: 'decorridos',
    completed: 'Análise concluída',
    stopped: 'Geração interrompida.',
    failed: 'Não foi possível concluir a resposta.',
    sources: 'Fontes e ferramentas',
    governed: 'Dados e ferramentas governados pelo Unity Catalog e Unity AI Gateway',
    review: 'Revisão humana recomendada',
    useful: 'Esta resposta foi útil?',
    feedbackSent: 'Feedback registrado',
    archive: 'Arquivar conversa',
    context: 'Contexto ativo',
    scope: 'Escopo',
    locale: 'Idioma',
    route: 'Rota',
    status: 'Estado',
    online: 'Supervisor online',
    mobileHistory: 'Abrir histórico',
    close: 'Fechar',
    evidence: 'Evidência',
    suggestions: [
      'Quais títulos tiveram as maiores altas na bolsa brasileira?',
      'Como está o cenário econômico do Equador no último mês?',
      'Qual foi a variação do petróleo e do ouro nesta semana?',
      'Como está o desempenho das big techs no mundo?',
    ],
  },
  es: {
    title: 'El mercado, en perspectiva.',
    subtitle: 'Noticias, macroeconomía, mercados e inversiones con investigación pública gobernada y fuentes verificables.',
    greeting: '¿Qué quieres entender sobre el mercado ahora?',
    placeholder: 'Pregunta sobre mercados, economía, activos o acontecimientos recientes…',
    send: 'Enviar pregunta',
    cancel: 'Detener generación',
    newChat: 'Nueva conversación',
    history: 'Conversaciones',
    emptyHistory: 'Tu historial aparecerá aquí.',
    loadingHistory: 'Cargando historial…',
    noTitle: 'Nueva conversación',
    analysing: 'Analizando y consultando fuentes gobernadas',
    connecting: 'Conectando con Quilla Supervisor',
    waiting: 'Esperando al Supervisor y las herramientas gobernadas',
    elapsed: 'transcurridos',
    completed: 'Análisis concluido',
    stopped: 'Generación interrumpida.',
    failed: 'No fue posible concluir la respuesta.',
    sources: 'Fuentes y herramientas',
    governed: 'Datos y herramientas gobernados por Unity Catalog y Unity AI Gateway',
    review: 'Se recomienda revisión humana',
    useful: '¿Esta respuesta fue útil?',
    feedbackSent: 'Feedback registrado',
    archive: 'Archivar conversación',
    context: 'Contexto activo',
    scope: 'Alcance',
    locale: 'Idioma',
    route: 'Ruta',
    status: 'Estado',
    online: 'Supervisor en línea',
    mobileHistory: 'Abrir historial',
    close: 'Cerrar',
    evidence: 'Evidencia',
    suggestions: [
      '¿Qué activos tuvieron las mayores subidas en la bolsa brasileña?',
      '¿Cómo está la economía de Ecuador en el último mes?',
      '¿Cuál fue la variación del petróleo y del oro esta semana?',
      '¿Cómo se están comportando las big techs globales?',
    ],
  },
}

function id(): string {
  return crypto.randomUUID()
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

function valueAsEvidence(value: unknown): Evidence[] {
  if (typeof value === 'string') {
    try { return valueAsEvidence(JSON.parse(value)) } catch { return [] }
  }
  return Array.isArray(value) ? value.filter(item => item && typeof item === 'object') as Evidence[] : []
}

function fromStored(message: StoredMessage): ChatMessage {
  const metadata = valueAsObject(message.metadata)
  return {
    id: message.message_id,
    storedMessageId: message.message_id,
    role: message.role,
    content: message.content,
    status: 'complete',
    traceId: message.mlflow_trace_id || undefined,
    route: typeof metadata.route === 'string' ? metadata.route : undefined,
    evidence: valueAsEvidence(message.citations),
    requiresReview: metadata.requires_human_review === true,
  }
}

function safeUrl(value: unknown): string | null {
  if (typeof value !== 'string') return null
  try {
    const parsed = new URL(value)
    return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null
  } catch { return null }
}

function EvidenceList({ items, label, fallbackLabel }: { items: Evidence[]; label: string; fallbackLabel: string }) {
  if (!items.length) return null
  return (
    <div className="iq-evidence">
      <b><Database size={14} />{label}</b>
      <div>
        {items.map((item, index) => {
          const url = safeUrl(item.url)
          const title = String(item.title || item.name || item.source || item.tool || item.type || `${fallbackLabel} ${index + 1}`)
          const content = <><span>{title}</span>{url && <ExternalLink size={12} />}</>
          return url
            ? <a key={`${title}-${index}`} href={url} target="_blank" rel="noreferrer">{content}</a>
            : <span className="iq-tool-chip" key={`${title}-${index}`}>{content}</span>
        })}
      </div>
    </div>
  )
}

export default function QuillaIQ({ lang, selectedCountry, avatar, launch }: Props) {
  const t = words[lang]
  const [query, setQuery] = useState('')
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [conversations, setConversations] = useState<ConversationSummary[]>([])
  const [conversationId, setConversationId] = useState<string | null>(null)
  const [sessionId, setSessionId] = useState<string | null>(null)
  const [conversationGeo, setConversationGeo] = useState<{ country: string; locale: string } | null>(null)
  const [busy, setBusy] = useState(false)
  const [loadingHistory, setLoadingHistory] = useState(true)
  const [historyOpen, setHistoryOpen] = useState(false)
  const [heartbeatCount, setHeartbeatCount] = useState(0)
  const [progress, setProgress] = useState<{ stage: 'request_accepted' | 'awaiting_supervisor'; elapsed: number }>({ stage: 'request_accepted', elapsed: 0 })
  const [feedback, setFeedback] = useState<Record<string, -1 | 1>>({})
  const [historyError, setHistoryError] = useState('')
  const abortRef = useRef<AbortController | null>(null)
  const bottomRef = useRef<HTMLDivElement | null>(null)
  const activeAssistant = useRef<string | null>(null)
  const launched = useRef<number | null>(null)

  const geo = useMemo(() => requestGeo(selectedCountry, lang), [selectedCountry, lang])
  const activeGeo = useMemo(() => {
    const country = conversationGeo?.country || geo.country
    return { country, locale: responseLocale(country, lang) }
  }, [conversationGeo?.country, geo.country, lang])
  const scope = conversationGeo?.country || (selectedCountry === 'REG' ? 'Regional · 12 países' : selectedCountry)

  const refreshConversations = useCallback(async () => {
    try {
      const result = await listConversations(false, 'market_intelligence')
      setConversations(result.conversations)
      setHistoryError('')
      return result.conversations
    } catch (error) {
      setHistoryError(error instanceof Error ? error.message : t.failed)
      return []
    } finally {
      setLoadingHistory(false)
    }
  }, [t.failed])

  useEffect(() => { void refreshConversations() }, [refreshConversations])
  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' }) }, [messages, heartbeatCount])
  useEffect(() => () => abortRef.current?.abort(), [])

  const openConversation = async (summary: ConversationSummary) => {
    if (busy) return
    setLoadingHistory(true)
    try {
      const detail: ConversationDetail = await getConversation(summary.session_id)
      setMessages(detail.messages.map(fromStored))
      setConversationId(detail.external_conversation_id)
      setSessionId(detail.session_id)
      setConversationGeo({ country: detail.country_code, locale: detail.locale })
      setHistoryOpen(false)
      setHistoryError('')
    } catch (error) {
      setHistoryError(error instanceof Error ? error.message : t.failed)
    } finally {
      setLoadingHistory(false)
    }
  }

  const newConversation = () => {
    if (busy) abortRef.current?.abort()
    setMessages([])
    setConversationId(null)
    setSessionId(null)
    setConversationGeo(null)
    setHeartbeatCount(0)
    setProgress({ stage: 'request_accepted', elapsed: 0 })
    setHistoryOpen(false)
  }

  const reconcileStoredMessage = useCallback(async (externalId: string, assistantId: string, traceId?: string) => {
    const summaries = await refreshConversations()
    const summary = summaries.find(item => item.external_conversation_id === externalId)
    if (!summary) return
    setSessionId(summary.session_id)
    const detail = await getConversation(summary.session_id)
    const stored = [...detail.messages].reverse().find(item =>
      item.role === 'assistant' && (!traceId || item.mlflow_trace_id === traceId)
    )
    if (stored) {
      setMessages(current => current.map(item => item.id === assistantId
        ? {
            ...item,
            storedMessageId: stored.message_id,
            evidence: valueAsEvidence(stored.citations).length
              ? valueAsEvidence(stored.citations)
              : item.evidence,
          }
        : item))
    }
  }, [refreshConversations])

  const ask = async (event?: FormEvent, suggested?: string, startFresh = false) => {
    event?.preventDefault()
    const prompt = (suggested ?? query).trim()
    if (!prompt || busy) return

    const externalId = startFresh ? id() : (conversationId || id())
    const requestContext = !startFresh && conversationId && conversationGeo ? activeGeo : geo
    const assistantId = id()
    const controller = new AbortController()
    abortRef.current = controller
    activeAssistant.current = assistantId
    setConversationId(externalId)
    setConversationGeo(requestContext)
    setQuery('')
    setBusy(true)
    setHeartbeatCount(0)
    setProgress({ stage: 'request_accepted', elapsed: 0 })
    const nextMessages: ChatMessage[] = [
      { id: id(), role: 'user', content: prompt, status: 'complete' },
      { id: assistantId, role: 'assistant', content: '', status: 'working' },
    ]
    setMessages(current => startFresh ? nextMessages : [...current, ...nextMessages])

    try {
      await streamChat({
        message: prompt,
        conversation_id: externalId,
        country_code: requestContext.country,
        locale: requestContext.locale,
        experience: 'market_intelligence',
      }, id(), controller.signal, async eventData => {
        if (eventData.type === 'accepted') {
          setProgress({ stage: eventData.data.stage, elapsed: eventData.data.elapsed_seconds })
        }
        if (eventData.type === 'heartbeat') {
          setHeartbeatCount(count => count + 1)
          setProgress({ stage: eventData.data.stage, elapsed: eventData.data.elapsed_seconds })
        }
        if (eventData.type === 'delta') {
          setMessages(current => current.map(item => item.id === assistantId
            ? { ...item, content: item.content + eventData.data.text }
            : item))
        }
        if (eventData.type === 'evidence') {
          setMessages(current => current.map(item => item.id === assistantId
            ? { ...item, evidence: eventData.data.items }
            : item))
        }
        if (eventData.type === 'completed') {
          const result = eventData.data
          setMessages(current => current.map(item => item.id === assistantId ? {
            ...item,
            content: result.message,
            status: 'complete',
            traceId: result.trace_id || undefined,
            route: result.route || undefined,
            evidence: result.evidence || item.evidence,
            requiresReview: result.requires_human_review,
          } : item))
          await reconcileStoredMessage(externalId, assistantId, result.trace_id || undefined)
        }
        if (eventData.type === 'error') throw new Error(eventData.data.message)
      })
    } catch (error) {
      const cancelled = error instanceof DOMException && error.name === 'AbortError'
      setMessages(current => current.map(item => item.id === assistantId ? {
        ...item,
        content: item.content || (cancelled ? t.stopped : (error instanceof Error ? error.message : t.failed)),
        status: cancelled ? 'cancelled' : 'error',
      } : item))
    } finally {
      setBusy(false)
      abortRef.current = null
      activeAssistant.current = null
    }
  }

  useEffect(() => {
    if (!launch || launch.id === launched.current || busy) return
    launched.current = launch.id
    void ask(undefined, launch.question, true)
    // A launch id is immutable and intentionally starts one governed conversation.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [launch?.id])

  const archive = async () => {
    if (!sessionId || busy) return
    await updateConversation(sessionId, { archived: true })
    await refreshConversations()
    newConversation()
  }

  const rate = async (message: ChatMessage, rating: -1 | 1) => {
    if (!sessionId || !message.storedMessageId || !message.traceId || feedback[message.id]) return
    await createFeedback({
      conversation_id: sessionId,
      message_id: message.storedMessageId,
      trace_id: message.traceId,
      rating,
      category: rating === 1 ? 'helpful' : 'needs_improvement',
    }, `ui-feedback-${message.traceId}-${rating}`)
    setFeedback(current => ({ ...current, [message.id]: rating }))
  }

  const route = [...messages].reverse().find(message => message.route)?.route

  return (
    <div className="iq-layout iq-live">
      <section className="iq-main">
        <div className="iq-heading-row">
          <div>
            <span className="eyebrow"><Sparkles size={14} /> QUILLA IQ · LIVE</span>
            <h1 className="page-title">{t.title}</h1>
            <p className="page-sub">{t.subtitle}</p>
          </div>
          <button className="iq-history-mobile" onClick={() => setHistoryOpen(true)} aria-label={t.mobileHistory}>
            <History size={18} />
          </button>
        </div>

        <div className="chat-window" aria-live="polite">
          <div className="assistant-message iq-message">
            <span className="ai-avatar">{avatar}</span>
            <div><small>QUILLA IQ</small><p>{t.greeting}</p></div>
          </div>

          {!messages.length && (
            <div className="suggestions">
              {t.suggestions.map(suggestion => (
                <button key={suggestion} onClick={() => void ask(undefined, suggestion)}>
                  {suggestion}<ArrowUpRight size={14} />
                </button>
              ))}
            </div>
          )}

          {messages.map(message => message.role === 'user' ? (
            <div className="user-message" key={message.id}>{message.content}</div>
          ) : (
            <div className={`assistant-message answer iq-message ${message.status}`} key={message.id}>
              <span className="ai-avatar">{avatar}</span>
              <div>
                <small>
                  {message.status === 'working' ? t.analysing : message.status === 'complete' ? t.completed : 'QUILLA IQ'}
                </small>
                {message.status === 'working' && !message.content && (
                  <div className="iq-working"><LoaderCircle size={16} /><span>{progress.stage === 'awaiting_supervisor' ? t.waiting : t.connecting}<small>{progress.elapsed.toFixed(0)}s {t.elapsed}</small></span></div>
                )}
                {message.content && (
                  <div className="iq-answer-text">
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm]}
                      components={{
                        a: ({ children, href }) => {
                          const url = safeUrl(href)
                          return url
                            ? <a href={url} target="_blank" rel="noreferrer">{children}<ExternalLink size={12} /></a>
                            : <span>{children}</span>
                        },
                      }}
                    >
                      {message.content}
                    </ReactMarkdown>
                  </div>
                )}
                {message.status === 'complete' && (
                  <>
                    <div className="reasoning-steps">
                      {message.route && <span><Check size={12} /> {t.route}: {message.route}</span>}
                      {message.traceId && <span><Check size={12} /> Trace: {message.traceId.slice(0, 8)}</span>}
                      <span><ShieldCheck size={12} /> Unity AI Gateway</span>
                    </div>
                    <EvidenceList items={message.evidence || []} label={t.sources} fallbackLabel={t.evidence} />
                    {message.requiresReview && <div className="iq-review"><ShieldCheck size={14} />{t.review}</div>}
                    <div className="iq-answer-footer">
                      <span><ShieldCheck size={13} />{t.governed}</span>
                      {message.traceId && (
                        <div className="iq-feedback" aria-label={t.useful}>
                          <small>{feedback[message.id] ? t.feedbackSent : t.useful}</small>
                          <button className={feedback[message.id] === 1 ? 'selected' : ''} disabled={!message.storedMessageId || !!feedback[message.id]} onClick={() => void rate(message, 1)}><ThumbsUp size={14} /></button>
                          <button className={feedback[message.id] === -1 ? 'selected' : ''} disabled={!message.storedMessageId || !!feedback[message.id]} onClick={() => void rate(message, -1)}><ThumbsDown size={14} /></button>
                        </div>
                      )}
                    </div>
                  </>
                )}
              </div>
            </div>
          ))}
          <div ref={bottomRef} />
        </div>

        <form className="prompt-bar iq-prompt" onSubmit={event => void ask(event)}>
          <Sparkles size={18} />
          <input value={query} onChange={event => setQuery(event.target.value)} placeholder={t.placeholder} disabled={busy} aria-label={t.placeholder} />
          {busy ? (
            <button type="button" className="stop" onClick={() => abortRef.current?.abort()} aria-label={t.cancel}><Square size={15} /></button>
          ) : (
            <button type="submit" disabled={!query.trim()} aria-label={t.send}><Send size={17} /></button>
          )}
        </form>
      </section>

      {historyOpen && <button className="iq-context-scrim" onClick={() => setHistoryOpen(false)} aria-label={t.close} />}
      <aside className={`iq-context ${historyOpen ? 'open' : ''}`}>
        <div className="iq-context-header">
          <div className="context-title"><History size={15} /> {t.history}</div>
          <button className="iq-context-close" onClick={() => setHistoryOpen(false)}><X size={17} /></button>
        </div>
        <button className="iq-new-chat" onClick={newConversation}><MessageSquarePlus size={16} />{t.newChat}</button>
        <div className="iq-history-list">
          {loadingHistory && <p><LoaderCircle size={14} />{t.loadingHistory}</p>}
          {!loadingHistory && !conversations.length && <p>{t.emptyHistory}</p>}
          {historyError && <p className="error">{historyError}</p>}
          {conversations.map(item => (
            <button key={item.session_id} className={item.session_id === sessionId ? 'active' : ''} onClick={() => void openConversation(item)}>
              <span>{item.title || t.noTitle}</span>
              <small><Clock3 size={11} />{new Date(item.last_activity_at).toLocaleString(lang === 'pt' ? 'pt-BR' : 'es-419', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })}</small>
            </button>
          ))}
        </div>
        {sessionId && <button className="iq-archive" onClick={() => void archive()}><Archive size={14} />{t.archive}</button>}

        <div className="iq-context-block">
          <div className="context-title"><ShieldCheck size={15} /> {t.context}</div>
          <div className="context-list">
            <span>{t.scope}<b>{scope}</b></span>
            <span>{t.locale}<b>{activeGeo.locale}</b></span>
            <span>{t.route}<b>{route || 'auto'}</b></span>
            <span>{t.status}<b className="online"><i className="live-dot" />{t.online}</b></span>
          </div>
          <div className="guardrail"><ShieldCheck size={18} /><div><b>Unity AI Gateway</b><small>{t.governed}</small></div></div>
        </div>
      </aside>
    </div>
  )
}
