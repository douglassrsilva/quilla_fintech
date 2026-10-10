import { ReactNode, useEffect, useMemo, useRef, useState } from 'react'
import {
  Activity,
  ArrowRight,
  ArrowUpRight,
  BadgeDollarSign,
  Bell,
  BriefcaseBusiness,
  Check,
  ChevronDown,
  CircleDollarSign,
  Database,
  Eye,
  ExternalLink,
  Fingerprint,
  Globe2,
  Gauge,
  Landmark,
  LayoutDashboard,
  LineChart,
  LockKeyhole,
  Menu,
  Moon,
  Orbit,
  Play,
  Radar,
  Radio,
  RefreshCw,
  Send,
  ServerCog,
  ShieldCheck,
  Sparkles,
  Sun,
  Timer,
  UserRound,
  WalletCards,
  Workflow,
  X,
  Zap,
} from 'lucide-react'
import QuillaIQ from './QuillaIQ'
import GenieResearch from './GenieResearch'
import {
  getRecommendations,
  getCustomerProfiles,
  getRegionalOverview,
  getTrustStatus,
  getBusinessOverview,
  getOperationsOverview,
  startRealtimeSimulation,
  getRealtimeSimulation,
  createDecisionAction,
  BusinessOverview,
  Customer360,
  DecisionActionResult,
  OperationsOverview,
  Recommendation,
  RecommendationFeed,
  RealtimeRun,
  RegionalOverview,
  TrustStatus,
} from './api'

type Page = 'command' | 'iq' | 'genie' | 'personal' | 'business' | 'regional' | 'operations' | 'trust'
type Lang = 'pt' | 'es'
type Theme = 'dark' | 'light'

const countries = [
  { code: 'REG', label: { pt: 'Regional · 12 países', es: 'Regional · 12 países' } },
  { code: 'BR', label: { pt: 'Brasil', es: 'Brasil' } },
  { code: 'MX', label: { pt: 'México', es: 'México' } },
  { code: 'CO', label: { pt: 'Colômbia', es: 'Colombia' } },
  { code: 'AR', label: { pt: 'Argentina', es: 'Argentina' } },
  { code: 'CL', label: { pt: 'Chile', es: 'Chile' } },
  { code: 'PE', label: { pt: 'Peru', es: 'Perú' } },
  { code: 'EC', label: { pt: 'Equador', es: 'Ecuador' } },
  { code: 'UY', label: { pt: 'Uruguai', es: 'Uruguay' } },
  { code: 'GT', label: { pt: 'Guatemala', es: 'Guatemala' } },
  { code: 'DO', label: { pt: 'República Dominicana', es: 'República Dominicana' } },
  { code: 'CR', label: { pt: 'Costa Rica', es: 'Costa Rica' } },
  { code: 'PA', label: { pt: 'Panamá', es: 'Panamá' } },
]

const nav: { id: Page; label: Record<Lang, string>; icon: typeof LayoutDashboard }[] = [
  { id: 'command', label: { pt: 'Comando', es: 'Comando' }, icon: LayoutDashboard },
  { id: 'iq', label: { pt: 'Quilla IQ', es: 'Quilla IQ' }, icon: Sparkles },
  { id: 'genie', label: { pt: 'Genie Research', es: 'Genie Research' }, icon: LineChart },
  { id: 'personal', label: { pt: 'Pessoas', es: 'Personas' }, icon: UserRound },
  { id: 'business', label: { pt: 'Empresas', es: 'Empresas' }, icon: BriefcaseBusiness },
  { id: 'regional', label: { pt: 'Regional', es: 'Regional' }, icon: Globe2 },
  { id: 'operations', label: { pt: 'Plataforma', es: 'Plataforma' }, icon: Radio },
  { id: 'trust', label: { pt: 'Confiança', es: 'Confianza' }, icon: ShieldCheck },
]

const copy = {
  pt: {
    greeting: 'Boa tarde, Camila.',
    headline: 'Seu dinheiro está em movimento.\nA Quilla já viu o próximo passo.',
    sub: 'Uma visão viva da sua operação financeira na América Latina — com decisões explicadas, antes que virem urgências.',
    ask: 'Perguntar à Quilla',
    actions: '3 decisões esperam por você',
    view: 'Ver recomendações',
    synthetic: 'DADOS SINTÉTICOS · AMBIENTE DE DEMONSTRAÇÃO',
    demo: 'Iniciar demo',
    welcome: 'A inteligência financeira da América Latina',
    enter: 'Entrar na órbita',
    intro: 'Doze mercados. Uma única visão. Decisões que chegam antes.',
  },
  es: {
    greeting: 'Buenas tardes, Camila.',
    headline: 'Tu dinero está en movimiento.\nQuilla ya vio el próximo paso.',
    sub: 'Una visión viva de tu operación financiera en América Latina — con decisiones explicadas, antes de que se vuelvan urgentes.',
    ask: 'Preguntarle a Quilla',
    actions: '3 decisiones te esperan',
    view: 'Ver recomendaciones',
    synthetic: 'DATOS SINTÉTICOS · AMBIENTE DE DEMOSTRACIÓN',
    demo: 'Iniciar demo',
    welcome: 'La inteligencia financiera de América Latina',
    enter: 'Entrar en órbita',
    intro: 'Doce mercados. Una sola visión. Decisiones que llegan antes.',
  },
}

const tr = (lang: Lang, pt: string, es: string) => lang === 'pt' ? pt : es

function freshnessLabel(lang: Lang, updatedAt: number | null, now: number, loading: boolean, unavailable: boolean): string {
  if (loading) return tr(lang, 'Atualizando dados…', 'Actualizando datos…')
  if (updatedAt === null) return unavailable
    ? tr(lang, 'Atualização indisponível', 'Actualización no disponible')
    : tr(lang, 'Aguardando atualização', 'Esperando actualización')
  const seconds = Math.max(0, Math.floor((now - updatedAt) / 1_000))
  if (seconds < 5) return tr(lang, 'Atualizado agora', 'Actualizado ahora')
  if (seconds < 60) return tr(lang, `Atualizado há ${seconds} segundos`, `Actualizado hace ${seconds} segundos`)
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return tr(lang, `Atualizado há ${minutes} min`, `Actualizado hace ${minutes} min`)
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return tr(lang, `Atualizado há ${hours} h`, `Actualizado hace ${hours} h`)
  const days = Math.floor(hours / 24)
  return tr(lang, `Atualizado há ${days} d`, `Actualizado hace ${days} d`)
}

type UnknownRecord = Record<string, unknown>

const asRecord = (value: unknown): UnknownRecord => value && typeof value === 'object' && !Array.isArray(value) ? value as UnknownRecord : {}
const asRecords = (value: unknown): UnknownRecord[] => Array.isArray(value) ? value.map(asRecord) : []
const firstValue = (record: UnknownRecord, keys: string[]) => keys.map(key => record[key]).find(value => value !== undefined && value !== null)
const firstString = (record: UnknownRecord, keys: string[], fallback = '') => {
  const value = firstValue(record, keys)
  return typeof value === 'string' || typeof value === 'number' ? String(value) : fallback
}
const firstNumber = (record: UnknownRecord, keys: string[]) => {
  const value = firstValue(record, keys)
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : undefined
}
const humanizeKey = (value: string) => value.replaceAll('_', ' ').replace(/\b\w/g, letter => letter.toUpperCase())

const flowNodes = [
  { code: 'MX', x: 35, y: 21, v: 'US$ 32,4M' },
  { code: 'CO', x: 49, y: 42, v: 'US$ 21,8M' },
  { code: 'BR', x: 67, y: 55, v: 'US$ 49,1M' },
  { code: 'PE', x: 43, y: 59, v: 'US$ 14,2M' },
  { code: 'CL', x: 48, y: 78, v: 'US$ 18,7M' },
  { code: 'AR', x: 61, y: 82, v: 'US$ 27,9M' },
]

function BrandMark({ size = 36 }: { size?: number }) {
  return (
    <svg className="brand-mark" width={size} height={size} viewBox="0 0 44 44" aria-label="Quilla">
      <defs>
        <linearGradient id="quilla-gradient" x1="5" y1="4" x2="39" y2="40" gradientUnits="userSpaceOnUse">
          <stop stopColor="#8094ff" />
          <stop offset=".52" stopColor="#5865f2" />
          <stop offset="1" stopColor="#20d3bc" />
        </linearGradient>
      </defs>
      <circle cx="22" cy="22" r="18" fill="none" stroke="url(#quilla-gradient)" strokeWidth="5" />
      <circle cx="28" cy="16" r="13" fill="var(--sidebar)" />
      <path d="M27.5 29.5 38 38" stroke="#20d3bc" strokeWidth="4.5" strokeLinecap="round" />
      <circle cx="10.5" cy="14" r="2" fill="#ff7968" />
    </svg>
  )
}

function Welcome({ lang, onLang, onEnter }: { lang: Lang; onLang: () => void; onEnter: () => void }) {
  const t = copy[lang]
  return (
    <section className="welcome-screen">
      <div className="welcome-noise" />
      <div className="welcome-orbit orbit-one"><i /><i /><i /></div>
      <div className="welcome-orbit orbit-two"><i /><i /></div>
      <button className="welcome-lang" onClick={onLang}>{lang === 'pt' ? 'ES' : 'PT'}</button>
      <div className="welcome-brand"><BrandMark size={48} /><span>quilla</span></div>
      <div className="welcome-copy">
        <span className="overline"><Sparkles size={14} /> {t.welcome}</span>
        <h1>{tr(lang, 'Clareza para', 'Claridad para')}<br /><em>{tr(lang, 'avançar.', 'avanzar.')}</em></h1>
        <p>{t.intro}</p>
        <button className="primary xl" onClick={onEnter}>{t.enter}<ArrowRight size={18} /></button>
      </div>
      <div className="welcome-signal">
        <span className="live-dot" />
        <div><b>Quilla Signal</b><small>{tr(lang, '12 mercados conectados agora', '12 mercados conectados ahora')}</small></div>
        <Activity size={18} />
      </div>
      <p className="powered">DEMO EXPERIENCE <span>×</span> DATABRICKS</p>
    </section>
  )
}

function Panel({ children, className = '' }: { children: ReactNode; className?: string }) {
  return <section className={`panel ${className}`}>{children}</section>
}

function DecisionCard({ recommendation, onClick }: { recommendation: Recommendation; onClick: () => void }) {
  const icons = {
    growth: <BadgeDollarSign size={18} />,
    payments: <RefreshCw size={18} />,
    risk: <CircleDollarSign size={18} />,
  }
  return (
    <button className="decision-card" onClick={onClick}>
      <span className={`decision-icon ${recommendation.tone}`}>{icons[recommendation.category]}</span>
      <span>
        <b>{recommendation.title}</b>
        <small>{recommendation.summary}</small>
        <em>{recommendation.model_status === 'validated_candidate' ? 'MLflow · Unity Catalog · validated' : 'Modelo indisponível'} · {recommendation.country_code}</em>
      </span>
      <span className="decision-action">{recommendation.action}<ArrowUpRight size={14} /></span>
    </button>
  )
}

function RegionalFlow({ lang, compact = false }: { lang: Lang; compact?: boolean }) {
  return (
    <div className={`regional-flow ${compact ? 'compact' : ''}`}>
      <svg viewBox="0 0 100 100" aria-label={tr(lang, 'Fluxos financeiros regionais', 'Flujos financieros regionales')}>
        <defs>
          <linearGradient id="flow-line" x1="0" x2="1"><stop stopColor="#5865f2" /><stop offset="1" stopColor="#20d3bc" /></linearGradient>
          <filter id="glow"><feGaussianBlur stdDeviation="1.2" result="b" /><feMerge><feMergeNode in="b" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
        </defs>
        <path className="land-hint" d="M31 12c9 4 12 13 15 21 5 8 13 12 17 22 5 12 3 26-3 36-7-7-9-18-14-25-7-10-9-19-7-29-5-8-12-15-8-25z" />
        <path className="flow-path p1" d="M35 21 Q51 20 67 55" />
        <path className="flow-path p2" d="M49 42 Q61 55 61 82" />
        <path className="flow-path p3" d="M43 59 Q52 66 48 78" />
        {flowNodes.map((n, i) => <g key={n.code} className="map-node" transform={`translate(${n.x} ${n.y})`} style={{ animationDelay: `${i * 120}ms` }}><circle r="4.8" /><circle r="2" /><text x="6" y="1">{n.code}</text></g>)}
      </svg>
      {!compact && <div className="flow-legend"><span><i className="blue" /> {tr(lang, 'Entradas', 'Entradas')}</span><span><i className="teal" /> {tr(lang, 'Saídas', 'Salidas')}</span><span><i className="coral" /> {tr(lang, 'Atenção', 'Atención')}</span></div>}
    </div>
  )
}

function TechnologyStrip({ lang }: { lang: Lang }) {
  return (
    <div className="tech-strip">
      <span><i className="live-dot" /> {tr(lang, 'Plataforma ativa', 'Plataforma activa')}</span>
      <span><Zap size={13} /> {tr(lang, 'Atualização contínua', 'Actualización continua')}</span>
      <span><Fingerprint size={13} /> {tr(lang, 'Governança em cada decisão', 'Gobernanza en cada decisión')}</span>
      <span className="dbx">Powered by <b>Databricks</b></span>
    </div>
  )
}

function CommandView({ lang, recommendations, loading, error, goIQ, onTour, onExplain, onRecommendations, onRegional, onRetry }: {
  lang: Lang
  recommendations: Recommendation[]
  loading: boolean
  error: string
  goIQ: () => void
  onTour: () => void
  onExplain: (recommendation: Recommendation) => void
  onRecommendations: () => void
  onRegional: () => void
  onRetry: () => void
}) {
  const t = copy[lang]
  const signal = recommendations[0]
  const modelCount = recommendations.filter(item => item.model_status === 'validated_candidate').length
  return (
    <>
      <div className="hero-grid">
        <div className="hero-copy">
          <span className="eyebrow"><Orbit size={14} /> QUİLLA SIGNAL · REGIONAL</span>
          <p className="greeting">{t.greeting}</p>
          <h1>{t.headline.split('\n').map((line, i) => <span key={line} className={i ? 'gradient-text' : ''}>{line}</span>)}</h1>
          <p className="hero-sub">{t.sub}</p>
          <div className="hero-actions">
            <button className="primary" onClick={goIQ}><Sparkles size={17} />{t.ask}</button>
            <button className="secondary" onClick={onTour}><Play size={16} /> {tr(lang, 'Ver história', 'Ver historia')}</button>
          </div>
        </div>
        <Panel className="signal-card">
          <div className="signal-head"><span><Radar size={15} /> {tr(lang, 'PRÓXIMA MELHOR AÇÃO', 'SIGUIENTE MEJOR ACCIÓN')}</span><b>{tr(lang, 'CANDIDATO GOVERNADO', 'CANDIDATO GOBERNADO')}</b></div>
          {loading && !signal && <div className="recommendation-loading"><RefreshCw size={18}/>{tr(lang,'Lendo sinais governados…','Leyendo señales gobernadas…')}</div>}
          {loading && signal && <div className="recommendation-refreshing"><RefreshCw size={13}/>{tr(lang,'Atualizando sem ocultar a última leitura','Actualizando sin ocultar la última lectura')}</div>}
          {!loading && error && !signal && <div className="recommendation-unavailable"><b>{tr(lang,'Feed temporariamente indisponível','Feed temporalmente no disponible')}</b><small>{error}</small><button className="secondary" onClick={onRetry}><RefreshCw size={14}/>{tr(lang,'Tentar novamente','Reintentar')}</button></div>}
          {!loading && error && signal && <div className="recommendation-stale"><Timer size={13}/><span>{tr(lang,'Exibindo a última leitura válida','Mostrando la última lectura válida')}</span><button onClick={onRetry}>{tr(lang,'Atualizar','Actualizar')}</button></div>}
          {signal && <>
            <h3>{signal.title}</h3>
            <p>{signal.summary}</p>
            <div className="impact-row"><span>{tr(lang, 'Evidência de impacto', 'Evidencia de impacto')}</span><strong>{signal.impact}</strong></div>
            <button className="text-action" onClick={()=>onExplain(signal)}>{tr(lang, 'Entender e agir', 'Entender y actuar')} <ArrowRight size={15} /></button>
          </>}
          <div className="signal-orb"><span /><span /><span /></div>
        </Panel>
      </div>

      <div className="metrics-grid">
        <Panel className="decision-metric"><span>{tr(lang,'Candidatos priorizados','Candidatos priorizados')}</span><strong>{recommendations.length || '—'}</strong><small>decision-candidate-v2</small></Panel>
        <Panel className="decision-metric"><span>{tr(lang,'Modelos validados','Modelos validados')}</span><strong>{loading?'—':modelCount}</strong><small>MLflow · Unity Catalog</small></Panel>
        <Panel className="decision-metric"><span>{tr(lang,'Fontes internas','Fuentes internas')}</span><strong>{loading?'—':new Set(recommendations.map(item=>item.internal_source)).size}</strong><small>{tr(lang,'Feature Tables + Gold','Feature Tables + Gold')}</small></Panel>
        <Panel className="decision-metric external"><span>{tr(lang,'Contexto externo','Contexto externo')}</span><strong>LIVE</strong><small>system.ai.web_search · {tr(lang,'sob demanda','bajo demanda')}</small></Panel>
      </div>

      <div className="content-grid">
        <Panel className="decisions-panel">
          <div className="panel-head"><div><span>{tr(lang, 'FEED DE DECISÕES', 'FEED DE DECISIONES')}</span><h2>{recommendations.length ? tr(lang, `${recommendations.length} recomendações esperam por você`, `${recommendations.length} recomendaciones te esperan`) : tr(lang,'Aguardando sinais','Esperando señales')}</h2></div><button disabled={!recommendations.length} onClick={onRecommendations}>{tr(lang,'Analisar conjunto','Analizar conjunto')}<ArrowRight size={15} /></button></div>
          <div className="decision-list">
            {recommendations.map(recommendation=><DecisionCard key={recommendation.recommendation_id} recommendation={recommendation} onClick={()=>onExplain(recommendation)}/>)}
            {!loading && !recommendations.length && <div className="decision-empty">{tr(lang,'Nenhum candidato foi fabricado. Verifique a camada analítica.','No se fabricó ningún candidato. Verifica la capa analítica.')}</div>}
          </div>
        </Panel>
        <Panel className="map-panel">
          <div className="panel-head"><div><span>{tr(lang, 'FLUXOS · 24H', 'FLUJOS · 24H')}</span><h2>{tr(lang, 'América Latina em movimento', 'América Latina en movimiento')}</h2></div><button className="icon-button" onClick={onRegional} aria-label={tr(lang,'Abrir visão regional','Abrir vista regional')}><ArrowUpRight size={17} /></button></div>
          <RegionalFlow lang={lang} compact />
        </Panel>
      </div>
      <TechnologyStrip lang={lang} />
    </>
  )
}

function PersonalView({ lang, country, onAsk }: { lang: Lang; country: string; onAsk: (question: string) => void }) {
  const [profiles, setProfiles] = useState<Customer360[]>([])
  const [profileId, setProfileId] = useState('')
  const [selectorOpen, setSelectorOpen] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [source, setSource] = useState('')
  useEffect(()=>{
    let active=true
    setLoading(true)
    getCustomerProfiles(country).then(feed=>{
      if(!active)return
      setProfiles(feed.customers)
      setProfileId(feed.customers[0]?.party_id||'')
      setSource(`${feed.source} · ${feed.statement_id.slice(0,8)}`)
      setError('')
    }).catch(reason=>{if(active){setProfiles([]);setProfileId('');setError(reason instanceof Error?reason.message:'Customer 360 unavailable')}})
      .finally(()=>{if(active)setLoading(false)})
    return()=>{active=false}
  },[country])
  const profile = profiles.find(item=>item.party_id===profileId) || profiles[0]
  const locale = lang==='pt'?'pt-BR':'es-419'
  const money = (value:number,currency:string)=>new Intl.NumberFormat(locale,{style:'currency',currency,maximumFractionDigits:2}).format(value)
  const number = (value:number)=>new Intl.NumberFormat(locale,{notation:'compact',maximumFractionDigits:1}).format(value)
  const date = (value?:string|null)=>value?new Date(value).toLocaleDateString(locale,{day:'2-digit',month:'short',year:'numeric'}):'—'
  const initials = (name:string)=>name.split(' ').slice(0,2).map(part=>part[0]).join('').toUpperCase()
  const ask = (purposePt:string,purposeEs:string) => {
    if(!profile)return
    onAsk(tr(lang,
      `${purposePt} para o cliente sintético ${profile.display_name}, usando obrigatoriamente party_id = '${profile.party_id}' como filtro exato. Não liste outros clientes nem exponha contatos ou documentos.`,
      `${purposeEs} para el cliente sintético ${profile.display_name}, usando obligatoriamente party_id = '${profile.party_id}' como filtro exacto. No listes otros clientes ni expongas contactos o documentos.`
    ))
  }
  if(loading)return <div className="customer-state"><RefreshCw size={18}/>{tr(lang,'Carregando Customer 360 governado…','Cargando Customer 360 gobernado…')}</div>
  if(error||!profile)return <div className="genie-error">{tr(lang,'Customer 360 indisponível: ','Customer 360 no disponible: ')}{error}</div>
  const liquidityRatio = profile.current_balance_local>0?Math.min(100,Math.round(100*profile.available_balance_local/profile.current_balance_local)):0
  const riskSignal = Math.min(100,Math.round(profile.average_event_risk_score*100))
  const sentimentSignal = Math.min(100,Math.max(0,Math.round((profile.average_sentiment_score+1)*50)))
  return (
    <>
      <div className="page-heading"><div><span className="eyebrow"><UserRound size={14} /> QUILLA PERSONAL · CUSTOMER 360</span><h1 className="page-title">{tr(lang, 'Cada cliente,', 'Cada cliente,')}<br /><span className="gradient-text">{tr(lang, 'uma próxima melhor ação.', 'una siguiente mejor acción.')}</span></h1><p className="page-sub">{tr(lang,'Sinais financeiros, relacionamento e risco reunidos com governança.','Señales financieras, relación y riesgo reunidos con gobierno.')}</p></div><div className="persona-selector"><button className="persona-chip" onClick={()=>setSelectorOpen(open=>!open)} aria-expanded={selectorOpen}><div className="avatar">{initials(profile.display_name)}</div><div><b>{profile.display_name}</b><small>{profile.country_code} · {profile.party_id}</small></div><ChevronDown size={15} /></button>{selectorOpen && <div className="persona-menu">{profiles.map(item=><button key={item.party_id} className={item.party_id===profile.party_id?'active':''} onClick={()=>{setProfileId(item.party_id);setSelectorOpen(false)}}><div className="avatar">{initials(item.display_name)}</div><span><b>{item.display_name}</b><small>{item.country_code} · {item.customer_segment}</small></span>{item.party_id===profile.party_id&&<Check size={14}/>}</button>)}</div>}</div></div>
      <div className="personal-grid">
        <Panel className="balance-card">
          <div className="balance-top"><span>{tr(lang, 'Saldo atual consolidado', 'Saldo actual consolidado')}</span><Eye size={18} /></div><strong>{money(profile.current_balance_local,profile.currency_code)}</strong><small>{profile.currency_code} · {tr(lang,'cliente verificado · dados sintéticos','cliente verificado · datos sintéticos')}</small>
          <div className="customer-kpi-row"><span><small>{tr(lang,'Disponível','Disponible')}</small><b>{money(profile.available_balance_local,profile.currency_code)}</b></span><span><small>{tr(lang,'Volume histórico','Volumen histórico')}</small><b>{money(profile.lifetime_volume_usd,'USD')}</b></span><span><small>{tr(lang,'Contas','Cuentas')}</small><b>{profile.account_count}</b></span></div>
          <div className="balance-actions"><button onClick={()=>ask('Analise opções seguras de transferência e remessa transfronteiriça','Analiza opciones seguras de transferencia y remesa transfronteriza')}><Send size={17} />{tr(lang, 'Transferir', 'Transferir')}</button><button onClick={()=>ask('Analise liquidez, saldos e necessidade de câmbio','Analiza liquidez, saldos y necesidad de cambio')}><RefreshCw size={17} />{tr(lang, 'Liquidez', 'Liquidez')}</button><button onClick={()=>ask('Analise uso de contas, produtos e oportunidades adequadas ao perfil','Analiza uso de cuentas, productos y oportunidades adecuadas al perfil')}><WalletCards size={17} />{tr(lang, 'Produtos', 'Productos')}</button></div>
        </Panel>
        <Panel className="customer-intelligence"><div className="panel-head"><div><span>QUILLA SIGNAL</span><h2>{tr(lang, 'Inteligência do relacionamento', 'Inteligencia de la relación')}</h2></div><b className="score-good">{profile.kyc_status.toUpperCase()}</b></div><div className="customer-tags"><span>{tr(lang,'Risco','Riesgo')}: <b>{profile.risk_band}</b></span><span>{tr(lang,'Segmento','Segmento')}: <b>{profile.customer_segment}</b></span><span>{tr(lang,'Alertas abertos','Alertas abiertos')}: <b>{profile.open_risk_alert_count}</b></span></div><div className="signal-bars"><div><span>{tr(lang,'Liquidez disponível','Liquidez disponible')}<b>{liquidityRatio}%</b></span><i><em style={{width:`${liquidityRatio}%`}}/></i></div><div><span>{tr(lang,'Sinal médio de risco','Señal promedio de riesgo')}<b>{riskSignal}%</b></span><i><em className="coral" style={{width:`${riskSignal}%`}}/></i></div><div><span>{tr(lang,'Sinal de sentimento','Señal de sentimiento')}<b>{sentimentSignal}%</b></span><i><em className="blue" style={{width:`${sentimentSignal}%`}}/></i></div></div><button className="text-action" onClick={()=>ask('Produza uma análise Customer 360 explicável e recomende próximos passos','Produce un análisis Customer 360 explicable y recomienda próximos pasos')}>{tr(lang,'Gerar análise 360','Generar análisis 360')}<ArrowRight size={15}/></button></Panel>
        <Panel className="timeline-card"><div className="panel-head"><div><span>{tr(lang, 'ATIVIDADE GOVERNADA', 'ACTIVIDAD GOBERNADA')}</span><h2>{tr(lang, 'Linha do tempo do relacionamento', 'Línea de tiempo de la relación')}</h2></div><small className="customer-source">{source}</small></div><div className="timeline"><span><i className="teal" /><b>{tr(lang, 'Último evento', 'Último evento')}</b><small>{date(profile.last_event_at)} · {number(profile.lifetime_event_count)} {tr(lang,'eventos','eventos')}</small></span><span><i className="blue" /><b>{tr(lang, 'Última interação', 'Última interacción')}</b><small>{date(profile.last_interaction_at)} · {number(profile.interaction_count)} {tr(lang,'interações','interacciones')}</small></span><span><i className="coral" /><b>{tr(lang, 'Relacionamento desde', 'Relación desde')}</b><small>{date(profile.onboarding_ts)} · {profile.risk_alert_count} {tr(lang,'alertas no histórico','alertas históricos')}</small></span></div></Panel>
        <Panel className="customer-actions"><div className="panel-head"><div><span>NEXT BEST ACTION</span><h2>{tr(lang,'Investigações recomendadas','Investigaciones recomendadas')}</h2></div></div><div><button onClick={()=>ask('Compare o comportamento transacional recente com clientes do mesmo segmento sem revelar outras identidades','Compara el comportamiento transaccional reciente con clientes del mismo segmento sin revelar otras identidades')}><Activity size={18}/><span><b>{tr(lang,'Comportamento','Comportamiento')}</b><small>{tr(lang,'Padrões, frequência e anomalias','Patrones, frecuencia y anomalías')}</small></span><ArrowUpRight size={16}/></button><button onClick={()=>ask('Revise os alertas de risco e explique os fatores permitidos para este cliente','Revisa las alertas de riesgo y explica los factores permitidos para este cliente')}><ShieldCheck size={18}/><span><b>{tr(lang,'Risco explicável','Riesgo explicable')}</b><small>{tr(lang,'Alertas, sinais e revisão humana','Alertas, señales y revisión humana')}</small></span><ArrowUpRight size={16}/></button><button onClick={()=>ask('Avalie a próxima melhor ação e compare não agir, educar, proteger e ofertar','Evalúa la siguiente mejor acción y compara no actuar, educar, proteger y ofrecer')}><Sparkles size={18}/><span><b>{tr(lang,'Próxima melhor ação','Siguiente mejor acción')}</b><small>{tr(lang,'Valor, adequação e alternativas','Valor, adecuación y alternativas')}</small></span><ArrowUpRight size={16}/></button></div></Panel>
      </div>
    </>
  )
}

function BusinessView({ lang, country, onAnalyze }: { lang: Lang; country: string; onAnalyze: () => void }) {
  const [overview, setOverview] = useState<BusinessOverview>()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    getBusinessOverview(country, lang)
      .then(value => { if (active) setOverview(value) })
      .catch(reason => { if (active) { setOverview(undefined); setError(reason instanceof Error ? reason.message : 'Business analytics unavailable') } })
      .finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [country, lang])

  const payload = asRecord(overview)
  const kpis = asRecord(payload.kpis)
  const trend = asRecords(payload.trend)
  const receivables = asRecords(payload.receivables)
  const locale = lang === 'pt' ? 'pt-BR' : 'es-419'
  const money = (value?: number) => value === undefined ? '—' : new Intl.NumberFormat(locale, { style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1 }).format(value)
  const percent = (value?: number) => value === undefined ? '—' : `${new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(value)}%`
  const values = trend.map(item => firstNumber(item, ['net_flow_usd']) || 0)
  const min = values.length ? Math.min(...values) : 0
  const max = values.length ? Math.max(...values) : 0
  const range = Math.max(1, max - min)
  const points = values.map((value, index) => `${values.length === 1 ? 300 : index * 600 / (values.length - 1)},${185 - ((value - min) / range) * 155}`).join(' ')
  const maxDeclined = Math.max(1, ...receivables.map(item => firstNumber(item, ['declined_volume_usd']) || 0))
  const generatedAt = firstString(payload, ['generated_at'])
  const source = firstString(payload, ['source'])

  return (
    <>
      <div className="page-heading"><div><span className="eyebrow"><BriefcaseBusiness size={14} /> QUILLA EMPRESAS · LIVE ANALYTICS</span><h1 className="page-title">{tr(lang, 'O pulso financeiro', 'El pulso financiero')}<br />{tr(lang, 'do seu negócio.', 'de tu negocio.')}</h1><p className="page-sub">{tr(lang,'Indicadores calculados sob demanda a partir da camada Gold governada.','Indicadores calculados bajo demanda desde la capa Gold gobernada.')}</p></div><button className="primary" disabled={!overview} onClick={onAnalyze}><Sparkles size={16} /> {tr(lang, 'Investigar no Genie', 'Investigar en Genie')}</button></div>
      {loading && <div className="business-state"><RefreshCw size={18}/>{tr(lang,'Consultando métricas de negócio…','Consultando métricas de negocio…')}</div>}
      {!loading && error && <div className="genie-error">{tr(lang,'Visão de negócios indisponível: ','Vista de negocio no disponible: ')}{error}</div>}
      {!loading && overview && <>
        <div className="business-kpis business-kpis-live">
          <div><small>{tr(lang, 'Volume bruto', 'Volumen bruto')}</small><strong>{money(firstNumber(kpis,['gross_volume_usd']))}</strong><span>USD</span></div>
          <div><small>{tr(lang, 'Fluxo líquido', 'Flujo neto')}</small><strong>{money(firstNumber(kpis,['net_flow_usd']))}</strong><span>{tr(lang,'período consultado','período consultado')}</span></div>
          <div><small>{tr(lang, 'Taxa de conclusão', 'Tasa de finalización')}</small><strong>{percent(firstNumber(kpis,['completion_rate_pct']))}</strong><span>{tr(lang,'eventos processados','eventos procesados')}</span></div>
          <div><small>{tr(lang, 'Volume recusado', 'Volumen rechazado')}</small><strong>{money(firstNumber(kpis,['declined_volume_usd']))}</strong><span className="warning">{tr(lang,'atenção operacional','atención operativa')}</span></div>
          <div><small>{tr(lang, 'Receita · 90d', 'Ingresos · 90d')}</small><strong>{money(firstNumber(kpis,['revenue_usd_90d']))}</strong><span>Metric View</span></div>
          <div><small>{tr(lang, 'Margem · 90d', 'Margen · 90d')}</small><strong>{money(firstNumber(kpis,['margin_usd_90d']))}</strong><span>Metric View</span></div>
        </div>
        <div className="content-grid business-content business-content-live">
          <Panel className="cash-panel"><div className="panel-head"><div><span>{tr(lang, 'SÉRIE DIÁRIA GOVERNADA', 'SERIE DIARIA GOBERNADA')}</span><h2>{tr(lang, 'Fluxo líquido diário', 'Flujo neto diario')}</h2></div><div className="legend-inline"><i className="teal" /> {tr(lang,'Fluxo líquido','Flujo neto')}</div></div>
            {trend.length ? <div className="business-live-chart"><svg viewBox="0 0 600 210" preserveAspectRatio="none" aria-label={tr(lang,'Série diária do fluxo líquido','Serie diaria del flujo neto')}><defs><linearGradient id="business-live-area" x1="0" y1="0" x2="0" y2="1"><stop stopColor="#20d3bc" stopOpacity=".34"/><stop offset="1" stopColor="#20d3bc" stopOpacity="0"/></linearGradient></defs><polygon points={`0,210 ${points} 600,210`} fill="url(#business-live-area)"/><polyline points={points} /></svg><div className="business-chart-axis"><span>{firstString(trend[0],['event_date'])}</span><strong>{money(values[values.length-1])}</strong><span>{firstString(trend[trend.length-1],['event_date'])}</span></div></div> : <div className="business-empty">{tr(lang,'A API não retornou série diária para este filtro.','La API no devolvió una serie diaria para este filtro.')}</div>}
          </Panel>
          <Panel className="receivables"><div className="panel-head"><div><span>{tr(lang, 'RECUSAS POR PAÍS', 'RECHAZOS POR PAÍS')}</span><h2>{tr(lang, 'Onde investigar primeiro', 'Dónde investigar primero')}</h2></div></div>{receivables.length ? receivables.map(item=>{const code=firstString(item,['country_code'],'—');const declined=firstNumber(item,['declined_volume_usd'])||0;return <div className="country-row" key={code}><span><i>{code}</i>{code}</span><b>{money(declined)}</b><div><em style={{width:`${Math.max(4,declined/maxDeclined*100)}%`}} /></div><small>{new Intl.NumberFormat(locale,{notation:'compact'}).format(firstNumber(item,['declined_events'])||0)} {tr(lang,'eventos','eventos')}</small></div>}) : <div className="business-empty">{tr(lang,'Sem recusas retornadas para este filtro.','Sin rechazos devueltos para este filtro.')}</div>}</Panel>
        </div>
        <small className="business-source"><Database size={12}/> {source || tr(lang,'Fonte informada pela API','Fuente informada por la API')}{generatedAt && ` · ${new Date(generatedAt).toLocaleString(locale)}`}</small>
      </>}
    </>
  )
}

function RegionalView({ lang, onAnalyze }: { lang: Lang; onAnalyze: (countryCode: string) => void }) {
  const [overview, setOverview] = useState<RegionalOverview>()
  const [error, setError] = useState('')
  useEffect(() => {
    let active = true
    getRegionalOverview().then(result => { if (active) { setOverview(result); setError('') } })
      .catch(reason => { if (active) setError(reason instanceof Error ? reason.message : 'Analytics unavailable') })
    return () => { active = false }
  }, [])
  const names: Record<string,string> = {BR:'Brasil',MX:'México',CO:tr(lang,'Colômbia','Colombia'),AR:'Argentina',CL:'Chile',PE:tr(lang,'Peru','Perú'),EC:tr(lang,'Equador','Ecuador'),UY:tr(lang,'Uruguai','Uruguay'),CR:'Costa Rica',PA:tr(lang,'Panamá','Panamá'),GT:'Guatemala',DO:tr(lang,'República Dominicana','República Dominicana')}
  const money = (value: number) => new Intl.NumberFormat(lang==='pt'?'pt-BR':'es-419',{style:'currency',currency:'USD',notation:'compact',maximumFractionDigits:1}).format(value)
  return (
    <div className="regional-page">
      <div className="page-heading"><div><span className="eyebrow"><Globe2 size={14} /> QUILLA REGIONAL · METRIC VIEW</span><h1 className="page-title">{tr(lang, 'Uma região.', 'Una región.')}<br /><span className="gradient-text">{tr(lang, 'Doze realidades.', 'Doce realidades.')}</span></h1><p className="page-sub">{tr(lang, 'Uma visão comum, respeitando moedas, hábitos e produtos de cada mercado.', 'Una visión común que respeta las monedas, hábitos y productos de cada mercado.')}</p></div><div className="regional-total"><small>{tr(lang, 'VOLUME GOVERNADO', 'VOLUMEN GOBERNADO')}</small><strong>{overview?money(overview.gross_volume_usd):'—'}</strong><span>{overview?`${new Intl.NumberFormat(lang==='pt'?'pt-BR':'es-419',{notation:'compact'}).format(overview.total_events)} ${tr(lang,'eventos','eventos')}`:tr(lang,'Carregando Metric View…','Cargando Metric View…')}</span></div></div>
      {error && <div className="genie-error">{tr(lang,'Não foi possível consultar a Metric View: ','No fue posible consultar la Metric View: ')}{error}</div>}
      <div className="regional-layout">
        <Panel className="regional-map-large"><RegionalFlow lang={lang} /><div className="map-callout"><span className="live-dot" /><div><small>{tr(lang, 'ANÁLISE DISPONÍVEL', 'ANÁLISIS DISPONIBLE')}</small><b>{tr(lang, 'Selecione um mercado', 'Selecciona un mercado')}</b><em>{tr(lang,'Genie investiga os dados governados','Genie investiga los datos gobernados')}</em></div></div></Panel>
        <Panel className="market-list live"><div className="panel-head"><div><span>METRIC VIEW</span><h2>{tr(lang, 'Performance local', 'Desempeño local')}</h2></div></div>{overview?.countries.map(metric=><button key={metric.country_code} onClick={()=>onAnalyze(metric.country_code)}><i>{metric.country_code}</i><span><b>{names[metric.country_code]||metric.country_code}</b><small>{new Intl.NumberFormat(lang==='pt'?'pt-BR':'es-419',{notation:'compact'}).format(metric.total_events)} {tr(lang,'eventos','eventos')} · {metric.completion_rate_pct.toFixed(1)}%</small></span><strong>{money(metric.gross_volume_usd)}</strong><ArrowUpRight size={14}/></button>)}{overview&&<small className="analytics-source">Unity Catalog · {overview.source} · {overview.statement_id.slice(0,8)}</small>}</Panel>
      </div>
    </div>
  )
}

function OperationsView({ lang }: { lang: Lang }) {
  const [overview, setOverview] = useState<OperationsOverview>()
  const [overviewError, setOverviewError] = useState('')
  const [scenario, setScenario] = useState<'normal'|'fraud_burst'|'cross_border'|'dq_violations'|'mixed'>('mixed')
  const [eventCount, setEventCount] = useState(1000)
  const [run, setRun] = useState<RealtimeRun>()
  const [runError, setRunError] = useState('')
  const [starting, setStarting] = useState(false)

  useEffect(() => {
    let active = true
    getOperationsOverview().then(value=>{if(active){setOverview(value);setOverviewError('')}})
      .catch(reason=>{if(active)setOverviewError(reason instanceof Error?reason.message:'Operations overview unavailable')})
    return()=>{active=false}
  }, [])

  const runRecord = asRecord(run)
  const runId = firstNumber(runRecord,['run_id'])
  const state = firstString(runRecord,['state','life_cycle_state','result_state']).toUpperCase()
  const terminal = ['SUCCESS','SUCCEEDED','FAILED','ERROR','CANCELED','CANCELLED','TERMINATED','SKIPPED'].some(value=>state.includes(value))

  useEffect(() => {
    if (!runId || terminal) return
    let active = true
    const timer = window.setInterval(() => {
      getRealtimeSimulation(runId).then(value=>{if(active){setRun(value);setRunError('')}})
        .catch(reason=>{if(active)setRunError(reason instanceof Error?reason.message:'Realtime status unavailable')})
    }, 4000)
    return()=>{active=false;window.clearInterval(timer)}
  }, [runId, terminal])

  const start = async () => {
    setStarting(true)
    setRunError('')
    try { setRun(await startRealtimeSimulation({event_count:eventCount,scenario})) }
    catch(reason){setRunError(reason instanceof Error?reason.message:'Unable to start realtime simulation')}
    finally{setStarting(false)}
  }

  const operations = asRecord(overview)
  const controls = asRecords(operations.controls)
  const models = asRecord(operations.models)
  const quality = asRecord(operations.data_quality)
  const realtime = asRecord(operations.realtime)
  const dashboards = overview?.dashboards || []
  const tasks = asRecords(runRecord.tasks)
  const evidence = asRecord(runRecord.evidence)
  const scenarios = [
    ['normal',tr(lang,'Operação normal','Operación normal')],
    ['fraud_burst',tr(lang,'Explosão de fraude','Explosión de fraude')],
    ['cross_border',tr(lang,'Fluxo transfronteiriço','Flujo transfronterizo')],
    ['dq_violations',tr(lang,'Violações de qualidade','Violaciones de calidad')],
    ['mixed',tr(lang,'Cenário misto','Escenario mixto')],
  ] as const

  return <div className="operations-page">
    <div className="page-heading"><div><span className="eyebrow"><Radio size={14}/> ZEROBUS · REAL-TIME CONTROL ROOM</span><h1 className="page-title">{tr(lang,'Da chegada do evento','Desde la llegada del evento')}<br/><span className="gradient-text">{tr(lang,'à decisão governada.','hasta la decisión gobernada.')}</span></h1><p className="page-sub">{tr(lang,'Dispare uma execução real sob demanda e acompanhe cada etapa do medallion.','Inicia una ejecución real bajo demanda y sigue cada etapa del medallion.')}</p></div><span className="operations-mode"><Timer size={15}/>{tr(lang,'Sob demanda','Bajo demanda')}</span></div>
    {overviewError && <div className="genie-error">{overviewError}</div>}
    <div className="operations-overview">
      <Panel><span>MLFLOW</span><strong>{overview?`${firstNumber(models,['validated'])??'—'} / ${firstNumber(models,['total'])??'—'}`:'—'}</strong><small>{tr(lang,'modelos validados','modelos validados')}</small></Panel>
      <Panel className="dq-review"><span>DQX · {tr(lang,'ACEITAÇÃO','ACEPTACIÓN')}</span><strong>{overview?`${firstNumber(quality,['acceptance_rate_pct'])?.toFixed(1)??'—'}%`:'—'}</strong><small>{overview?`${firstNumber(quality,['reconciled_datasets'])??'—'}/${firstNumber(quality,['datasets'])??'—'} ${tr(lang,'reconciliados · meta de aceitação requer definição','reconciliados · la meta de aceptación requiere definición')}`:tr(lang,'Aguardando API','Esperando API')}</small></Panel>
      <Panel><span>REALTIME</span><strong>{overview?firstString(realtime,['realtime_status'],'—'):'—'}</strong><small>{overview?`${firstString(realtime,['scenario'],'—')} · ${firstString(realtime,['simulation_id'],'—')}`:tr(lang,'Aguardando API','Esperando API')}</small></Panel>
      <Panel><span>{tr(lang,'CONTROLES','CONTROLES')}</span><strong>{overview?controls.length:'—'}</strong><small>{tr(lang,'retornados pela API','devueltos por la API')}</small></Panel>
    </div>
    <div className="operations-grid">
      <Panel className="realtime-console"><div className="panel-head"><div><span>ZEROBUS + LAKEFLOW SDP</span><h2>{tr(lang,'Nova simulação de eventos','Nueva simulación de eventos')}</h2></div><Radio size={20}/></div>
        <div className="realtime-form"><label><span>{tr(lang,'Cenário','Escenario')}</span><select value={scenario} disabled={starting||(!terminal&&!!runId)} onChange={event=>setScenario(event.target.value as typeof scenario)}>{scenarios.map(([value,label])=><option key={value} value={value}>{label}</option>)}</select></label><label><span>{tr(lang,'Volume de eventos','Volumen de eventos')}</span><input type="number" min="100" max="5000" step="100" value={eventCount} disabled={starting||(!terminal&&!!runId)} onChange={event=>setEventCount(Math.min(5000,Math.max(100,Number(event.target.value)||100)))}/></label><button className="primary" disabled={starting||(!terminal&&!!runId)} onClick={()=>void start()}>{starting?<RefreshCw size={16}/>:<Play size={16}/>} {starting?tr(lang,'Iniciando…','Iniciando…'):tr(lang,'Iniciar fluxo','Iniciar flujo')}</button></div>
        {runError&&<div className="realtime-error">{runError}</div>}
        {!run && !runError && <div className="realtime-idle"><Radio size={28}/><b>{tr(lang,'Nenhuma execução iniciada','Ninguna ejecución iniciada')}</b><small>{tr(lang,'Escolha cenário e volume para acionar a API.','Elige escenario y volumen para activar la API.')}</small></div>}
        {run&&<div className="realtime-run"><div className="realtime-run-head"><span><small>RUN ID</small><b>{runId||'—'}</b></span><em className={terminal?(state.includes('SUCCESS')?'success':'error'):'running'}>{!terminal&&<RefreshCw size={13}/>} {state||tr(lang,'INICIANDO','INICIANDO')}</em></div><div className="realtime-timeline">{tasks.length?tasks.map((task,index)=>{const taskState=firstString(task,['result_state','life_cycle_state','state','status'],'PENDING');const taskOk=taskState.toUpperCase().includes('SUCCESS');return <div key={firstString(task,['task_key','name'],String(index))} className={taskState.toLowerCase()}><i>{taskOk?<Check size={13}/>:<span>{index+1}</span>}</i><span><b>{firstString(task,['task_key','name'],`${tr(lang,'Etapa','Etapa')} ${index+1}`)}</b><small>{taskState}</small></span></div>}) : <p>{tr(lang,'Aguardando as etapas retornadas pela execução…','Esperando las etapas devueltas por la ejecución…')}</p>}</div>{terminal&&Object.keys(evidence).length>0&&<div className="realtime-evidence"><h3>{tr(lang,'Evidência da execução','Evidencia de la ejecución')}</h3>{Object.entries(evidence).map(([key,value])=><span key={key}><small>{humanizeKey(key)}</small><b>{typeof value==='object'?JSON.stringify(value):String(value)}</b></span>)}</div>}</div>}
      </Panel>
      <Panel className="platform-controls"><div className="panel-head"><div><span>{tr(lang,'PLATAFORMA VERIFICADA','PLATAFORMA VERIFICADA')}</span><h2>{tr(lang,'Controles operacionais','Controles operativos')}</h2></div><ServerCog size={20}/></div>{controls.length?controls.map((control,index)=>{const state=firstString(control,['state','status']).toLowerCase();return <div className="platform-control" key={firstString(control,['key','name'],String(index))}><span className={state}>{state==='requires_review'?<Timer size={13}/>:state==='operational'||state==='configured'||state==='ready'?<Check size={13}/>:<X size={13}/>}</span><div><b>{firstString(control,['name','key'],tr(lang,'Controle','Control'))}</b><small>{firstString(control,['evidence','detail','status','state'],'—')}</small></div></div>}):<div className="business-empty">{tr(lang,'Aguardando overview de operações.','Esperando el overview de operaciones.')}</div>}</Panel>
    </div>
    <div className="dashboard-section"><div className="panel-head"><div><span>LAKEVIEW · PUBLISHED</span><h2>{tr(lang,'Dashboards executivos','Dashboards ejecutivos')}</h2></div></div><div className="dashboard-links">{dashboards.map(item=>{const title=lang==='pt'?item.title_pt:item.title_es;return <a key={item.key} href={item.url} target="_blank" rel="noreferrer"><span><LineChart size={18}/></span><div><b>{title}</b><small>{tr(lang,'Publicado e governado no Lakeview','Publicado y gobernado en Lakeview')}</small></div><ExternalLink size={15}/></a>})}</div></div>
  </div>
}

function TrustView({ lang }: { lang: Lang }) {
  const [status, setStatus] = useState<TrustStatus>()
  const [operations, setOperations] = useState<OperationsOverview>()
  const [error, setError] = useState('')
  const [operationsError, setOperationsError] = useState('')
  useEffect(()=>{let active=true;getTrustStatus().then(value=>{if(active){setStatus(value);setError('')}}).catch(reason=>{if(active)setError(reason instanceof Error?reason.message:'Unavailable')});getOperationsOverview().then(value=>{if(active){setOperations(value);setOperationsError('')}}).catch(reason=>{if(active)setOperationsError(reason instanceof Error?reason.message:'Operations unavailable')});return()=>{active=false}},[])
  const labels: Record<string,[typeof Fingerprint,string,string]> = {
    lakebase:[Database,'Lakebase',tr(lang,'Memória e estado LTAP','Memoria y estado LTAP')],
    supervisor:[Sparkles,tr(lang,'Supervisor custom','Supervisor custom'),tr(lang,'Endpoint de agentes','Endpoint de agentes')],
    genie:[LineChart,'Genie Agent',tr(lang,'Spaces governados','Spaces gobernados')],
    warehouse:[Activity,'SQL Warehouse',tr(lang,'Camada analítica','Capa analítica')],
    gateway:[ShieldCheck,'Unity AI Gateway',tr(lang,'Model Services e políticas','Model Services y políticas')],
  }
  const verified=status?.controls.filter(control=>control.state==='operational'||control.state==='configured').length||0
  const total=status?.controls.length||0
  const operationRecord=asRecord(operations)
  const models=asRecord(operationRecord.models)
  const quality=asRecord(operationRecord.data_quality)
  const realtime=asRecord(operationRecord.realtime)
  const dashboards=asRecords(operationRecord.dashboards)
  return (
    <>
      <div className="page-heading"><div><span className="eyebrow"><ShieldCheck size={14} /> QUILLA TRUST · LIVE</span><h1 className="page-title">{tr(lang, 'Confiança que você', 'Confianza que')}<br />{tr(lang, 'consegue enxergar.', 'puedes ver.')}</h1><p className="page-sub">{tr(lang, 'Estado verificado dos componentes da demonstração, sem métricas decorativas.', 'Estado verificado de los componentes de la demostración, sin métricas decorativas.')}</p></div><div className="trust-status"><span className={total>0&&verified===total?'live-dot':'status-warning'}/><div><b>{status?`${verified}/${total} ${tr(lang,'controles verificados','controles verificados')}`:tr(lang,'Verificando ambiente…','Verificando el entorno…')}</b><small>{status?new Date(status.checked_at).toLocaleTimeString(lang==='pt'?'pt-BR':'es-419'):''}</small></div></div></div>
      {error&&<div className="genie-error">{error}</div>}
      {operationsError&&<div className="genie-error">{tr(lang,'Observabilidade avançada indisponível: ','Observabilidad avanzada no disponible: ')}{operationsError}</div>}
      <div className="trust-grid">
        <Panel className="trust-score"><div className="panel-head"><div><span>{tr(lang, 'EVIDÊNCIA OPERACIONAL', 'EVIDENCIA OPERATIVA')}</span><h2>{tr(lang, 'Componentes verificados', 'Componentes verificados')}</h2></div><ShieldCheck size={20}/></div><div className="trust-number">{status?verified:'—'}<small>/{status?total:'—'}</small></div><div className="trust-bar"><i style={{width:`${total?verified/total*100:0}%`}} /></div><p>{tr(lang,'Estados obtidos do ambiente nesta sessão. “Configurado” não significa probe operacional.','Estados obtenidos del entorno en esta sesión. “Configurado” no equivale a una prueba operativa.')}</p></Panel>
        <Panel className="control-center"><div className="panel-head"><div><span>{tr(lang, 'CONTROLES DO AMBIENTE', 'CONTROLES DEL ENTORNO')}</span><h2>{tr(lang, 'Evidência verificável', 'Evidencia verificable')}</h2></div></div>{status?.controls.map(control=>{const [Icon,title,desc]=labels[control.key]||[ShieldCheck,control.key,''];return <div className={`control-row ${control.state}`} key={control.key}><Icon size={17}/><span><b>{title} · {control.state}</b><small>{desc} — {control.evidence}</small></span>{control.state==='operational'||control.state==='configured'?<Check size={15}/>:<X size={15}/>}</div>})}</Panel>
        <Panel className="observability-panel"><div className="panel-head"><div><span>MLFLOW · MODELS</span><h2>{tr(lang,'Ciclo de vida dos modelos','Ciclo de vida de los modelos')}</h2></div><Gauge size={20}/></div>{operations?<><strong>{firstNumber(models,['validated'])??'—'}<small> / {firstNumber(models,['total'])??'—'}</small></strong><p>{tr(lang,'modelos validados no MLflow e registrados no Unity Catalog','modelos validados en MLflow y registrados en Unity Catalog')}</p></>:<div className="business-empty">{tr(lang,'Verificando MLflow…','Verificando MLflow…')}</div>}</Panel>
        <Panel className="observability-panel dq-review"><div className="panel-head"><div><span>DQX · DATA QUALITY</span><h2>{tr(lang,'Aceitação requer revisão','La aceptación requiere revisión')}</h2></div><Fingerprint size={20}/></div>{operations?<><strong>{firstNumber(quality,['acceptance_rate_pct'])?.toFixed(1)??'—'}<small>%</small></strong><p>{firstNumber(quality,['reconciled_datasets'])??'—'}/{firstNumber(quality,['datasets'])??'—'} {tr(lang,'datasets reconciliados; todas as linhas contabilizadas, não necessariamente aprovadas.','datasets reconciliados; todas las filas contabilizadas, no necesariamente aprobadas.')} {firstNumber(quality,['quarantine_rows'])??'—'} {tr(lang,'linhas em quarentena na fotografia materializada atual. Meta contratual ainda não definida.','filas en cuarentena en la fotografía materializada actual. Meta contractual aún no definida.')}</p></>:<div className="business-empty">{tr(lang,'Verificando DQX…','Verificando DQX…')}</div>}</Panel>
        <Panel className="observability-panel"><div className="panel-head"><div><span>ZEROBUS · REALTIME</span><h2>{tr(lang,'Fluxo de eventos','Flujo de eventos')}</h2></div><Radio size={20}/></div>{operations?<><strong className="textual">{firstString(realtime,['realtime_status'],'—')}</strong><p>{firstString(realtime,['scenario'],'—')} · {firstString(realtime,['reconciled_distinct_events'],'—')} {tr(lang,'eventos reconciliados','eventos reconciliados')}</p></>:<div className="business-empty">{tr(lang,'Verificando realtime…','Verificando realtime…')}</div>}</Panel>
        <Panel className="observability-links"><div className="panel-head"><div><span>{tr(lang,'LINKS OPERACIONAIS','ENLACES OPERATIVOS')}</span><h2>{tr(lang,'Abrir evidências','Abrir evidencias')}</h2></div><ExternalLink size={20}/></div>{dashboards.length?dashboards.map((dashboard,index)=>{const url=firstString(dashboard,['url','href']);const title=firstString(dashboard,[lang==='pt'?'title_pt':'title_es','title_pt','title_es'],`${tr(lang,'Dashboard','Dashboard')} ${index+1}`);return url?<a key={`${title}-${index}`} href={url} target="_blank" rel="noreferrer"><LineChart size={15}/><span><b>{title}</b><small>Lakeview · {tr(lang,'publicado','publicado')}</small></span><ExternalLink size={13}/></a>:null}):<div className="business-empty">{operations?tr(lang,'A API não retornou links operacionais.','La API no devolvió enlaces operativos.'):tr(lang,'Verificando links…','Verificando enlaces…')}</div>}</Panel>
        <Panel className="lineage-panel"><div className="panel-head"><div><span>{tr(lang, 'DA PERGUNTA À DECISÃO', 'DE LA PREGUNTA A LA DECISIÓN')}</span><h2>{tr(lang, 'Arquitetura da recomendação explicável', 'Arquitectura de la recomendación explicable')}</h2></div></div><div className="lineage"><div><i><Landmark size={16}/></i><b>Unity Catalog</b><small>{tr(lang, 'Dados autorizados', 'Datos autorizados')}</small></div><ArrowRight/><div><i><Activity size={16}/></i><b>Feature Store + MLflow</b><small>{tr(lang, 'Sinais e modelos', 'Señales y modelos')}</small></div><ArrowRight/><div><i><Sparkles size={16}/></i><b>Gateway + Agents</b><small>{tr(lang, 'Recomendação e fontes', 'Recomendación y fuentes')}</small></div><ArrowRight/><div><i><UserRound size={16}/></i><b>{tr(lang, 'Decisão humana', 'Decisión humana')}</b><small>{tr(lang, 'Aceite, rejeição ou revisão', 'Aceptación, rechazo o revisión')}</small></div></div></Panel>
      </div>
      <TechnologyStrip lang={lang} />
    </>
  )
}

function RecommendationExplain({ lang, recommendation, onClose, onInvestigate, onExternal }: {
  lang: Lang
  recommendation: Recommendation
  onClose: () => void
  onInvestigate: () => void
  onExternal: () => void
}) {
  const [reason, setReason] = useState('')
  const [decision, setDecision] = useState<DecisionActionResult>()
  const [decisionError, setDecisionError] = useState('')
  const [submittingAction, setSubmittingAction] = useState<'approve'|'reject'|'defer'>()
  const [selectedAlternativeKey, setSelectedAlternativeKey] = useState<Recommendation['explanation']['alternatives'][number]['key']>()
  const selectedAlternative = recommendation.explanation.alternatives.find(alternative=>alternative.key===selectedAlternativeKey)
  const decide = async (action: 'approve'|'reject'|'defer') => {
    if (!selectedAlternative) {
      setDecisionError(tr(lang,'Selecione uma alternativa antes de registrar a decisão.','Selecciona una alternativa antes de registrar la decisión.'))
      return
    }
    setSubmittingAction(action)
    setDecisionError('')
    try {
      setDecision(await createDecisionAction({
        recommendation_id: recommendation.recommendation_id,
        country_code: recommendation.country_code,
        decision_type: recommendation.category,
        selected_alternative: selectedAlternative.key,
        selected_alternative_label: selectedAlternative.label,
        selected_alternative_rationale: selectedAlternative.rationale,
        action,
        reason: reason.trim() || tr(lang,'Decisão registrada pela demonstração Quilla.','Decisión registrada por la demostración Quilla.'),
      }))
    } catch (error) {
      setDecisionError(error instanceof Error ? error.message : tr(lang,'Não foi possível registrar a decisão.','No fue posible registrar la decisión.'))
    } finally { setSubmittingAction(undefined) }
  }
  const decisionRecord = asRecord(decision)
  const explanation = recommendation.explanation
  const score = decision?.model
  const observedAt = explanation.provenance.observed_at
  const featureLabels: Record<string,string> = {
    active_days_30d: tr(lang,'Dias ativos · 30d','Días activos · 30d'),
    transaction_count_30d: tr(lang,'Transações · 30d','Transacciones · 30d'),
    debit_volume_usd_30d: tr(lang,'Débitos · 30d','Débitos · 30d'),
    credit_volume_usd_30d: tr(lang,'Créditos · 30d','Créditos · 30d'),
    average_balance_usd_30d: tr(lang,'Saldo médio · 30d','Saldo promedio · 30d'),
    product_count: tr(lang,'Produtos','Productos'),
    interaction_count_30d: tr(lang,'Interações · 30d','Interacciones · 30d'),
    complaint_count_90d: tr(lang,'Reclamações · 90d','Reclamos · 90d'),
    digital_engagement_score: tr(lang,'Engajamento digital','Interacción digital'),
    country_code: tr(lang,'País','País'),
    treatment_group: tr(lang,'Grupo experimental','Grupo experimental'),
  }
  return <div className="explain-scrim" role="presentation" onMouseDown={event=>{if(event.target===event.currentTarget)onClose()}}><section className="explain-panel" role="dialog" aria-modal="true" aria-label={tr(lang,'Explicação da recomendação','Explicación de la recomendación')}>
    <button className="explain-close" onClick={onClose}><X size={18}/></button>
    <span className="eyebrow"><Sparkles size={14}/>{tr(lang,'RECOMENDAÇÃO EXPLICÁVEL · DADOS SINTÉTICOS','RECOMENDACIÓN EXPLICABLE · DATOS SINTÉTICOS')}</span>
    <h2>{recommendation.title}</h2>
    <p className="explain-lead">{recommendation.summary}</p>
    <div className="recommendation-impact"><span>{tr(lang,'Evidência de impacto','Evidencia de impacto')}</span><strong>{recommendation.impact}</strong><em>{tr(lang,'Candidato analítico — não constitui decisão automática','Candidato analítico — no constituye una decisión automática')}</em></div>
    <div className="why-now"><span><Radar size={16}/>{tr(lang,'POR QUE AGORA','POR QUÉ AHORA')}</span><p>{explanation.why_now}</p></div>
    <h3>{tr(lang,'Sinais determinantes','Señales determinantes')}</h3>
    <div className="explain-signals">
      {explanation.determining_signals.map(signal=><div className={signal.direction} key={signal.source_field}><span>{signal.label}</span><strong>{signal.value}</strong><small>{signal.source_field}</small></div>)}
    </div>
    <div className="explain-grid explain-provenance">
      <div><span>{tr(lang,'Método de priorização','Método de priorización')}</span><b>{explanation.method}</b></div>
      <div><span>{tr(lang,'Recorte observado','Corte observado')}</span><b>{observedAt || tr(lang,'Sem timestamp na fonte','Sin marca temporal en la fuente')}</b></div>
      <div><span>{tr(lang,'Fonte governada','Fuente gobernada')}</span><b>{explanation.provenance.source}</b></div>
      <div><span>SQL statement ID</span><b>{explanation.provenance.statement_id || '—'}</b></div>
    </div>
    <div className="model-evidence">
      <div className="model-evidence-head"><span><Activity size={16}/>MLFLOW · UNITY CATALOG</span><b>{explanation.model.validation_status}</b></div>
      <strong>{explanation.model.registered_name || tr(lang,'Modelo indisponível','Modelo no disponible')}{explanation.model.version&&` @ ${explanation.model.version}`}</strong>
      <p>{explanation.model.role}</p>
      {!score&&<small>{tr(lang,'O scoring online será executado somente ao aprovar a ação de crescimento.','El scoring en línea se ejecutará solo al aprobar la acción de crecimiento.')}</small>}
      {score?.status==='scored'&&<div className="online-score"><Check size={15}/><span><b>{tr(lang,'Scoring online concluído','Scoring en línea completado')} · {String(score.prediction||'—')}</b><small>{score.endpoint} · {score.latency_ms??'—'} ms · {score.scored_at||'—'}</small></span></div>}
      {score&&score.status!=='scored'&&<div className="online-score warning"><Timer size={15}/><span><b>{tr(lang,'Scoring online não concluído','Scoring en línea no completado')}</b><small>{score.status} · {score.endpoint||'—'}</small></span></div>}
      {score?.feature_snapshot&&<details className="feature-evidence"><summary>{tr(lang,'Ver snapshot de features sem PII','Ver snapshot de features sin PII')}</summary><div>{Object.entries(score.feature_snapshot).map(([key,value])=><span key={key}><small>{featureLabels[key]||humanizeKey(key)}</small><b>{String(value)}</b></span>)}</div><em>{score.feature_source} · {score.statement_id}</em></details>}
    </div>
    <h3>{tr(lang,'Escolha uma alternativa de ação','Elige una alternativa de acción')}</h3>
    <p className="alternative-help">{tr(lang,'A opção selecionada fará parte da decisão auditável.','La opción seleccionada formará parte de la decisión auditable.')}</p>
    <div className="alternative-grid" role="radiogroup" aria-label={tr(lang,'Alternativas de ação','Alternativas de acción')}>{explanation.alternatives.map(alternative=><button type="button" key={alternative.key} role="radio" aria-checked={selectedAlternativeKey===alternative.key} disabled={!!decision} onClick={()=>{setSelectedAlternativeKey(alternative.key);setDecisionError('')}} className={`${alternative.key}${selectedAlternativeKey===alternative.key?' selected':''}`}><span className="alternative-choice">{selectedAlternativeKey===alternative.key?<Check size={14}/>:null}</span><b>{alternative.label}</b><p>{alternative.rationale}</p></button>)}</div>
    <div className="explain-limitations"><span><ShieldCheck size={15}/>{tr(lang,'LIMITES E CONTROLES','LÍMITES Y CONTROLES')}</span><ul>{explanation.limitations.map(item=><li key={item}>{item}</li>)}</ul><div>{explanation.controls.map(control=><small key={control}>{control}</small>)}</div></div>
    <div className="decision-action-panel"><div><span>{tr(lang,'DECISÃO HUMANA GOVERNADA','DECISIÓN HUMANA GOBERNADA')}</span><h3>{tr(lang,'Registrar uma ação','Registrar una acción')}</h3><p>{selectedAlternative?tr(lang,`Alternativa selecionada: ${selectedAlternative.label}. A ação e a justificativa serão persistidas no Lakebase.`,`Alternativa seleccionada: ${selectedAlternative.label}. La acción y la justificación se guardarán en Lakebase.`):tr(lang,'Selecione uma alternativa acima para habilitar a decisão.','Selecciona una alternativa arriba para habilitar la decisión.')}</p></div><textarea value={reason} onChange={event=>setReason(event.target.value)} placeholder={tr(lang,'Justificativa opcional…','Justificación opcional…')} maxLength={500}/><div className="decision-action-buttons"><button disabled={!selectedAlternative||!!submittingAction||!!decision} className="approve" onClick={()=>void decide('approve')}>{submittingAction==='approve'?<RefreshCw size={14}/>:<Check size={14}/>} {tr(lang,'Aprovar','Aprobar')}</button><button disabled={!selectedAlternative||!!submittingAction||!!decision} className="reject" onClick={()=>void decide('reject')}>{submittingAction==='reject'?<RefreshCw size={14}/>:<X size={14}/>} {tr(lang,'Rejeitar','Rechazar')}</button><button disabled={!selectedAlternative||!!submittingAction||!!decision} className="defer" onClick={()=>void decide('defer')}>{submittingAction==='defer'?<RefreshCw size={14}/>:<Timer size={14}/>} {tr(lang,'Adiar','Posponer')}</button></div>{decision&&<div className="decision-feedback success"><Check size={15}/><span><b>{tr(lang,'Decisão registrada','Decisión registrada')}</b><small>{selectedAlternative?.label} · {firstString(decisionRecord,['status','outcome','action'],tr(lang,'Persistida com sucesso','Persistida con éxito'))}{firstString(decisionRecord,['action_id'])&&` · ${firstString(decisionRecord,['action_id']).slice(0,8)}`}</small></span></div>}{decisionError&&<div className="decision-feedback error"><X size={15}/><span><b>{tr(lang,'Falha ao registrar','Error al registrar')}</b><small>{decisionError}</small></span></div>}</div>
    <div className="explain-actions"><button className="secondary" onClick={onClose}>{tr(lang,'Fechar','Cerrar')}</button><button className="secondary" onClick={onExternal}><Globe2 size={16}/>{tr(lang,'Atualizar contexto externo','Actualizar contexto externo')}</button><button className="primary" onClick={onInvestigate}><LineChart size={16}/>{tr(lang,'Investigar no Genie','Investigar en Genie')}</button></div>
  </section></div>
}

const tourSteps: Record<Lang, { title: string; body: string }[]> = {
  pt: [
    { title: 'Comece pelo Signal', body: 'A home abre com uma decisão concreta, não com um dashboard genérico.' },
    { title: 'Converse com os dados', body: 'Quilla IQ investiga, simula e explica cada recomendação.' },
    { title: 'Troque de perspectiva', body: 'A mesma plataforma atende pessoas, empresas e operações regionais.' },
    { title: 'Mostre a confiança', body: 'A demo termina revelando governança, contexto e confirmação humana.' },
  ],
  es: [
    { title: 'Empieza por Signal', body: 'La página inicial abre con una decisión concreta, no con un dashboard genérico.' },
    { title: 'Conversa con los datos', body: 'Quilla IQ investiga, simula y explica cada recomendación.' },
    { title: 'Cambia de perspectiva', body: 'La misma plataforma atiende personas, empresas y operaciones regionales.' },
    { title: 'Muestra la confianza', body: 'La demo termina revelando gobernanza, contexto y confirmación humana.' },
  ],
}

function DemoTour({ lang, step, onStep, onClose, onNavigate }: { lang: Lang; step: number; onStep: (n: number) => void; onClose: () => void; onNavigate: (p: Page) => void }) {
  const steps = tourSteps[lang]
  const next = () => {
    if (step === steps.length - 1) return onClose()
    const pages: Page[] = ['command', 'iq', 'business', 'trust']
    onStep(step + 1); onNavigate(pages[step + 1])
  }
  return <div className="tour-card"><div className="tour-progress">{steps.map((_,i)=><i key={i} className={i<=step?'on':''}/>)}</div><button className="tour-close" onClick={onClose} aria-label={tr(lang, 'Fechar tour', 'Cerrar recorrido')}><X size={15}/></button><span>{tr(lang, 'PASSO', 'PASO')} {step+1} {tr(lang, 'DE', 'DE')} {steps.length}</span><h3>{steps[step].title}</h3><p>{steps[step].body}</p><div><button className="text-action" onClick={onClose}>{tr(lang, 'Sair', 'Salir')}</button><button className="primary" onClick={next}>{step === steps.length-1 ? tr(lang, 'Concluir', 'Finalizar') : tr(lang, 'Próximo', 'Siguiente')}<ArrowRight size={15}/></button></div></div>
}

export default function App() {
  const [entered, setEntered] = useState(false)
  const [lang, setLang] = useState<Lang>('pt')
  const [theme, setTheme] = useState<Theme>('dark')
  const [page, setPage] = useState<Page>('command')
  const [country, setCountry] = useState(countries[0].code)
  const [mobileOpen, setMobileOpen] = useState(false)
  const [tourStep, setTourStep] = useState<number | null>(null)
  const [selectedRecommendation, setSelectedRecommendation] = useState<Recommendation>()
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [recommendationsLoading, setRecommendationsLoading] = useState(false)
  const [recommendationsError, setRecommendationsError] = useState('')
  const [recommendationRefreshKey, setRecommendationRefreshKey] = useState(0)
  const recommendationCache = useRef(new Map<string, RecommendationFeed>())
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null)
  const [now, setNow] = useState(() => Date.now())
  const [genieLaunch, setGenieLaunch] = useState<{id:number;domain:string;question:string}>()
  const [iqLaunch, setIqLaunch] = useState<{id:number;question:string}>()
  const t = copy[lang]
  const pageName = useMemo(() => nav.find(n => n.id === page)?.label[lang], [page, lang])

  useEffect(() => {
    document.documentElement.lang = lang === 'pt' ? 'pt-BR' : 'es'
  }, [lang])

  useEffect(() => {
    if (!entered) return
    setNow(Date.now())
    const timer = window.setInterval(() => setNow(Date.now()), 1_000)
    return () => window.clearInterval(timer)
  }, [entered])

  useEffect(() => {
    if (!entered) return
    let active = true
    const cacheKey = `${country}:${lang}`
    const cached = recommendationCache.current.get(cacheKey)
    setRecommendations(cached?.recommendations || [])
    setRecommendationsError('')
    if (cached) {
      const cachedAt = Date.parse(cached.generated_at)
      setLastUpdatedAt(Number.isFinite(cachedAt) ? cachedAt : null)
    } else setLastUpdatedAt(null)
    setRecommendationsLoading(true)
    setSelectedRecommendation(undefined)
    getRecommendations(country, lang)
      .then(feed => { if (active) {
        recommendationCache.current.set(cacheKey, feed)
        const generatedAt = Date.parse(feed.generated_at)
        setRecommendations(feed.recommendations)
        setRecommendationsError('')
        setLastUpdatedAt(Number.isFinite(generatedAt) ? generatedAt : Date.now())
        setNow(Date.now())
      } })
      .catch(reason => { if (active) { setRecommendationsError(reason instanceof Error ? reason.message : 'Recommendation feed unavailable') } })
      .finally(() => { if (active) setRecommendationsLoading(false) })
    return () => { active = false }
  }, [entered, country, lang, recommendationRefreshKey])

  if (!entered) return <Welcome lang={lang} onLang={() => setLang(lang === 'pt' ? 'es' : 'pt')} onEnter={() => setEntered(true)} />

  const navigate = (p: Page) => { setPage(p); setMobileOpen(false) }
  const launchGenie = (domain: string, question: string) => {
    setGenieLaunch({ id: Date.now(), domain, question })
    navigate('genie')
  }
  const launchIQ = (recommendation: Recommendation) => {
    setCountry(recommendation.country_code)
    setIqLaunch({ id: Date.now(), question: recommendation.external_signal.prompt })
    setSelectedRecommendation(undefined)
    navigate('iq')
  }
  return (
    <div className="app-shell" data-theme={theme}>
      <aside className={`sidebar ${mobileOpen ? 'open' : ''}`}>
        <div className="sidebar-brand"><BrandMark /><span>quilla<small>FINANCIAL INTELLIGENCE</small></span><button className="mobile-close" onClick={() => setMobileOpen(false)}><X /></button></div>
        <nav>{nav.map(n => { const Icon = n.icon; return <button key={n.id} className={page === n.id ? 'active' : ''} onClick={() => navigate(n.id)}><Icon size={18}/><span>{n.label[lang]}</span>{n.id === 'iq' && <b>AI</b>}</button> })}</nav>
        <button className="demo-button" onClick={() => { setTourStep(0); navigate('command') }}><span><Play size={15}/></span><div><b>{t.demo}</b><small>{tr(lang, '4 minutos · guiada', '4 minutos · guiada')}</small></div><ArrowRight size={15}/></button>
        <div className="sidebar-profile"><div className="avatar coral">CT</div><div><b>Camila Torres</b><small>Regional CFO</small></div><ShieldCheck size={14}/></div>
      </aside>
      {mobileOpen && <button className="sidebar-scrim" onClick={() => setMobileOpen(false)} aria-label={tr(lang, 'Fechar menu', 'Cerrar menú')} />}

      <main>
        <header className="topbar">
          <div className="topbar-left"><button className="menu-button" onClick={() => setMobileOpen(true)}><Menu size={20}/></button><div><small>QUILLA / {pageName?.toUpperCase()}</small><b>{t.synthetic}</b></div></div>
          <div className="topbar-actions">
            <label className="country-select"><Globe2 size={15}/><select value={country} onChange={e=>setCountry(e.target.value)}>{countries.map(c=><option key={c.code} value={c.code}>{c.label[lang]}</option>)}</select><ChevronDown size={14}/></label>
            <button className="status-pill" onClick={()=>navigate('trust')}><span className="live-dot" />{freshnessLabel(lang, lastUpdatedAt, now, recommendationsLoading, !!recommendationsError)}</button>
            <button
              className="utility"
              onClick={() => setLang(current => current === 'pt' ? 'es' : 'pt')}
              aria-label={tr(lang, 'Mudar idioma para espanhol', 'Cambiar idioma a portugués')}
              title={tr(lang, 'Mudar para espanhol', 'Cambiar a portugués')}
            >{lang.toUpperCase()}</button>
            <button className="utility" onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? <Sun size={16}/> : <Moon size={16}/>}</button>
            <button className="utility notification" disabled={!recommendations.length} onClick={()=>{navigate('command');setSelectedRecommendation(recommendations[0])}} aria-label={tr(lang,'Abrir recomendação prioritária','Abrir recomendación prioritaria')}><Bell size={16}/>{recommendations.length>0&&<i />}</button>
          </div>
        </header>
        <div className="page-content">
          {page === 'command' && <CommandView lang={lang} recommendations={recommendations} loading={recommendationsLoading} error={recommendationsError} goIQ={() => navigate('iq')} onTour={()=>setTourStep(0)} onExplain={setSelectedRecommendation} onRecommendations={()=>launchGenie('executive',recommendations.map(item=>item.internal_prompt).join('\n\n'))} onRegional={()=>navigate('regional')} onRetry={()=>setRecommendationRefreshKey(value=>value+1)} />}
          {page === 'iq' && <QuillaIQ lang={lang} selectedCountry={country} avatar={<BrandMark size={28} />} launch={iqLaunch} />}
          {page === 'genie' && <GenieResearch lang={lang} selectedCountry={country} launch={genieLaunch} />}
          {page === 'personal' && <PersonalView lang={lang} country={country} onAsk={question=>launchGenie('customers',question)} />}
          {page === 'business' && <BusinessView lang={lang} country={country} onAnalyze={()=>launchGenie('executive',tr(lang,'Analise o caixa consolidado dos próximos 90 dias, explique riscos, oportunidades e recomende ações priorizadas.','Analiza la caja consolidada de los próximos 90 días, explica riesgos, oportunidades y recomienda acciones priorizadas.'))} />}
          {page === 'regional' && <RegionalView lang={lang} onAnalyze={countryCode=>{setCountry(countryCode);launchGenie('executive',tr(lang,`Investigue a performance financeira da Quilla em ${countryCode}, compare com a região e recomende ações.`,`Investiga el desempeño financiero de Quilla en ${countryCode}, compara con la región y recomienda acciones.`))}} />}
          {page === 'operations' && <OperationsView lang={lang} />}
          {page === 'trust' && <TrustView lang={lang} />}
        </div>
      </main>
      <nav className="mobile-nav">{nav.slice(0,5).map(n=>{const Icon=n.icon;return <button key={n.id} className={page===n.id?'active':''} onClick={()=>navigate(n.id)}><Icon size={19}/><span>{n.label[lang]}</span></button>})}<button onClick={()=>setMobileOpen(true)}><Menu size={19}/><span>{tr(lang, 'Mais', 'Más')}</span></button></nav>
      {tourStep !== null && <DemoTour lang={lang} step={tourStep} onStep={setTourStep} onClose={()=>setTourStep(null)} onNavigate={navigate}/>} 
      {selectedRecommendation && <RecommendationExplain lang={lang} recommendation={selectedRecommendation} onClose={()=>setSelectedRecommendation(undefined)} onExternal={()=>launchIQ(selectedRecommendation)} onInvestigate={()=>{const question=selectedRecommendation.internal_prompt;setSelectedRecommendation(undefined);launchGenie('executive',question)}}/>}
    </div>
  )
}
