// ── Auth ─────────────────────────────────────────────────────────────────────

export interface User {
  id: string
  email: string
  full_name: string | null
  plan: 'free' | 'starter' | 'pro' | 'unlimited'
  is_active: boolean
  created_at: string
}

export interface TokenResponse {
  access_token: string
  token_type: string
}

// ── CV ───────────────────────────────────────────────────────────────────────

export type CVStatus = 'uploaded' | 'parsing' | 'parsed' | 'failed'
export type CVBuildMode = 'edit' | 'rebuild' | 'from_scratch'

export interface CV {
  id: string
  original_filename: string
  status: CVStatus
  file_size_bytes: number
  mime_type: string
  created_at: string
  quality_score: number | null
  build_mode: CVBuildMode | null
}

export interface CVStatusResponse {
  id: string
  status: CVStatus
  error_message: string | null
  parsed_section_count: number | null
  parsed_word_count: number | null
  quality_score: number | null
  quality_verdict: 'poor' | 'weak' | 'good' | 'strong' | null
  rebuild_recommended: boolean | null
  build_mode: CVBuildMode | null
  template_slug: string | null
}

export interface BestPracticesSuggestion {
  priority: 'high' | 'medium' | 'low'
  category: 'impact' | 'grammar' | 'formatting' | 'structure' | 'length' | 'ats'
  title: string
  detail: string
  example_fix?: string
}

export interface BestPracticesResponse {
  cv_id: string
  suggestions: BestPracticesSuggestion[]
  top_strength: string
  summary: string
}

export interface CVTemplate {
  slug: string
  display_name: string
  description: string | null
  sort_order: number
  preview_metadata: {
    accent_color?: string
    font_name?: string
  } | null
}

// ── Job Runs ─────────────────────────────────────────────────────────────────

export type JobRunStatus =
  | 'queued'
  | 'retrieving'
  | 'llm_processing'
  | 'rendering'
  | 'completed'
  | 'failed'

export interface JobRun {
  id: string
  user_id: string
  cv_id: string
  status: JobRunStatus
  jd_text: string | null
  jd_url: string | null
  output_s3_key: string | null
  edit_plan: Record<string, unknown> | null
  positioning: PositioningOutput | null
  reports: ReportPack | null
  retrieval_data: RetrievalData | null
  created_at: string
  completed_at: string | null
  failed_step: string | null
  error_message: string | null
}

export interface JobRunSummary {
  id: string
  status: JobRunStatus
  jd_text: string | null
  jd_url: string | null
  keyword_match_score: number | null
  pipeline_stage: string | null
  pipeline_notes: string | null
  applied_at: string | null
  created_at: string
  completed_at: string | null
}

// ── Output types ─────────────────────────────────────────────────────────────

export interface PositioningOutput {
  positioning_headline: string
  narrative_thread: string
  strongest_angles: Array<{ title: string; explanation: string }>
  gaps_to_address: Array<{ gap: string; severity: 'low' | 'medium' | 'high'; mitigation: string }>
  interview_themes: string[]
  red_flags_to_preempt: string[]
}

export interface NamedContact {
  name: string
  title: string
  linkedin_url: string | null
  why_relevant: string
  outreach_message: string
}

export interface KnownNetworkAsk {
  person: string
  ask: string
}

export interface NetworkingOutput {
  named_contacts: NamedContact[]
  known_network_asks: KnownNetworkAsk[]
  warm_path_hypotheses: string[]
  linkedin_search_strings: string[]
  outreach_template_hiring_manager: string
  outreach_template_alumni: string
  outreach_template_recruiter: string
  seven_day_action_plan: string[]
}

export interface CompanyOutput {
  company_name: string
  hq_location: string
  business_description: string
  revenue_and_scale: string
  recent_news: string[]
  strategic_priorities: string[]
  culture_signals: string[]
  competitor_landscape: string
  hiring_signals: string[]
  red_flags: string[]
}

export interface SalaryRange {
  title: string
  base_monthly_aed_low: number | null
  base_monthly_aed_high: number | null
  base_annual_aed_low: number | null
  base_annual_aed_high: number | null
  bonus_pct_low: number | null
  bonus_pct_high: number | null
  total_comp_note: string
  source: string
  confidence: 'low' | 'medium' | 'high'
}

export interface InterviewStage {
  stage: string
  format: string
  who: string
  duration: string
  what_to_expect: string
}

export interface BehaviouralQuestion {
  question: string
  why_they_ask: string
  your_story: string
  key_points: string[]
}

export interface CaseQuestion {
  question: string
  case_type: string
  how_to_frame: string
  watch_out: string
}

export interface SituationalQuestion {
  question: string
  what_they_want: string
  suggested_answer_angle: string
}

export interface CultureQuestion {
  question: string
  ideal_answer_angle: string
}

export interface QuestionToAsk {
  question: string
  why_powerful: string
}

export interface ApplicationOutput {
  positioning_headline: string
  cover_letter_angle: string
  interview_process: InterviewStage[]
  question_bank: {
    behavioural: BehaviouralQuestion[]
    business_case: CaseQuestion[]
    situational: SituationalQuestion[]
    culture_and_motivation: CultureQuestion[]
  }
  questions_to_ask_them: QuestionToAsk[]
  thirty_sixty_ninety: { '30': string; '60': string; '90': string }
  risks_to_address: string[]
  differentiators: string[]
}

export interface ReportPack {
  company: CompanyOutput
  role: {
    role_title: string
    seniority_level: string
    reporting_line: string
    what_they_really_want: string
    hidden_requirements: string[]
    hiring_manager_worries: string[]
    keyword_match_gaps: string[]
    positioning_recommendation: string
  }
  salary: SalaryRange[]
  networking: NetworkingOutput
  application: ApplicationOutput
  exec_summary: string[]
}

export interface RetrievalData {
  company_overview: string
  salary_data: string
  news: string[]
  competitors: string
  contacts: Array<{
    name: string
    title: string
    linkedin_url: string | null
    relevance: string
    suggested_outreach: string
  }>
  sources: string[]
  partial_failure: boolean
}

// ── Candidate Profile ─────────────────────────────────────────────────────────

export type ProfileStatus = 'draft' | 'active' | 'archived'

export interface ExperienceEntry {
  id: string
  company_name: string
  role_title: string
  start_date: string
  end_date: string | null
  location: string | null
  context: string
  contribution: string
  outcomes: string
  methods: string
  hidden: string
  freeform: string
}

export interface JobPreferences {
  regions?: string[]
  roles?: string[]
  seniority?: string[]
  companyType?: string[]
  stage?: string[]
  sectors?: string[]
  salaryMin?: string
  openToRelo?: string
}

export interface CandidateProfile {
  id: string
  user_id: string
  status: ProfileStatus
  headline: string | null
  global_context: string
  global_notes: string
  preferences?: JobPreferences
  experiences: ExperienceEntry[]
  created_at: string
  updated_at: string
}

// ── Billing ───────────────────────────────────────────────────────────────────

export interface UsageSummary {
  plan: string
  monthly_run_limit: number | null
  runs_used_this_month: number
  runs_remaining: number | null
  reset_date: string | null
}

// ── API request types ─────────────────────────────────────────────────────────

export interface JobRunCreateRequest {
  cv_id: string
  jd_text?: string
  jd_url?: string
  profile_id?: string
  preferences?: {
    region?: string
    role_title?: string
    page_limit?: number
    tone?: string
  }
  known_contacts?: string[]
}

export interface JDExtractResponse {
  url: string
  jd_text: string
  char_count: number
}
