"""Pydantic schemas exposed through the MCP tool contract."""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field, model_validator
from .config import PROTOCOL_VERSION

class RetrievalSpec(BaseModel):
    purpose: str = Field(
        default="",
        description="Why this retrieval is being performed. Sent to TinyFish purpose for result quality.",
    )
    domain_type: Literal["web", "news", "research_paper"] = Field(
        default="web",
        description="Use research_paper for academic/scientific literature; news for recent reporting; web otherwise.",
    )
    include_domains: list[str] = Field(default_factory=list)
    exclude_domains: list[str] = Field(default_factory=list)
    recency_minutes: Optional[int] = None
    after_date: Optional[str] = None
    before_date: Optional[str] = None
    pub_year_min: Optional[int] = None
    pub_year_max: Optional[int] = None
    language: str = "en"
    location: Optional[str] = None

    @model_validator(mode="after")
    def _validate_filters(self):
        if self.domain_type == "research_paper":
            if self.recency_minutes is not None or self.after_date or self.before_date:
                raise ValueError(
                    "research_paper does not support recency_minutes/after_date/before_date; use pub_year_min/pub_year_max"
                )
        return self

class TemporalMode(str, Enum):
    CURRENT = "CURRENT"
    HISTORICAL = "HISTORICAL"
    FOUNDATIONAL = "FOUNDATIONAL"
    TIMELESS = "TIMELESS"

class ClaimDraft(BaseModel):
    client_ref: str = Field(description="Client-only correlation label; never a claim_id.")
    text: str = Field(min_length=8, description="One atomic factual/propositional claim.")
    entity: Optional[str] = None
    temporal_mode: TemporalMode = TemporalMode.TIMELESS
    atomicity_override: bool = False
    atomicity_override_reason: Optional[str] = None

    @model_validator(mode="after")
    def _override_reason(self):
        if self.atomicity_override and not (self.atomicity_override_reason or "").strip():
            raise ValueError("atomicity_override=True requires atomicity_override_reason")
        return self

class EvidenceBinding(BaseModel):
    claim_id: str
    source_id: str
    quote: str

class EvidenceRelationJudgment(BaseModel):
    evidence_id: str
    relation: Literal["SUPPORTS", "CONTRADICTS", "RELATED_BUT_INSUFFICIENT", "IRRELEVANT"]
    strength: float = Field(default=0.7, ge=0.0, le=1.0)

class SourceScreening(BaseModel):
    source_id: str
    verdict: Literal["RELEVANT", "PARTIAL", "IRRELEVANT"]
    reason: str = Field(min_length=3, max_length=500)

class CandidateEvidenceReview(BaseModel):
    claim_id: str
    source_id: str
    verdict: Literal["RELEVANT", "PARTIAL", "IRRELEVANT"]
    reason: str = Field(min_length=3, max_length=500)
    quote: Optional[str] = None
    relation: Optional[Literal["SUPPORTS", "CONTRADICTS", "RELATED_BUT_INSUFFICIENT", "IRRELEVANT"]] = None
    strength: float = Field(default=0.7, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _require_relation_for_quote(self):
        if self.quote and self.relation is None:
            raise ValueError("relation is required when quote is provided")
        return self

class GapPlan(BaseModel):
    claim_id: str
    queries: list[str] = Field(min_length=1, max_length=6)
    mode: Literal["support", "disconfirm", "resolve_conflict"]
    reason: str
    retrieval: RetrievalSpec = Field(default_factory=RetrievalSpec)

class CitationCheck(BaseModel):
    claim_id: str
    source_id: str
    quote: str

class ClaimTension(BaseModel):
    claim_id_a: str
    claim_id_b: str
    description: str
    resolution: Optional[str] = None

class SubagentTask(BaseModel):
    subagent_id: str
    objective: str
    output_format: str = "Evidence-focused findings with source passages"
    task_boundaries: str
    queries: list[str] = Field(min_length=1, max_length=8)
    retrieval: RetrievalSpec = Field(default_factory=RetrievalSpec)
    excluded_topics: list[str] = Field(default_factory=list)
    related_topics: list[str] = Field(default_factory=list)
    seeks_disconfirming_evidence: bool = False

class ResearchPlan(BaseModel):
    execution_mode: Literal["parallel_subagents", "sequential"] = "parallel_subagents"
    tasks: list[SubagentTask] = Field(default_factory=list)
    sequential_steps: list[str] = Field(default_factory=list)

class NextAction(BaseModel):
    tool: str
    reason: str
    instructions: list[str] = Field(default_factory=list)
    required_input: dict[str, Any] = Field(default_factory=dict)
    completion_condition: Optional[str] = None

class ResearchToolResponse(BaseModel):
    status: str
    data: dict[str, Any] = Field(default_factory=dict)
    next_action: Optional[NextAction] = None
    quality_gate: Optional[dict[str, Any]] = None
    agent_rules: list[str] = Field(default_factory=list)
    protocol_version: str = PROTOCOL_VERSION

class ClaimAssessment(BaseModel):
    claim_id: str
    status: str
    stance_confidence: float
    evidence_quality: float
    resolution_confidence: float
    metrics: dict[str, Any] = Field(default_factory=dict)
