"""Trusted final-request instruction shared by provider request gates."""

FINAL_PREPARATION_INSTRUCTION = (
    'This is the LAST allowed model request for PREPARATION. All tools are disabled '
    'for this response. Return your final concise evidence summary now, using only '
    'the context and tool results already available. Include relevant virtual paths '
    'and rationale for the later separate plan. If evidence is insufficient, explicitly '
    'state what is missing and your uncertainty. Do not call tools, request another '
    'turn, invent observations, produce implementation code, or produce a plan JSON.'
)
