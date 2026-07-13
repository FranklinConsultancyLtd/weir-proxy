use std::sync::Arc;
use bytes::Bytes;
use serde::Deserialize;
use tiktoken_rs::CoreBPE;

use crate::provider::{ChunkCost, NonStreamingCost, ProviderAdapter};

pub struct AnthropicAdapter {
    tokenizer: Arc<CoreBPE>,
    input_tokens: u64,
}

impl AnthropicAdapter {
    pub fn new(tokenizer: Arc<CoreBPE>) -> Self {
        Self { tokenizer, input_tokens: 0 }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicMessageStart },
    #[serde(rename = "content_block_start")]
    ContentBlockStart { content_block: AnthropicContentBlockStart },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { delta: AnthropicDelta },
    #[serde(rename = "message_delta")]
    MessageDelta { usage: AnthropicOutputUsage },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlockStart {
    #[serde(rename = "tool_use")]
    ToolUse { name: String },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct AnthropicMessageStart {
    usage: AnthropicInputUsage,
}

#[derive(Deserialize)]
struct AnthropicInputUsage {
    input_tokens: u64,
}

#[derive(Deserialize, Default)]
struct AnthropicDelta {
    #[serde(default)]
    text: Option<String>,
    // Present on tool-use content blocks: incremental fragments of the
    // tool call's JSON input, streamed the same way `text` is streamed
    // for a text content block. Must be counted too, or a tool-call-heavy
    // stream's interim estimate stays near zero.
    #[serde(default)]
    partial_json: Option<String>,
}

#[derive(Deserialize)]
struct AnthropicOutputUsage {
    output_tokens: u64,
}

impl ProviderAdapter for AnthropicAdapter {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost {
        let mut estimated_tokens = 0u64;
        let mut authoritative_total = None;
        let mut tool_calls = Vec::new();
        let text = String::from_utf8_lossy(raw);

        for line in text.lines() {
            let Some(payload) = line.strip_prefix("data: ") else { continue };
            let Ok(event) = serde_json::from_str::<AnthropicEvent>(payload) else { continue };

            match event {
                AnthropicEvent::MessageStart { message } => {
                    self.input_tokens = message.usage.input_tokens;
                    authoritative_total = Some(self.input_tokens);
                }
                AnthropicEvent::ContentBlockStart { content_block } => {
                    if let AnthropicContentBlockStart::ToolUse { name } = content_block {
                        tool_calls.push(name);
                    }
                }
                AnthropicEvent::ContentBlockDelta { delta } => {
                    if let Some(t) = delta.text {
                        estimated_tokens += self.tokenizer.encode_ordinary(&t).len() as u64;
                    }
                    if let Some(json) = delta.partial_json {
                        estimated_tokens += self.tokenizer.encode_ordinary(&json).len() as u64;
                    }
                }
                AnthropicEvent::MessageDelta { usage } => {
                    authoritative_total = Some(self.input_tokens + usage.output_tokens);
                }
                AnthropicEvent::Other => {}
            }
        }

        ChunkCost { estimated_tokens, authoritative_total, tool_calls }
    }

    fn non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        enum NonStreamingContentBlock {
            #[serde(rename = "tool_use")]
            ToolUse { name: String },
            #[serde(other)]
            Other,
        }
        #[derive(Deserialize)]
        struct NonStreamingUsage {
            input_tokens: u64,
            output_tokens: u64,
        }
        #[derive(Deserialize)]
        struct NonStreamingResponse {
            #[serde(default)]
            content: Vec<NonStreamingContentBlock>,
            usage: NonStreamingUsage,
        }

        let Ok(parsed) = serde_json::from_slice::<NonStreamingResponse>(body) else {
            return NonStreamingCost { total_tokens: None, tool_calls: Vec::new() };
        };

        let tool_calls = parsed
            .content
            .into_iter()
            .filter_map(|block| match block {
                NonStreamingContentBlock::ToolUse { name } => Some(name),
                NonStreamingContentBlock::Other => None,
            })
            .collect();

        NonStreamingCost {
            total_tokens: Some(parsed.usage.input_tokens + parsed.usage.output_tokens),
            tool_calls,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenizer() -> Arc<CoreBPE> {
        Arc::new(tiktoken_rs::cl100k_base().unwrap())
    }

    #[test]
    fn message_start_sets_authoritative_baseline() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.authoritative_total, Some(25));
    }

    #[test]
    fn content_block_delta_contributes_estimate() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(cost.estimated_tokens >= 1);
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn message_delta_combines_input_baseline_with_output_tokens() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let start = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
        );
        adapter.chunk_cost(&start);

        let delta = Bytes::from_static(
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":15}}\n\n",
        );
        let cost = adapter.chunk_cost(&delta);
        assert_eq!(cost.authoritative_total, Some(40)); // 25 input + 15 output
    }

    #[test]
    fn estimates_tokens_from_tool_use_partial_json() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"location\\\": \\\"San Francisco, CA\\\"}\"}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(
            cost.estimated_tokens >= 3,
            "tool-use partial_json should tokenize to a meaningful count, got {}",
            cost.estimated_tokens
        );
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn non_streaming_cost_combines_input_and_output_tokens() {
        let adapter = AnthropicAdapter::new(tokenizer());
        let body = Bytes::from_static(
            b"{\"content\":[{\"type\":\"text\",\"text\":\"Hi\"}],\"usage\":{\"input_tokens\":25,\"output_tokens\":15}}",
        );
        assert_eq!(adapter.non_streaming_cost(&body), Some(40));
    }

    #[test]
    fn non_streaming_cost_returns_none_for_unparseable_body() {
        let adapter = AnthropicAdapter::new(tokenizer());
        let body = Bytes::from_static(b"not json at all");
        let cost = adapter.non_streaming_cost(&body);
        assert_eq!(cost.total_tokens, None);
        assert!(cost.tool_calls.is_empty());
    }

    #[test]
    fn chunk_cost_reports_tool_use_name_from_content_block_start() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_01\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
    }

    #[test]
    fn non_streaming_cost_reports_tool_use_names() {
        let adapter = AnthropicAdapter::new(tokenizer());
        let body = Bytes::from_static(
            b"{\"content\":[{\"type\":\"text\",\"text\":\"Hi\"},{\"type\":\"tool_use\",\"id\":\"toolu_01\",\"name\":\"get_weather\",\"input\":{}}],\"usage\":{\"input_tokens\":25,\"output_tokens\":15}}",
        );
        let cost = adapter.non_streaming_cost(&body);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
        assert_eq!(cost.total_tokens, Some(40));
    }

    #[test]
    fn content_block_start_for_text_block_is_ignored() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(cost.tool_calls.is_empty());
        assert_eq!(cost.estimated_tokens, 0);
    }
}
