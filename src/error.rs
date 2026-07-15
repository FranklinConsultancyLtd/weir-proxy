use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum SymfynityError {
    #[error("tenant '{0}' has exceeded its token budget")]
    BudgetExceeded(String),
    #[error("unknown tenant or missing X-Symfynity-Tenant header")]
    UnknownTenant,
    #[error("upstream provider request failed: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("tenant '{tenant}' violated policy: {reason}")]
    PolicyViolation { tenant: String, reason: String },
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

impl IntoResponse for SymfynityError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            SymfynityError::BudgetExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "budget_exceeded"),
            SymfynityError::UnknownTenant => (StatusCode::UNAUTHORIZED, "unknown_tenant"),
            SymfynityError::Upstream(_) => (StatusCode::BAD_GATEWAY, "upstream_error"),
            SymfynityError::Config(_) => (StatusCode::INTERNAL_SERVER_ERROR, "config_error"),
            SymfynityError::PolicyViolation { .. } => (StatusCode::FORBIDDEN, "policy_violation"),
        };
        let body = ErrorBody { error: code, message: self.to_string() };
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_exceeded_maps_to_429() {
        let response = SymfynityError::BudgetExceeded("acct_1".into()).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn unknown_tenant_maps_to_401() {
        let response = SymfynityError::UnknownTenant.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn policy_violation_maps_to_403() {
        let response = SymfynityError::PolicyViolation {
            tenant: "acct_1".into(),
            reason: "blocked_tool: send_email".into(),
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
