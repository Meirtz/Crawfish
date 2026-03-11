use crawfish_types::{AgentState, DegradedProfileName};
use std::fmt;

/// Error returned when a lifecycle state transition is not valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleError {
    pub from: AgentState,
    pub event: LifecycleEvent,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid lifecycle transition: event {:?} is not allowed in state {:?}",
            self.event, self.from
        )
    }
}

impl std::error::Error for LifecycleError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEvent {
    ConfigureSucceeded,
    ConfigureFailed,
    ActivateRequested,
    ActivationHealthy,
    SoftFailure(DegradedProfileName),
    HardFailure,
    DrainRequested,
    DrainCompleted,
    FinalizeRequested,
    RetryRequested,
    DependencyRecovered,
}

/// Attempt a state transition, returning an error if the event is not valid
/// for the current state.
pub fn try_next_agent_state(
    current: AgentState,
    event: LifecycleEvent,
) -> Result<AgentState, LifecycleError> {
    let next = match (&current, &event) {
        (AgentState::Unconfigured, LifecycleEvent::ConfigureSucceeded) => AgentState::Inactive,
        (AgentState::Unconfigured, LifecycleEvent::ActivateRequested) => AgentState::Configuring,
        (AgentState::Configuring, LifecycleEvent::ConfigureSucceeded) => AgentState::Inactive,
        (AgentState::Configuring, LifecycleEvent::ConfigureFailed) => AgentState::Failed,
        (AgentState::Configuring, LifecycleEvent::RetryRequested) => AgentState::Configuring,
        (AgentState::Inactive, LifecycleEvent::ActivateRequested) => AgentState::Activating,
        (AgentState::Inactive, LifecycleEvent::FinalizeRequested) => AgentState::Finalized,
        (AgentState::Activating, LifecycleEvent::ActivationHealthy) => AgentState::Active,
        (AgentState::Activating, LifecycleEvent::SoftFailure(_)) => AgentState::Degraded,
        (AgentState::Activating, LifecycleEvent::HardFailure) => AgentState::Failed,
        (AgentState::Active, LifecycleEvent::SoftFailure(_)) => AgentState::Degraded,
        (AgentState::Active, LifecycleEvent::DrainRequested) => AgentState::Draining,
        (AgentState::Active, LifecycleEvent::HardFailure) => AgentState::Failed,
        (AgentState::Degraded, LifecycleEvent::DependencyRecovered) => AgentState::Active,
        (AgentState::Degraded, LifecycleEvent::DrainRequested) => AgentState::Draining,
        (AgentState::Degraded, LifecycleEvent::HardFailure) => AgentState::Failed,
        (AgentState::Draining, LifecycleEvent::DrainCompleted) => AgentState::Inactive,
        (AgentState::Draining, LifecycleEvent::FinalizeRequested) => AgentState::Finalized,
        (AgentState::Draining, LifecycleEvent::HardFailure) => AgentState::Failed,
        (AgentState::Failed, LifecycleEvent::RetryRequested) => AgentState::Configuring,
        (AgentState::Failed, LifecycleEvent::FinalizeRequested) => AgentState::Finalized,
        _ => {
            return Err(LifecycleError {
                from: current,
                event,
            });
        }
    };
    Ok(next)
}

/// Backward-compatible wrapper that silently returns the current state on
/// invalid transitions. Prefer [`try_next_agent_state`] for new code.
pub fn next_agent_state(current: AgentState, event: LifecycleEvent) -> AgentState {
    let fallback = current.clone();
    try_next_agent_state(current, event).unwrap_or(fallback)
}

/// Returns `true` if the agent is in a state that can accept new actions.
pub fn can_accept_action(state: &AgentState) -> bool {
    matches!(state, AgentState::Active | AgentState::Degraded)
}

/// Returns `true` if the given event is a valid transition from the given state.
pub fn is_valid_transition(current: &AgentState, event: &LifecycleEvent) -> bool {
    // Clone is cheap for these enums.
    try_next_agent_state(current.clone(), event.clone()).is_ok()
}

/// Returns `true` if the state is terminal (no further transitions possible).
pub fn is_terminal(state: &AgentState) -> bool {
    matches!(state, AgentState::Finalized)
}

/// Returns `true` if the agent can potentially recover to an active state.
pub fn is_recoverable(state: &AgentState) -> bool {
    matches!(
        state,
        AgentState::Degraded
            | AgentState::Failed
            | AgentState::Inactive
            | AgentState::Configuring
    )
}

/// Lists all events that are valid outbound transitions from the given state.
pub fn allowed_events(state: &AgentState) -> Vec<LifecycleEvent> {
    let candidates = [
        LifecycleEvent::ConfigureSucceeded,
        LifecycleEvent::ConfigureFailed,
        LifecycleEvent::ActivateRequested,
        LifecycleEvent::ActivationHealthy,
        LifecycleEvent::SoftFailure(DegradedProfileName::DependencyIsolation),
        LifecycleEvent::HardFailure,
        LifecycleEvent::DrainRequested,
        LifecycleEvent::DrainCompleted,
        LifecycleEvent::FinalizeRequested,
        LifecycleEvent::RetryRequested,
        LifecycleEvent::DependencyRecovered,
    ];
    candidates
        .into_iter()
        .filter(|event| is_valid_transition(state, event))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_soft_failure_degrades() {
        let next = next_agent_state(
            AgentState::Active,
            LifecycleEvent::SoftFailure(DegradedProfileName::DependencyIsolation),
        );
        assert_eq!(next, AgentState::Degraded);
    }

    #[test]
    fn failed_retry_reconfigures() {
        let next = next_agent_state(AgentState::Failed, LifecycleEvent::RetryRequested);
        assert_eq!(next, AgentState::Configuring);
    }

    #[test]
    fn try_next_returns_error_on_invalid_transition() {
        let result =
            try_next_agent_state(AgentState::Finalized, LifecycleEvent::ActivateRequested);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.from, AgentState::Finalized);
    }

    #[test]
    fn next_agent_state_returns_current_on_invalid_transition() {
        let result = next_agent_state(AgentState::Finalized, LifecycleEvent::ActivateRequested);
        assert_eq!(result, AgentState::Finalized);
    }

    #[test]
    fn can_accept_action_active_and_degraded() {
        assert!(can_accept_action(&AgentState::Active));
        assert!(can_accept_action(&AgentState::Degraded));
        assert!(!can_accept_action(&AgentState::Inactive));
        assert!(!can_accept_action(&AgentState::Failed));
        assert!(!can_accept_action(&AgentState::Finalized));
    }

    #[test]
    fn is_terminal_only_finalized() {
        assert!(is_terminal(&AgentState::Finalized));
        assert!(!is_terminal(&AgentState::Active));
        assert!(!is_terminal(&AgentState::Failed));
    }

    #[test]
    fn is_recoverable_checks() {
        assert!(is_recoverable(&AgentState::Degraded));
        assert!(is_recoverable(&AgentState::Failed));
        assert!(is_recoverable(&AgentState::Inactive));
        assert!(!is_recoverable(&AgentState::Finalized));
        assert!(!is_recoverable(&AgentState::Active));
    }

    #[test]
    fn allowed_events_for_active() {
        let events = allowed_events(&AgentState::Active);
        assert!(events.iter().any(|e| matches!(e, LifecycleEvent::SoftFailure(_))));
        assert!(events.iter().any(|e| matches!(e, LifecycleEvent::DrainRequested)));
        assert!(events.iter().any(|e| matches!(e, LifecycleEvent::HardFailure)));
        assert!(!events.iter().any(|e| matches!(e, LifecycleEvent::ConfigureSucceeded)));
    }

    #[test]
    fn allowed_events_for_finalized_is_empty() {
        let events = allowed_events(&AgentState::Finalized);
        assert!(events.is_empty());
    }

    #[test]
    fn is_valid_transition_checks() {
        assert!(is_valid_transition(
            &AgentState::Active,
            &LifecycleEvent::DrainRequested
        ));
        assert!(!is_valid_transition(
            &AgentState::Active,
            &LifecycleEvent::ConfigureSucceeded
        ));
    }
}
