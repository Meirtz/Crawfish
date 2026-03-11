use crawfish_types::ActionPhase;
use std::fmt;

/// Error returned when an action phase transition is not valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionPhaseError {
    pub from: ActionPhase,
    pub event: ActionEvent,
}

impl fmt::Display for ActionPhaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid action phase transition: event {:?} is not allowed in phase {:?}",
            self.event, self.from
        )
    }
}

impl std::error::Error for ActionPhaseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionEvent {
    Start,
    Block,
    ApprovalRequired,
    Resume,
    Cancel,
    Complete,
    Fail,
    Expire,
}

/// Attempt an action phase transition, returning an error if the event is
/// not valid for the current phase.
pub fn try_next_action_phase(
    current: ActionPhase,
    event: ActionEvent,
) -> Result<ActionPhase, ActionPhaseError> {
    let next = match (&current, &event) {
        (ActionPhase::Accepted, ActionEvent::Start) => ActionPhase::Running,
        (ActionPhase::Accepted, ActionEvent::Expire) => ActionPhase::Expired,
        (ActionPhase::Running, ActionEvent::Block) => ActionPhase::Blocked,
        (ActionPhase::Running, ActionEvent::ApprovalRequired) => ActionPhase::AwaitingApproval,
        (ActionPhase::Running, ActionEvent::Cancel) => ActionPhase::Cancelling,
        (ActionPhase::Running, ActionEvent::Complete) => ActionPhase::Completed,
        (ActionPhase::Running, ActionEvent::Fail) => ActionPhase::Failed,
        (ActionPhase::Blocked, ActionEvent::Resume) => ActionPhase::Running,
        (ActionPhase::Blocked, ActionEvent::Expire) => ActionPhase::Expired,
        (ActionPhase::AwaitingApproval, ActionEvent::Resume) => ActionPhase::Running,
        (ActionPhase::AwaitingApproval, ActionEvent::Expire) => ActionPhase::Expired,
        (ActionPhase::Cancelling, ActionEvent::Complete) => ActionPhase::Completed,
        (ActionPhase::Cancelling, ActionEvent::Fail) => ActionPhase::Failed,
        _ => {
            return Err(ActionPhaseError {
                from: current,
                event,
            });
        }
    };
    Ok(next)
}

/// Backward-compatible wrapper that silently returns the current phase on
/// invalid transitions. Prefer [`try_next_action_phase`] for new code.
pub fn next_action_phase(current: ActionPhase, event: ActionEvent) -> ActionPhase {
    let fallback = current.clone();
    try_next_action_phase(current, event).unwrap_or(fallback)
}

/// Returns `true` if the action is in a terminal phase (no further transitions).
pub fn is_terminal_phase(phase: &ActionPhase) -> bool {
    matches!(
        phase,
        ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Expired
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_starts_running() {
        let next = next_action_phase(ActionPhase::Accepted, ActionEvent::Start);
        assert_eq!(next, ActionPhase::Running);
    }

    #[test]
    fn blocked_resumes_to_running() {
        let next = next_action_phase(ActionPhase::Blocked, ActionEvent::Resume);
        assert_eq!(next, ActionPhase::Running);
    }

    #[test]
    fn try_next_returns_error_on_invalid_transition() {
        let result = try_next_action_phase(ActionPhase::Completed, ActionEvent::Start);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.from, ActionPhase::Completed);
    }

    #[test]
    fn next_action_phase_returns_current_on_invalid_transition() {
        let result = next_action_phase(ActionPhase::Completed, ActionEvent::Start);
        assert_eq!(result, ActionPhase::Completed);
    }

    #[test]
    fn terminal_phases() {
        assert!(is_terminal_phase(&ActionPhase::Completed));
        assert!(is_terminal_phase(&ActionPhase::Failed));
        assert!(is_terminal_phase(&ActionPhase::Expired));
        assert!(!is_terminal_phase(&ActionPhase::Running));
        assert!(!is_terminal_phase(&ActionPhase::Accepted));
    }
}
