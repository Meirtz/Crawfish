use crawfish_types::ActionPhase;

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

pub fn next_action_phase(current: ActionPhase, event: ActionEvent) -> ActionPhase {
    match (current, event) {
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
        (phase, _) => phase,
    }
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
}
