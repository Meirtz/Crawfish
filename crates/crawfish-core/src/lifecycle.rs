use crawfish_types::{AgentState, DegradedProfileName};

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

pub fn next_agent_state(current: AgentState, event: LifecycleEvent) -> AgentState {
    match (current, event) {
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
        (state, _) => state,
    }
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
}
