use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

use lapce_rpc::plugin::PluginId;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use super::fabric_types::{
    FabricCandidate, FabricEvent, FabricHealth, FabricModuleRegistration,
    FabricModuleSnapshot, FabricModuleState, FabricRegistrationId, FabricRequest,
    FabricRoute, FabricSnapshot, RegisteredCapability,
};

pub type SharedFabric = Arc<WasmFabric>;

pub struct WasmFabric {
    inner: RwLock<FabricInner>,
    next_registration_id: AtomicU64,
    next_generation: AtomicU64,
    routing_policy: Arc<dyn FabricRoutingPolicy>,
}

#[derive(Default)]
struct FabricInner {
    modules: HashMap<PluginId, FabricModuleRecord>,
    events: Vec<FabricEvent>,
    generation: u64,
}

#[derive(Clone)]
struct FabricModuleRecord {
    registration_id: FabricRegistrationId,
    plugin_id: PluginId,
    volt_id: lapce_rpc::plugin::VoltID,
    name: String,
    capabilities: Vec<RegisteredCapability>,
    state: FabricModuleState,
    health: FabricHealth,
}

pub trait FabricRoutingPolicy: Send + Sync {
    fn rank(
        &self,
        request: &FabricRequest,
        candidate: &FabricCandidate,
    ) -> FabricCandidateScore;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FabricCandidateScore {
    pub health: u8,
    pub specificity: u8,
    pub priority: i32,
}

#[derive(Default)]
pub struct DefaultFabricRoutingPolicy;

impl FabricRoutingPolicy for DefaultFabricRoutingPolicy {
    fn rank(
        &self,
        request: &FabricRequest,
        candidate: &FabricCandidate,
    ) -> FabricCandidateScore {
        FabricCandidateScore {
            health: candidate.health.routing_rank(),
            specificity: candidate.capability.capability.specificity(request),
            priority: candidate.capability.capability.priority,
        }
    }
}

impl Default for WasmFabric {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmFabric {
    pub fn new() -> Self {
        Self::with_policy(Arc::new(DefaultFabricRoutingPolicy))
    }

    pub fn with_policy(policy: Arc<dyn FabricRoutingPolicy>) -> Self {
        Self {
            inner: RwLock::new(FabricInner::default()),
            next_registration_id: AtomicU64::new(1),
            next_generation: AtomicU64::new(1),
            routing_policy: policy,
        }
    }

    pub fn register(
        &self,
        registration: FabricModuleRegistration,
    ) -> FabricRegistrationId {
        let registration_id = FabricRegistrationId(
            self.next_registration_id
                .fetch_add(1, AtomicOrdering::Relaxed),
        );

        let record = FabricModuleRecord {
            registration_id,
            plugin_id: registration.plugin_id,
            volt_id: registration.volt_id,
            name: registration.name,
            capabilities: registration.capabilities,
            state: registration.state,
            health: registration.health,
        };

        let mut inner = self.inner.write();
        let event = if inner.modules.contains_key(&record.plugin_id) {
            FabricEvent::Updated {
                registration_id,
                plugin_id: record.plugin_id,
            }
        } else {
            FabricEvent::Registered {
                registration_id,
                plugin_id: record.plugin_id,
                name: record.name.clone(),
            }
        };

        inner.modules.insert(record.plugin_id, record);
        inner.events.push(event);
        self.bump_generation(&mut inner);

        info!(
            registration_id = %registration_id,
            "registered wasm fabric module"
        );

        registration_id
    }

    pub fn unregister(
        &self,
        plugin_id: PluginId,
        registration_id: FabricRegistrationId,
    ) -> bool {
        let mut inner = self.inner.write();

        let should_remove = inner
            .modules
            .get(&plugin_id)
            .map(|module| module.registration_id == registration_id)
            .unwrap_or(false);

        if !should_remove {
            debug!(
                registration_id = %registration_id,
                plugin_id = plugin_id.0,
                "ignored stale or unknown unregister"
            );
            return false;
        }

        if let Some(module) = inner.modules.remove(&plugin_id) {
            inner.events.push(FabricEvent::Unregistered {
                registration_id,
                plugin_id,
                name: module.name,
            });
            self.bump_generation(&mut inner);
            info!(
                registration_id = %registration_id,
                plugin_id = plugin_id.0,
                "unregistered wasm fabric module"
            );
            return true;
        }

        false
    }

    pub fn update_state(
        &self,
        plugin_id: PluginId,
        registration_id: FabricRegistrationId,
        state: FabricModuleState,
    ) -> bool {
        self.update_module(plugin_id, registration_id, |module| module.state = state)
    }

    pub fn update_health(
        &self,
        plugin_id: PluginId,
        registration_id: FabricRegistrationId,
        health: FabricHealth,
    ) -> bool {
        self.update_module(plugin_id, registration_id, |module| {
            module.health = health;
        })
    }

    pub fn update_capabilities(
        &self,
        plugin_id: PluginId,
        registration_id: FabricRegistrationId,
        capabilities: Vec<RegisteredCapability>,
    ) -> bool {
        self.update_module(plugin_id, registration_id, |module| {
            module.capabilities = capabilities;
        })
    }

    pub fn route(&self, request: FabricRequest) -> Option<FabricRoute> {
        let mut inner = self.inner.write();
        let mut candidates = Vec::new();

        for module in inner.modules.values() {
            if !module.state.is_routable() || !module.health.is_routable() {
                continue;
            }

            let Some(capability) = module
                .capabilities
                .iter()
                .filter(|capability| capability.capability.matches(&request))
                .max_by(|left, right| compare_capabilities(left, right, &request))
                .cloned()
            else {
                continue;
            };

            candidates.push(FabricCandidate {
                registration_id: module.registration_id,
                plugin_id: module.plugin_id,
                volt_id: module.volt_id.clone(),
                name: module.name.clone(),
                capability,
                state: module.state,
                health: module.health,
            });
        }

        candidates.sort_by(|left, right| {
            compare_candidates(left, right, &request, self.routing_policy.as_ref())
        });

        if let Some(selected) = candidates.first().cloned() {
            let route = FabricRoute {
                request: request.clone(),
                selected: selected.clone(),
                candidates,
            };
            inner
                .events
                .push(FabricEvent::RouteResolved { request, selected });
            return Some(route);
        }

        inner.events.push(FabricEvent::RouteMiss { request });
        None
    }

    pub fn snapshot(&self) -> FabricSnapshot {
        let inner = self.inner.read();
        let mut modules: Vec<_> = inner
            .modules
            .values()
            .map(|module| FabricModuleSnapshot {
                registration_id: module.registration_id,
                plugin_id: module.plugin_id,
                volt_id: module.volt_id.clone(),
                name: module.name.clone(),
                capabilities: module.capabilities.clone(),
                state: module.state,
                health: module.health,
            })
            .collect();

        modules.sort_by(|left, right| {
            left.plugin_id
                .0
                .cmp(&right.plugin_id.0)
                .then_with(|| left.registration_id.cmp(&right.registration_id))
        });

        FabricSnapshot {
            generation: inner.generation,
            modules,
        }
    }

    pub fn events(&self) -> Vec<FabricEvent> {
        self.inner.read().events.clone()
    }

    pub fn drain_events(&self) -> Vec<FabricEvent> {
        let mut inner = self.inner.write();
        std::mem::take(&mut inner.events)
    }

    fn update_module(
        &self,
        plugin_id: PluginId,
        registration_id: FabricRegistrationId,
        update: impl FnOnce(&mut FabricModuleRecord),
    ) -> bool {
        let mut inner = self.inner.write();
        let Some(module) = inner.modules.get_mut(&plugin_id) else {
            return false;
        };

        if module.registration_id != registration_id {
            warn!(
                registration_id = %registration_id,
                plugin_id = plugin_id.0,
                "ignored stale wasm fabric update"
            );
            return false;
        }

        update(module);
        inner.events.push(FabricEvent::Updated {
            registration_id,
            plugin_id,
        });
        self.bump_generation(&mut inner);
        true
    }

    fn bump_generation(&self, inner: &mut FabricInner) {
        inner.generation =
            self.next_generation.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

fn compare_capabilities(
    left: &RegisteredCapability,
    right: &RegisteredCapability,
    request: &FabricRequest,
) -> Ordering {
    left.capability
        .specificity(request)
        .cmp(&right.capability.specificity(request))
        .then_with(|| left.capability.priority.cmp(&right.capability.priority))
        .then_with(|| {
            left.capability
                .key
                .language
                .cmp(&right.capability.key.language)
        })
        .then_with(|| {
            left.capability
                .key
                .namespace
                .cmp(&right.capability.key.namespace)
        })
        .then_with(|| {
            left.capability
                .key
                .operation
                .cmp(&right.capability.key.operation)
        })
}

fn compare_candidates(
    left: &FabricCandidate,
    right: &FabricCandidate,
    request: &FabricRequest,
    routing_policy: &dyn FabricRoutingPolicy,
) -> Ordering {
    routing_policy
        .rank(request, right)
        .cmp(&routing_policy.rank(request, left))
        .then_with(|| left.plugin_id.0.cmp(&right.plugin_id.0))
        .then_with(|| left.registration_id.cmp(&right.registration_id))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lapce_rpc::plugin::{PluginId, VoltID};

    use super::{
        super::fabric_types::{
            CapabilitySource, FabricCapability, FabricHealth,
            FabricModuleRegistration, FabricModuleState, FabricRequest,
            RegisteredCapability,
        },
        DefaultFabricRoutingPolicy, WasmFabric,
    };

    fn manifest_capability(capability: FabricCapability) -> RegisteredCapability {
        RegisteredCapability::new(capability, CapabilitySource::VoltManifest)
    }

    fn registration(
        plugin_id: u64,
        name: &str,
        state: FabricModuleState,
        health: FabricHealth,
        capabilities: Vec<RegisteredCapability>,
    ) -> FabricModuleRegistration {
        FabricModuleRegistration::new(
            PluginId(plugin_id),
            VoltID {
                author: "author".to_string(),
                name: name.to_string(),
            },
            name,
        )
        .state(state)
        .health(health)
        .capabilities(capabilities)
    }

    #[test]
    fn routes_exact_over_wildcard_and_generic() {
        let fabric = WasmFabric::new();

        fabric.register(registration(
            1,
            "generic",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        fabric.register(registration(
            2,
            "wildcard",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(
                FabricCapability::new("language", "format").language("*"),
            )],
        ));

        fabric.register(registration(
            3,
            "exact",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(
                FabricCapability::new("language", "format").language("rust"),
            )],
        ));

        let route = fabric
            .route(FabricRequest::new("language", "format").language("rust"))
            .expect("route should resolve");

        assert_eq!(route.selected.plugin_id.0, 3);
        assert_eq!(
            route
                .candidates
                .iter()
                .map(|candidate| candidate.plugin_id.0)
                .collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }

    #[test]
    fn prefers_health_then_priority() {
        let fabric = WasmFabric::new();

        fabric.register(registration(
            1,
            "degraded-high-priority",
            FabricModuleState::Ready,
            FabricHealth::Degraded,
            vec![manifest_capability(
                FabricCapability::new("language", "format").priority(99),
            )],
        ));

        fabric.register(registration(
            2,
            "healthy-lower-priority",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(
                FabricCapability::new("language", "format").priority(1),
            )],
        ));

        let route = fabric
            .route(FabricRequest::new("language", "format"))
            .expect("route should resolve");

        assert_eq!(route.selected.plugin_id.0, 2);
    }

    #[test]
    fn filters_unroutable_modules() {
        let fabric = WasmFabric::new();

        fabric.register(registration(
            1,
            "starting",
            FabricModuleState::Starting,
            FabricHealth::Healthy,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        fabric.register(registration(
            2,
            "unhealthy",
            FabricModuleState::Ready,
            FabricHealth::Unhealthy,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        assert!(
            fabric
                .route(FabricRequest::new("language", "format"))
                .is_none()
        );
    }

    #[test]
    fn stale_unregistration_is_ignored() {
        let fabric = WasmFabric::new();

        let plugin = PluginId(1);
        let first_registration = fabric.register(registration(
            plugin.0,
            "module",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        let second_registration = fabric.register(registration(
            plugin.0,
            "module-new",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        assert_ne!(first_registration, second_registration);
        assert!(!fabric.unregister(plugin, first_registration));

        let snapshot = fabric.snapshot();
        assert_eq!(snapshot.modules.len(), 1);
        assert_eq!(snapshot.modules[0].registration_id, second_registration);
    }

    #[test]
    fn updates_require_current_registration() {
        let fabric = WasmFabric::new();

        let plugin = PluginId(7);
        let registration_id = fabric.register(registration(
            plugin.0,
            "stateful",
            FabricModuleState::Ready,
            FabricHealth::Unknown,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        ));

        assert!(!fabric.update_state(
            plugin,
            super::super::fabric_types::FabricRegistrationId(999),
            FabricModuleState::Stopped,
        ));

        assert!(fabric.update_health(
            plugin,
            registration_id,
            FabricHealth::Healthy
        ));

        let snapshot = fabric.snapshot();
        assert_eq!(snapshot.modules[0].health, FabricHealth::Healthy);
        assert_eq!(snapshot.modules[0].state, FabricModuleState::Ready);
    }

    #[test]
    fn default_routing_policy_matches_default_constructor() {
        let with_default_ctor = WasmFabric::new();
        let with_policy =
            WasmFabric::with_policy(Arc::new(DefaultFabricRoutingPolicy));

        let registration = registration(
            10,
            "formatter",
            FabricModuleState::Ready,
            FabricHealth::Healthy,
            vec![manifest_capability(
                FabricCapability::new("language", "format").language("rust"),
            )],
        );
        let registration2 = registration(
            11,
            "fallback",
            FabricModuleState::Ready,
            FabricHealth::Unknown,
            vec![manifest_capability(FabricCapability::new(
                "language", "format",
            ))],
        );

        with_default_ctor.register(registration.clone());
        with_default_ctor.register(registration2.clone());
        with_policy.register(registration);
        with_policy.register(registration2);

        let request = FabricRequest::new("language", "format").language("rust");
        let route_a = with_default_ctor.route(request.clone()).unwrap();
        let route_b = with_policy.route(request).unwrap();
        assert_eq!(route_a.selected.plugin_id, route_b.selected.plugin_id);
        assert_eq!(route_a.candidates.len(), route_b.candidates.len());
    }
}
