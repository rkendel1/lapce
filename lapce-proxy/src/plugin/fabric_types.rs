use std::fmt;

use lapce_rpc::plugin::{PluginId, VoltID};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FabricCapability {
    pub namespace: String,
    pub operation: String,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub priority: i32,
}

impl FabricCapability {
    pub fn new(namespace: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            operation: operation.into(),
            language: None,
            priority: 0,
        }
    }

    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn matches(&self, request: &FabricRequest) -> bool {
        if self.namespace != request.namespace || self.operation != request.operation
        {
            return false;
        }

        match (self.language.as_deref(), request.language.as_deref()) {
            (None, _) => true,
            (Some("*"), _) => true,
            (Some(_), None) => false,
            (Some(capability_language), Some(request_language)) => {
                capability_language == request_language
            }
        }
    }

    pub fn specificity(&self, request: &FabricRequest) -> u8 {
        match (self.language.as_deref(), request.language.as_deref()) {
            (Some(capability_language), Some(request_language))
                if capability_language == request_language =>
            {
                3
            }
            (Some("*"), _) => 2,
            (None, _) => 1,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FabricRequest {
    pub namespace: String,
    pub operation: String,
    #[serde(default)]
    pub language: Option<String>,
}

impl FabricRequest {
    pub fn new(namespace: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            operation: operation.into(),
            language: None,
        }
    }

    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FabricModuleState {
    Starting,
    Ready,
    Suspended,
    Stopping,
    Stopped,
    Failed,
}

impl Default for FabricModuleState {
    fn default() -> Self {
        Self::Starting
    }
}

impl FabricModuleState {
    pub fn is_routable(self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FabricHealth {
    Unknown,
    Healthy,
    Degraded,
    Unhealthy,
}

impl Default for FabricHealth {
    fn default() -> Self {
        Self::Unknown
    }
}

impl FabricHealth {
    pub fn routing_rank(self) -> u8 {
        match self {
            Self::Healthy => 3,
            Self::Unknown => 2,
            Self::Degraded => 1,
            Self::Unhealthy => 0,
        }
    }

    pub fn is_routable(self) -> bool {
        !matches!(self, Self::Unhealthy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricModuleRegistration {
    pub plugin_id: PluginId,
    pub volt_id: VoltID,
    pub name: String,
    pub capabilities: Vec<FabricCapability>,
    pub state: FabricModuleState,
    pub health: FabricHealth,
}

impl FabricModuleRegistration {
    pub fn new(
        plugin_id: PluginId,
        volt_id: VoltID,
        name: impl Into<String>,
    ) -> Self {
        Self {
            plugin_id,
            volt_id,
            name: name.into(),
            capabilities: Vec::new(),
            state: FabricModuleState::Starting,
            health: FabricHealth::Unknown,
        }
    }

    pub fn capabilities(mut self, capabilities: Vec<FabricCapability>) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn state(mut self, state: FabricModuleState) -> Self {
        self.state = state;
        self
    }

    pub fn health(mut self, health: FabricHealth) -> Self {
        self.health = health;
        self
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct FabricRegistrationId(pub u64);

impl fmt::Display for FabricRegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricCandidate {
    pub registration_id: FabricRegistrationId,
    pub plugin_id: PluginId,
    pub volt_id: VoltID,
    pub name: String,
    pub capability: FabricCapability,
    pub state: FabricModuleState,
    pub health: FabricHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricRoute {
    pub request: FabricRequest,
    pub selected: FabricCandidate,
    pub candidates: Vec<FabricCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricSnapshot {
    pub generation: u64,
    pub modules: Vec<FabricModuleSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricModuleSnapshot {
    pub registration_id: FabricRegistrationId,
    pub plugin_id: PluginId,
    pub volt_id: VoltID,
    pub name: String,
    pub capabilities: Vec<FabricCapability>,
    pub state: FabricModuleState,
    pub health: FabricHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FabricEvent {
    Registered {
        registration_id: FabricRegistrationId,
        plugin_id: PluginId,
        name: String,
    },
    Updated {
        registration_id: FabricRegistrationId,
        plugin_id: PluginId,
    },
    Unregistered {
        registration_id: FabricRegistrationId,
        plugin_id: PluginId,
        name: String,
    },
    RouteResolved {
        request: FabricRequest,
        selected: FabricCandidate,
    },
    RouteMiss {
        request: FabricRequest,
    },
}
