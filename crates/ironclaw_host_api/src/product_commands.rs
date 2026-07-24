//! The canonical product-command inventory (vocabulary only).
//!
//! One descriptor per standardized slash command: the name, aliases, and
//! user-facing presentation every surface derives from. Behavior (parse +
//! execute) binds to these descriptors in the product-workflow crate, whose
//! contract test pins the two tables 1:1. Extension manifests validate their
//! `channel.commands` opt-in lists against this inventory, which is why the
//! metadata lives in this vocabulary crate rather than beside the behavior.

use serde::Serialize;

/// Public command inventory metadata. Policy decisions based on actor,
/// installation, trigger, or product surface belong to the product-workflow
/// admission service, never to this table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductCommandDescriptor {
    /// Canonical lowercase command name (the `/name` spelling).
    pub name: &'static str,
    /// Accepted alternate spellings. Aliases resolve to the same command;
    /// manifests declare canonical names only.
    pub aliases: &'static [&'static str],
    /// Short human title for menus.
    pub title: &'static str,
    /// One-line description for menus and help text.
    pub description: &'static str,
    /// Usage hint, starting with the slash spelling.
    pub usage: &'static str,
}

/// The canonical inventory. Order is presentation order for menus and help.
pub const PRODUCT_COMMANDS: &[ProductCommandDescriptor] = &[
    ProductCommandDescriptor {
        name: "model",
        aliases: &[],
        title: "Model",
        description: "Show or switch the active LLM provider and model",
        usage: "/model [<model> | set-provider <provider> [--model <model>]]",
    },
    ProductCommandDescriptor {
        name: "status",
        aliases: &["progress"],
        title: "Status",
        description: "Show what the assistant is doing in this conversation",
        usage: "/status",
    },
    ProductCommandDescriptor {
        name: "extension_search",
        aliases: &[],
        title: "Search extensions",
        description: "Search the extension registry",
        usage: "/extension_search <query>",
    },
    ProductCommandDescriptor {
        name: "extension_list",
        aliases: &[],
        title: "List extensions",
        description: "List installed extensions",
        usage: "/extension_list",
    },
    ProductCommandDescriptor {
        name: "extension_install",
        aliases: &[],
        title: "Install extension",
        description: "Install an extension by id",
        usage: "/extension_install <id>",
    },
    ProductCommandDescriptor {
        name: "extension_auth",
        aliases: &[],
        title: "Connect extension account",
        description: "Start authentication for an installed extension",
        usage: "/extension_auth <id>",
    },
    ProductCommandDescriptor {
        name: "extension_activate",
        aliases: &[],
        title: "Activate extension",
        description: "Activate an installed extension",
        usage: "/extension_activate <id>",
    },
    ProductCommandDescriptor {
        name: "extension_configure",
        aliases: &[],
        title: "Configure extension",
        description: "Update an installed extension's configuration values",
        usage: "/extension_configure <id> <json>",
    },
    ProductCommandDescriptor {
        name: "extension_remove",
        aliases: &[],
        title: "Remove extension",
        description: "Remove an installed extension",
        usage: "/extension_remove <id>",
    },
    ProductCommandDescriptor {
        name: "skill_search",
        aliases: &[],
        title: "Search skills",
        description: "Search the skill registry",
        usage: "/skill_search <query>",
    },
    ProductCommandDescriptor {
        name: "skill_install",
        aliases: &[],
        title: "Install skill",
        description: "Install a skill from JSON content",
        usage: "/skill_install <json>",
    },
    ProductCommandDescriptor {
        name: "skill_remove",
        aliases: &[],
        title: "Remove skill",
        description: "Remove an installed skill",
        usage: "/skill_remove <id or name>",
    },
];

/// Resolve a descriptor by canonical name or alias. Input is expected in the
/// lowercase form the shared slash parser produces.
pub fn find_product_command(name: &str) -> Option<&'static ProductCommandDescriptor> {
    PRODUCT_COMMANDS
        .iter()
        .find(|descriptor| descriptor.name == name || descriptor.aliases.contains(&name))
}

/// True only for canonical names (not aliases). Manifest `channel.commands`
/// lists validate against this.
pub fn is_product_command_name(name: &str) -> bool {
    PRODUCT_COMMANDS
        .iter()
        .any(|descriptor| descriptor.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_aliases_are_unique_across_the_inventory() {
        let mut seen = std::collections::BTreeSet::new();
        for descriptor in PRODUCT_COMMANDS {
            assert!(
                seen.insert(descriptor.name),
                "duplicate command name or alias: {}",
                descriptor.name
            );
            for alias in descriptor.aliases {
                assert!(
                    seen.insert(alias),
                    "duplicate command name or alias: {alias}"
                );
            }
        }
    }

    #[test]
    fn every_descriptor_has_presentation_metadata() {
        for descriptor in PRODUCT_COMMANDS {
            assert!(
                !descriptor.title.trim().is_empty(),
                "{} title",
                descriptor.name
            );
            assert!(
                !descriptor.description.trim().is_empty(),
                "{} description",
                descriptor.name
            );
            assert!(
                descriptor.usage.starts_with('/'),
                "{} usage must start with the slash spelling",
                descriptor.name
            );
        }
    }

    #[test]
    fn names_are_lowercase_ascii() {
        for descriptor in PRODUCT_COMMANDS {
            for name in std::iter::once(descriptor.name).chain(descriptor.aliases.iter().copied()) {
                assert_eq!(
                    name,
                    name.to_ascii_lowercase(),
                    "the shared slash parser lowercases input; inventory names must match"
                );
            }
        }
    }

    #[test]
    fn aliases_resolve_but_are_not_canonical_names() {
        let status = find_product_command("progress").expect("alias resolves");
        assert_eq!(status.name, "status");
        assert!(is_product_command_name("status"));
        assert!(!is_product_command_name("progress"));
        assert!(find_product_command("nonsense").is_none());
    }
}
