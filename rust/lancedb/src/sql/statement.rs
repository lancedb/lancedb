// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The statement registry: the seam between the SQL dialect and the behaviour
//! an embedder adds to it.
//!
//! A statement owns three things that are otherwise easy to spread across
//! parallel `downcast_ref` chains: the grammar that produces its node, the
//! audit label it reports, and the access it requires. Keeping them in one
//! place is what makes a statement addable from outside this crate.
//!
//! Two registries, because the axes differ. Grammar is matched against tokens
//! before a node exists, and several statements can share one handler -- an
//! `ALTER TABLE` handler may yield a different node per subcommand. A planned
//! node, by contrast, is claimed by exactly one statement.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::{parser::Parser, tokenizer::Token};

/// A pluggable handler for custom SQL statements.
pub trait CustomSqlHandler: Send + Sync {
    /// Whether this handler wants to handle these tokens.
    ///
    /// The tokens have had whitespace removed, so a handler can match on
    /// leading keywords without accounting for layout.
    fn matches(&self, tokens: &[&Token]) -> bool;

    /// Parse the statement into a logical plan.
    ///
    /// Returning `Ok(None)` declines a form this handler matched on; the
    /// statement then goes to DataFusion's own planner. See
    /// [`StatementRegistry`] for why that stops routing rather than falling
    /// through to the next handler.
    fn parse(&self, parser: &mut Parser) -> DfResult<Option<LogicalPlan>>;
}

/// What kind of relation a requirement is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationKind {
    Table,
    View,
}

/// What kind of object a DDL statement brings into existence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateKind {
    Table,
    View,
    MaterializedView,
}

/// What a statement needs authorized before it runs.
///
/// The vocabulary is deliberately generic: it names *what is being reached
/// for*, not the privilege that grants it. An embedder maps these onto its own
/// privilege model and audit labels, so no access-control concept has to live
/// in the dialect.
///
/// The variants are finer-grained than a bare read/write split because the
/// distinctions are load-bearing for that mapping -- appending to a table and
/// redefining it are different grants, and collapsing them would silently
/// widen what a statement is allowed to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessRequirement {
    /// Read the contents of a relation.
    Read {
        relation: ResolvedTableReference,
        kind: RelationKind,
    },
    /// Change the rows of a relation.
    Write {
        relation: ResolvedTableReference,
        kind: RelationKind,
        mode: WriteMode,
    },
    /// Change a relation's definition, or anything about it other than its
    /// rows. Index and column changes land here.
    Own {
        relation: ResolvedTableReference,
        kind: RelationKind,
    },
    /// Bring a new relation into existence.
    CreateIn {
        relation: ResolvedTableReference,
        kind: CreateKind,
    },
    /// Reach the connected database itself rather than a relation in it.
    Database { name: String, scope: DatabaseScope },
    /// Reach a namespace's metadata.
    Namespace { database: String, namespace: String },
    /// Reach the deployment rather than any one database.
    System { scope: SystemScope },
}

/// How an [`AccessRequirement::Write`] changes a relation's rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Add rows.
    Append,
    /// Change existing rows.
    Modify,
    /// Take rows away.
    Remove,
}

/// How far into a database an [`AccessRequirement::Database`] reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseScope {
    /// See that the database exists and list what is in it.
    Usage,
    /// Change what the database contains.
    Ownership,
}

/// How far into the deployment an [`AccessRequirement::System`] reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemScope {
    /// Observe deployment-wide state.
    Usage,
    /// Act on deployment-wide state.
    Operate,
}

impl AccessRequirement {
    /// The relation this requirement is about, for the relation-shaped
    /// variants.
    ///
    /// An embedder's privilege mapping is written against the variants
    /// directly; this is the shortcut for the common case of needing the
    /// relation without caring which shape asked for it.
    pub fn relation(&self) -> Option<(&ResolvedTableReference, RelationKind)> {
        match self {
            Self::Read { relation, kind }
            | Self::Write { relation, kind, .. }
            | Self::Own { relation, kind } => Some((relation, *kind)),
            Self::CreateIn { .. }
            | Self::Database { .. }
            | Self::Namespace { .. }
            | Self::System { .. } => None,
        }
    }
}

/// Everything a statement needs in order to state its requirements, without
/// reaching for the engine running it.
pub struct RequirementContext<'a> {
    /// The database a bare relation name resolves against.
    pub default_database: &'a str,
    /// The schema a bare relation name resolves against.
    pub default_schema: &'a str,
}

impl RequirementContext<'_> {
    /// Resolve a possibly-bare reference against the request's defaults.
    pub fn resolve(&self, relation: TableReference) -> ResolvedTableReference {
        relation.resolve(self.default_database, self.default_schema)
    }

    /// Resolve a bare relation name against the request's defaults.
    pub fn resolve_bare(&self, name: impl Into<String>) -> ResolvedTableReference {
        self.resolve(TableReference::bare(name.into()))
    }
}

/// One statement in the dialect: the node it plans to, what it is called in an
/// audit log, and what it needs authorized.
pub trait SqlStatement: Send + Sync {
    /// Whether this statement owns the given planned node.
    fn claims(&self, node: &dyn Any) -> bool;

    /// The audit label for this statement.
    ///
    /// This is an open string rather than an enum so that an embedder can add
    /// a statement -- and a label for it -- without changing this crate.
    fn audit_operation(&self) -> &'static str;

    /// What must be authorized before the node runs.
    ///
    /// Returning an empty set means the statement needs nothing beyond
    /// whatever the engine already collects from the plan's scans.
    fn access_requirements(
        &self,
        node: &dyn Any,
        context: &RequirementContext<'_>,
    ) -> DfResult<Vec<AccessRequirement>>;
}

/// The set of statements and grammars an engine knows about.
///
/// Ordering is load-bearing on the parse side and stays explicit. A handler
/// may be a catch-all over its leading keyword -- erroring on any form of that
/// keyword it does not recognize, or matching on the first token alone -- so a
/// handler registered *after* such a one can never be reached for that
/// keyword. Extensions are therefore consulted before whatever is already
/// registered.
///
/// A handler that matches and then returns `Ok(None)` stops routing entirely
/// rather than falling through to the next handler; the statement then goes to
/// DataFusion's own planner. That veto is intentional -- it is how a handler
/// declines a form it matched on -- but it means an overlapping handler
/// registered later is shadowed rather than reported, which is the other
/// reason ordering is explicit here.
#[derive(Default)]
pub struct StatementRegistry {
    parsers: Vec<Arc<dyn CustomSqlHandler>>,
    statements: Vec<Arc<dyn SqlStatement>>,
}

impl StatementRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from an explicit, already-ordered set.
    ///
    /// The ordering is used as given -- unlike [`Self::register_parser`], this
    /// does not reverse anything. It is how an embedder that owns the whole
    /// dialect states the order once.
    pub fn from_parts(
        parsers: Vec<Arc<dyn CustomSqlHandler>>,
        statements: Vec<Arc<dyn SqlStatement>>,
    ) -> Self {
        Self {
            parsers,
            statements,
        }
    }

    /// Add a grammar, consulted before every grammar already registered.
    ///
    /// Registration is front-insertion because an existing catch-all handler
    /// would otherwise shadow anything added later; see the type docs.
    pub fn register_parser(&mut self, parser: Arc<dyn CustomSqlHandler>) -> &mut Self {
        self.parsers.insert(0, parser);
        self
    }

    /// Add a statement, consulted before every statement already registered.
    pub fn register_statement(&mut self, statement: Arc<dyn SqlStatement>) -> &mut Self {
        self.statements.insert(0, statement);
        self
    }

    /// The grammars, in the order they are consulted.
    pub fn parsers(&self) -> &[Arc<dyn CustomSqlHandler>] {
        &self.parsers
    }

    /// The statement owning this planned node, if any.
    pub fn claim(&self, node: &dyn Any) -> Option<&Arc<dyn SqlStatement>> {
        self.statements.iter().find(|s| s.claims(node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Claimant {
        label: &'static str,
        claims_everything: bool,
    }

    impl SqlStatement for Claimant {
        fn claims(&self, node: &dyn Any) -> bool {
            self.claims_everything && node.is::<u8>()
        }

        fn audit_operation(&self) -> &'static str {
            self.label
        }

        fn access_requirements(
            &self,
            _node: &dyn Any,
            _context: &RequirementContext<'_>,
        ) -> DfResult<Vec<AccessRequirement>> {
            Ok(vec![])
        }
    }

    fn claimant(label: &'static str, claims_everything: bool) -> Arc<dyn SqlStatement> {
        Arc::new(Claimant {
            label,
            claims_everything,
        })
    }

    #[test]
    fn an_unclaimed_node_has_no_statement() {
        let mut registry = StatementRegistry::new();
        registry.register_statement(claimant("never", false));
        assert!(registry.claim(&0u8).is_none());
    }

    /// Front-insertion on the claim side too: an extension must be able to
    /// take over a node shape that something already registered also claims.
    #[test]
    fn the_last_registered_statement_claims_first() {
        let mut registry = StatementRegistry::new();
        registry
            .register_statement(claimant("first", true))
            .register_statement(claimant("second", true));

        assert_eq!(registry.claim(&0u8).unwrap().audit_operation(), "second");
    }

    #[test]
    fn from_parts_keeps_the_claim_order_it_was_given() {
        let registry =
            StatementRegistry::from_parts(vec![], vec![claimant("a", true), claimant("b", true)]);
        assert_eq!(registry.claim(&0u8).unwrap().audit_operation(), "a");
    }

    #[test]
    fn a_bare_name_resolves_against_the_request_defaults() {
        let context = RequirementContext {
            default_database: "db",
            default_schema: "public",
        };
        let resolved = context.resolve_bare("t");
        assert_eq!(&*resolved.catalog, "db");
        assert_eq!(&*resolved.schema, "public");
        assert_eq!(&*resolved.table, "t");
    }

    #[test]
    fn a_qualified_name_keeps_its_own_parts() {
        let context = RequirementContext {
            default_database: "db",
            default_schema: "public",
        };
        let resolved = context.resolve(TableReference::partial("other", "t"));
        assert_eq!(&*resolved.catalog, "db");
        assert_eq!(&*resolved.schema, "other");
    }

    #[test]
    fn only_the_relation_shaped_requirements_name_a_relation() {
        let relation = TableReference::bare("t").resolve("db", "public");

        let read = AccessRequirement::Read {
            relation: relation.clone(),
            kind: RelationKind::Table,
        };
        assert_eq!(read.relation().unwrap().1, RelationKind::Table);

        let create = AccessRequirement::CreateIn {
            relation,
            kind: CreateKind::MaterializedView,
        };
        assert!(create.relation().is_none());

        let system = AccessRequirement::System {
            scope: SystemScope::Operate,
        };
        assert!(system.relation().is_none());
    }
}
